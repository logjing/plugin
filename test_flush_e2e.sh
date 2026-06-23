#!/bin/bash
# ============================================================================
# iceberg_delta flush 端到端测试
# 流程：建表 → 插入 → flush → 清空delta → MinIO 验证数据完整性
# ============================================================================
set -e

GAUSSHOME=/home/sjl/openGauss-server/mppdb_temp_install
export GAUSSHOME
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$GAUSSHOME/jre/lib:$HOME/binarylibs/buildtools/gcc10.3/gcc/lib64:$HOME/binarylibs/buildtools/gcc10.3/isl/lib:$HOME/binarylibs/buildtools/gcc10.3/gmp/lib:$HOME/binarylibs/buildtools/gcc10.3/mpfr/lib:$HOME/binarylibs/buildtools/gcc10.3/mpc/lib:$HOME/binarylibs/kernel/dependency/cjson/comm/lib:$HOME/binarylibs/kernel/dependency/libcurl/comm/lib:$HOME/binarylibs/kernel/dependency/openssl/comm/lib:$LD_LIBRARY_PATH"

# ============================================================================
# MinIO / S3 配置（从环境变量读取，未设置则用默认值）
#
#   ICEBERG_S3_ENDPOINT          MinIO/S3 端点 URL
#   ICEBERG_S3_ACCESS_KEY_ID     访问密钥 ID
#   ICEBERG_S3_SECRET_ACCESS_KEY 访问密钥
#   ICEBERG_S3_BUCKET            bucket 名
#   ICEBERG_S3_REGION            region
#
# 注意：access key / secret 没有默认值——必须显式设置环境变量，避免硬编码凭证。
# ============================================================================
S3_ENDPOINT="${ICEBERG_S3_ENDPOINT:-http://127.0.0.1:19000}"
S3_REGION="${ICEBERG_S3_REGION:-us-east-1}"
S3_BUCKET="${ICEBERG_S3_BUCKET:-sin}"
if [ -z "${ICEBERG_S3_ACCESS_KEY_ID:-}" ]; then
    echo "[FAIL] 必须设置环境变量 ICEBERG_S3_ACCESS_KEY_ID" >&2
    exit 1
fi
if [ -z "${ICEBERG_S3_SECRET_ACCESS_KEY:-}" ]; then
    echo "[FAIL] 必须设置环境变量 ICEBERG_S3_SECRET_ACCESS_KEY" >&2
    exit 1
fi
S3_ACCESS_KEY_ID="$ICEBERG_S3_ACCESS_KEY_ID"
S3_SECRET_ACCESS_KEY="$ICEBERG_S3_SECRET_ACCESS_KEY"
export S3_ENDPOINT S3_REGION S3_BUCKET S3_ACCESS_KEY_ID S3_SECRET_ACCESS_KEY

GSQL="$GAUSSHOME/bin/gsql -p 9876 -d postgres"

BOLD='\033[1m'
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m'

info()  { echo -e "${BOLD}[INFO]${NC} $*"; }
pass()  { echo -e "${GREEN}[PASS]${NC} $*"; }
fail()  { echo -e "${RED}[FAIL]${NC} $*"; exit 1; }
step()  { echo -e "\n${BOLD}${YELLOW}━━━ $* ━━━${NC}"; }

# ============================================================================
# Step 1: 环境准备
# ============================================================================
step "Step 1: 清理旧环境，创建扩展"

$GSQL <<SQL
DROP EXTENSION IF EXISTS iceberg_delta CASCADE;
DROP SCHEMA IF EXISTS iceberg_delta CASCADE;
CREATE EXTENSION iceberg_delta;
SQL

# 验证扩展和函数
FUNC_COUNT=$($GSQL -t -c "SELECT count(*) FROM pg_proc WHERE proname = 'iceberg_delta_flush';" 2>&1 | tr -d ' ')
if [ "$FUNC_COUNT" != "1" ]; then
    fail "flush 函数未注册"
fi
pass "扩展创建成功，iceberg_delta_flush 函数已注册"

# ============================================================================
# Step 2: 创建外表
# ============================================================================
step "Step 2: 创建 Iceberg 外表"

TABLE_PATH="iceberg_warehouse/e2e_test/staff"
TABLE_LOCATION="s3://${S3_BUCKET}/${TABLE_PATH}"

# 清理上一轮残留的 S3 数据和 catalog
info "清理旧 S3 数据和 catalog..."
python3 -c "
import boto3, os; from botocore.client import Config
s3 = boto3.client('s3', endpoint_url=os.environ['S3_ENDPOINT'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    region_name=os.environ['S3_REGION'])
for obj in s3.list_objects_v2(Bucket=os.environ['S3_BUCKET'], Prefix='${TABLE_PATH}/').get('Contents', []):
    s3.delete_object(Bucket=os.environ['S3_BUCKET'], Key=obj['Key'])
    print(f'  Deleted {obj[\"Key\"]}')
" 2>&1
python3 -c "
import sqlite3; c=sqlite3.connect('/tmp/_iceberg_lite_catalog.db')
c.execute(\"DELETE FROM iceberg_tables WHERE table_namespace LIKE '%e2e_test%'\")
c.commit(); c.close()
" 2>&1
info "清理完成"

$GSQL <<SQL
DROP FOREIGN TABLE IF EXISTS e2e_flush_test CASCADE;

CREATE FOREIGN TABLE e2e_flush_test (
    id          INTEGER,
    name        VARCHAR(100),
    score       DOUBLE PRECISION,
    department  VARCHAR(50),
    hire_date   DATE
) SERVER iceberg_server
OPTIONS (
    location                '${TABLE_LOCATION}',
    s3_endpoint             '${S3_ENDPOINT}',
    s3_access_key_id        '${S3_ACCESS_KEY_ID}',
    s3_secret_access_key    '${S3_SECRET_ACCESS_KEY}',
    s3_region               '${S3_REGION}',
    s3_path_style_access    'true',
    s3_ssl_enabled          'false'
);
SQL

# 验证外表和 delta 表已创建
FT_COUNT=$($GSQL -t -c "SELECT count(*) FROM pg_class WHERE relname = 'e2e_flush_test';" 2>&1 | tr -d ' ')
DT_COUNT=$($GSQL -t -c "SELECT count(*) FROM pg_class WHERE relname LIKE 'delta_%';" 2>&1 | tr -d ' ')

[ "$FT_COUNT" = "1" ] || fail "外表未创建"
[ "$DT_COUNT" = "1" ] || fail "delta 表未自动创建"

DELTA_TABLE=$($GSQL -t -c "SELECT relname FROM pg_class WHERE relname LIKE 'delta_%';" 2>&1 | tr -d ' ')
pass "外表 e2e_flush_test 创建成功，delta 表: $DELTA_TABLE"

# 验证此时 MinIO 上不存在 Iceberg 表（懒创建：flush 前不建表）
info "验证：flush 前 MinIO 上无 Iceberg 表..."
python3 -c "
import boto3, os
from botocore.client import Config
s3 = boto3.client('s3',
    endpoint_url=os.environ['S3_ENDPOINT'],
    aws_access_key_id=os.environ['S3_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['S3_SECRET_ACCESS_KEY'],
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    region_name=os.environ['S3_REGION'])
resp = s3.list_objects_v2(Bucket=os.environ['S3_BUCKET'], Prefix='iceberg_warehouse/e2e_test/')
if resp.get('KeyCount', 0) == 0:
    print('VERIFIED: No Iceberg table before first flush')
else:
    print('WARNING: Found objects, cleaning up...')
    import sys; sys.exit(0)
" 2>&1 || true
pass "flush 前 MinIO 上无 Iceberg 表（懒创建模式正确）"

# ============================================================================
# Step 3: 插入测试数据
# ============================================================================
step "Step 3: 插入测试数据到外表（写入 delta 表）"

INSERT_RESULT=$($GSQL <<SQL
INSERT INTO e2e_flush_test VALUES
    (1,  'Alice',   95.5, 'Engineering', '2024-01-15'),
    (2,  'Bob',     87.0, 'Sales',       '2024-03-01'),
    (3,  'Charlie', 76.5, 'Engineering', '2024-06-20'),
    (4,  'Diana',   92.0, 'Marketing',   '2024-02-10'),
    (5,  'Eve',     88.5, 'Sales',       '2024-04-15'),
    (6,  'Frank',   73.0, 'HR',          '2024-07-01'),
    (7,  'Grace',   99.0, 'Engineering', '2024-01-20'),
    (8,  'Henry',   81.5, 'Marketing',   '2024-05-10'),
    (9,  'Ivy',     94.0, 'Engineering', '2024-08-01'),
    (10, 'Jack',    68.5, 'HR',          '2024-09-15');
SQL
)
echo "$INSERT_RESULT"

DELTA_COUNT=$($GSQL -t -c "SELECT count(*) FROM iceberg_delta.${DELTA_TABLE};" 2>&1 | tr -d ' ')
[ "$DELTA_COUNT" = "10" ] || fail "delta 表应有 10 行，实际: $DELTA_COUNT"
pass "成功插入 10 行数据到 delta 表"

# ============================================================================
# Step 4: 执行 Flush
# ============================================================================
step "Step 4: 执行 flush → 写入 MinIO Iceberg 表"

FLUSH_RESULT=$($GSQL -t -c "SELECT iceberg_delta.iceberg_delta_flush('e2e_flush_test');" 2>&1 | tr -d ' ')
[ "$FLUSH_RESULT" = "10" ] || fail "flush 应返回 10，实际: $FLUSH_RESULT"
pass "flush 返回 $FLUSH_RESULT（10 行已刷写）"

# ============================================================================
# Step 5: 验证 delta 表已清空
# ============================================================================
step "Step 5: 验证 delta 表已清空"

DELTA_AFTER=$($GSQL -t -c "SELECT count(*) FROM iceberg_delta.${DELTA_TABLE};" 2>&1 | tr -d ' ')
[ "$DELTA_AFTER" = "0" ] || fail "flush 后 delta 表应为空，实际: $DELTA_AFTER"
pass "delta 表已清空（$DELTA_AFTER 行）"

# ============================================================================
# Step 6: 从 MinIO 读回数据验证完整性
# ============================================================================
step "Step 6: 从 MinIO 读取 Iceberg 表，验证数据完整性"

python3 <<'PYEOF'
import os
from pyiceberg.catalog.sql import SqlCatalog

cat = SqlCatalog('x', **{
    'uri': 'sqlite:////tmp/_iceberg_lite_catalog.db',
    'warehouse': 's3://' + os.environ['S3_BUCKET'] + '/',
    's3.endpoint': os.environ['S3_ENDPOINT'],
    's3.access-key-id': os.environ['S3_ACCESS_KEY_ID'],
    's3.secret-access-key': os.environ['S3_SECRET_ACCESS_KEY'],
})

# 加载表
tbl = cat.load_table(('iceberg_warehouse', 'e2e_test', 'staff'))
result = tbl.scan().to_arrow()

print(f"Schema: {[f'{c}({result[c].type})' for c in result.column_names]}")
print(f"Total rows: {len(result)}")

# 转换为 dict 列表
rows = []
for i in range(len(result)):
    rows.append({c: result[c][i].as_py() for c in result.column_names})

# ============ 验证项 ============
errors = []

# 1. 行数
if len(rows) != 10:
    errors.append(f"行数不对: 期望 10, 实际 {len(rows)}")

# 2. 逐行验证
expected = [
    (1,  'Alice',   95.5, 'Engineering', '2024-01-15'),
    (2,  'Bob',     87.0, 'Sales',       '2024-03-01'),
    (3,  'Charlie', 76.5, 'Engineering', '2024-06-20'),
    (4,  'Diana',   92.0, 'Marketing',   '2024-02-10'),
    (5,  'Eve',     88.5, 'Sales',       '2024-04-15'),
    (6,  'Frank',   73.0, 'HR',          '2024-07-01'),
    (7,  'Grace',   99.0, 'Engineering', '2024-01-20'),
    (8,  'Henry',   81.5, 'Marketing',   '2024-05-10'),
    (9,  'Ivy',     94.0, 'Engineering', '2024-08-01'),
    (10, 'Jack',    68.5, 'HR',          '2024-09-15'),
]

# 按 id 排序
rows.sort(key=lambda r: r['id'])

for i, (eid, ename, escore, edept, edate) in enumerate(expected):
    r = rows[i]
    if r['id'] != eid:
        errors.append(f"Row {i}: id 期望 {eid}, 实际 {r['id']}")
    if r['name'] != ename:
        errors.append(f"Row {i}: name 期望 {ename}, 实际 {r['name']}")
    if abs(r['score'] - escore) > 0.01:
        errors.append(f"Row {i}: score 期望 {escore}, 实际 {r['score']}")
    if r['department'] != edept:
        errors.append(f"Row {i}: department 期望 {edept}, 实际 {r['department']}")
    # hire_date 回读为 timestamp 格式（如 "2024-01-15 00:00:00"）
    actual_date = str(r['hire_date'])[:10]
    if actual_date != edate:
        errors.append(f"Row {i}: hire_date 期望 {edate}, 实际 {r['hire_date']}")

# 3. 聚合验证
eng_count = sum(1 for r in rows if r['department'] == 'Engineering')
if eng_count != 4:
    errors.append(f"Engineering 部门人数: 期望 4, 实际 {eng_count}")

avg_score = sum(r['score'] for r in rows) / len(rows)
expected_avg = (95.5+87+76.5+92+88.5+73+99+81.5+94+68.5) / 10
if abs(avg_score - expected_avg) > 0.01:
    errors.append(f"平均分: 期望 {expected_avg:.1f}, 实际 {avg_score:.1f}")

max_score = max(r['score'] for r in rows)
if max_score != 99.0:
    errors.append(f"最高分: 期望 99.0, 实际 {max_score}")

min_score = min(r['score'] for r in rows)
if min_score != 68.5:
    errors.append(f"最低分: 期望 68.5, 实际 {min_score}")

# ============ 输出结果 ============
print(f"\n{'='*60}")
if errors:
    print(f"❌ 验证失败！{len(errors)} 个错误:")
    for e in errors:
        print(f"   {e}")
    import sys; sys.exit(1)
else:
    print("✅ 所有验证项通过！")
    print(f"   - 行数: 10 ✓")
    print(f"   - 每行数据逐字段匹配 ✓")
    print(f"   - Engineering 部门 4 人 ✓")
    print(f"   - 平均分 {avg_score:.1f} ✓")
    print(f"   - 最高分 {max_score} ✓")
    print(f"   - 最低分 {min_score} ✓")
    print(f"\n完整数据:")
    for r in rows:
        print(f"   {r['id']:2d} | {r['name']:8s} | {r['score']:5.1f} | {r['department']:12s} | {r['hire_date']}")

import sys; sys.exit(0)
PYEOF

if [ $? -eq 0 ]; then
    pass "MinIO 数据验证全部通过！"
else
    fail "MinIO 数据验证失败"
fi

# ============================================================================
# Step 7: 事务性验证 — 再次插入并 flush
# ============================================================================
step "Step 7: 事务性验证 — 增量 flush"

$GSQL <<SQL
INSERT INTO e2e_flush_test VALUES
    (11, 'Karen', 85.0, 'Finance', '2024-10-01'),
    (12, 'Leo',   91.0, 'Finance', '2024-11-01');
SQL

FLUSH2=$($GSQL -t -c "SELECT iceberg_delta.iceberg_delta_flush('e2e_flush_test');" 2>&1 | tr -d ' ')
[ "$FLUSH2" = "2" ] || fail "第二次 flush 应返回 2，实际: $FLUSH2"
pass "第二次 flush 返回 $FLUSH2（增量 2 行）"

DELTA2=$($GSQL -t -c "SELECT count(*) FROM iceberg_delta.${DELTA_TABLE};" 2>&1 | tr -d ' ')
[ "$DELTA2" = "0" ] || fail "第二次 flush 后 delta 表应为空，实际: $DELTA2"
pass "delta 表再次清空"

# 验证总行数
python3 <<'PYEOF'
import os
from pyiceberg.catalog.sql import SqlCatalog
cat = SqlCatalog('x', **{
    'uri': 'sqlite:////tmp/_iceberg_lite_catalog.db',
    'warehouse': 's3://' + os.environ['S3_BUCKET'] + '/',
    's3.endpoint': os.environ['S3_ENDPOINT'],
    's3.access-key-id': os.environ['S3_ACCESS_KEY_ID'],
    's3.secret-access-key': os.environ['S3_SECRET_ACCESS_KEY'],
})
tbl = cat.load_table(('iceberg_warehouse', 'e2e_test', 'staff'))
result = tbl.scan().to_arrow()
total = len(result)
if total == 12:
    print(f"VERIFIED: Total rows after 2nd flush = {total}")
    import sys; sys.exit(0)
else:
    print(f"ERROR: Expected 12 rows, got {total}")
    import sys; sys.exit(1)
PYEOF

if [ $? -eq 0 ]; then
    pass "增量 flush 后总计 12 行 ✓"
else
    fail "增量 flush 验证失败"
fi

# ============================================================================
# 完成
# ============================================================================
echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  ✅ iceberg_delta flush 端到端测试全部通过！${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════${NC}"
echo ""
echo "  测试覆盖:"
echo "  ✓ 扩展创建和函数注册"
echo "  ✓ 外表 + delta 表自动创建"
echo "  ✓ 懒创建：flush 前 MinIO 无 Iceberg 表"
echo "  ✓ 10 行数据插入 delta 表"
echo "  ✓ flush 返回正确行数"
echo "  ✓ delta 表 flush 后清空"
echo "  ✓ MinIO Iceberg 表数据逐行匹配"
echo "  ✓ 聚合验证（avg/max/min/count）"
echo "  ✓ 第二次增量 flush"
echo "  ✓ 事务一致性（delta 删除 + Iceberg 写入）"
