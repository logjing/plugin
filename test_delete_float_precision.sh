#!/bin/bash
# ============================================================================
# iceberg_delta DELETE 浮点精度漏删测试（delete 分支）
#
# 目的：验证 DELETE 的 WHERE 字面量序列化在"USTORE float4 与 Iceberg float4
#       精度不对称"场景下，是否会出现"该删的行没删掉"（漏删）。
#
# 根因（见 fdw_modify.cpp::AppendFilterConst 与 flush.cpp）：
#   - flush 路径：  float4 经 (float)DatumGetFloat4 存为单精度，写进 Iceberg 的
#                   是该值的"精确单精度表示"（如 0.1 → 0.100000001490...）。
#   - DELETE 路径： 同一个值经 appendStringInfo("%f", ...) 序列化，%f 默认仅
#                   6 位小数（0.1 → "0.100000"），PyIceberg 解析为 double 0.1。
#   - 于是 Iceberg 列里存 0.10000000149，filter 用 0.1 去匹配 → 不相等 → 漏删。
#
# 本测试构造一个 score=0.1(float4) 的行，flush 落湖后 DELETE ... WHERE score = 0.1，
# 然后用 pyiceberg 读回 Iceberg 表，断言该行是否真的被删除。
# ============================================================================
set -e

GAUSSHOME=/home/sjl/openGauss-server/mppdb_temp_install
export GAUSSHOME
export LD_LIBRARY_PATH="$GAUSSHOME/lib:$GAUSSHOME/jre/lib:$HOME/binarylibs/buildtools/gcc10.3/gcc/lib64:$HOME/binarylibs/buildtools/gcc10.3/isl/lib:$HOME/binarylibs/buildtools/gcc10.3/gmp/lib:$HOME/binarylibs/buildtools/gcc10.3/mpfr/lib:$HOME/binarylibs/kernel/dependency/cjson/comm/lib:$HOME/binarylibs/kernel/dependency/libcurl/comm/lib:$HOME/binarylibs/kernel/dependency/openssl/comm/lib:$LD_LIBRARY_PATH"

# ============================================================================
# S3 配置（从环境变量读取，与 test_flush_e2e.sh 一致）
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

# 用 pyiceberg 读 Iceberg 表并返回指定列的行（JSON），供 bash 断言
read_lake_rows() {
    python3 <<'PYEOF'
import os, json
from pyiceberg.catalog.sql import SqlCatalog
cat = SqlCatalog('x', **{
    'uri': 'sqlite:////tmp/_iceberg_lite_catalog.db',
    'warehouse': 's3://' + os.environ['S3_BUCKET'] + '/',
    's3.endpoint': os.environ['S3_ENDPOINT'],
    's3.access-key-id': os.environ['S3_ACCESS_KEY_ID'],
    's3.secret-access-key': os.environ['S3_SECRET_ACCESS_KEY'],
})
tbl = cat.load_table(('iceberg_warehouse', 'del_test', 'fp'))
result = tbl.scan().to_arrow()
rows = []
for i in range(len(result)):
    rows.append({c: result[c][i].as_py() for c in result.column_names})
print(json.dumps(rows))
PYEOF
}

# ============================================================================
# Step 1: 环境准备
# ============================================================================
step "Step 1: 清理旧环境，创建扩展"

$GSQL <<SQL
DROP EXTENSION IF EXISTS iceberg_delta CASCADE;
DROP SCHEMA IF EXISTS iceberg_delta CASCADE;
CREATE EXTENSION iceberg_delta;
SQL
pass "扩展已创建"

TABLE_PATH="iceberg_warehouse/del_test/fp"
TABLE_LOCATION="s3://${S3_BUCKET}/${TABLE_PATH}"

# 清理上一轮 S3 数据和 catalog
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
" 2>&1
python3 -c "
import sqlite3; c=sqlite3.connect('/tmp/_iceberg_lite_catalog.db')
c.execute(\"DELETE FROM iceberg_tables WHERE table_namespace LIKE '%del_test%'\")
c.commit(); c.close()
" 2>&1
info "清理完成"

# ============================================================================
# Step 2: 创建外表 —— score 用 REAL（float4 单精度），刻意制造精度陷阱
# ============================================================================
step "Step 2: 创建外表（score = REAL，float4 单精度）"

$GSQL <<SQL
DROP FOREIGN TABLE IF EXISTS fp_test CASCADE;

-- 关键：score 是 float4。0.1 无法用有限二进制精确表示，
-- 单精度往返后是 0.1000000014901161...（见 flush 写入值）
CREATE FOREIGN TABLE fp_test (
    id    INTEGER,
    score REAL
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
pass "外表 fp_test 已创建（score = float4）"

# ============================================================================
# Step 3: 插入 3 行并 flush 落湖
#         id=1 score=0.1  ← 唯一的目标漏删候选（精度陷阱值）
#         id=2 score=95.5 ← 对照：恰好可精确表示，不受影响
#         id=3 score=0.3  ← 另一个精度陷阱值
# ============================================================================
step "Step 3: 插入并 flush 落湖"

$GSQL <<SQL
INSERT INTO fp_test VALUES (1, 0.1), (2, 95.5), (3, 0.3);
SQL

FLUSH_RESULT=$($GSQL -t -c "SELECT iceberg_delta.iceberg_delta_flush('fp_test');" 2>&1 | tr -d ' ')
[ "$FLUSH_RESULT" = "3" ] || fail "flush 应返回 3，实际: $FLUSH_RESULT"
pass "flush 落湖 $FLUSH_RESULT 行"

# 动态获取 delta 表名（不同版本命名规则不同：<名>_delta 或 delta_<oid>）
DELTA_TABLE=$($GSQL -t -c "SELECT relname FROM pg_class WHERE relname LIKE 'delta_%' OR relname LIKE 'fp_test_delta' ORDER BY relname;" 2>&1 | tr -d '\n' | head -1)
[ -n "$DELTA_TABLE" ] || fail "未找到 fp_test 的 delta 内表"
info "delta 内表名: $DELTA_TABLE"

# 确认 delta 已清空（这 3 行已全部进 Iceberg）
DELTA_AFTER=$($GSQL -t -c "SELECT count(*) FROM iceberg_delta.${DELTA_TABLE};" 2>&1 | tr -d ' ')
[ "$DELTA_AFTER" = "0" ] || fail "flush 后 delta 应为空，实际: $DELTA_AFTER"

# ============================================================================
# Step 4: 先读回 Iceberg 表，确认落湖的真实浮点值（暴露精度真相）
# ============================================================================
step "Step 4: 读回 Iceberg 表，确认落湖的真实浮点值"

info "Iceberg 表中的 score 列真实值（注意 id=1 的 0.1 存成什么）："
read_lake_rows | python3 -c "
import json, sys
rows = json.loads(sys.stdin.read())
for r in rows:
    # repr 显示完整精度，确认 flush 存的不是干净的 0.1
    print(f'  id={r[\"id\"]}  score={r[\"score\"]!r}  (type={type(r[\"score\"]).__name__})')
"

# ============================================================================
# Step 5: 执行 DELETE WHERE score = 0.1 —— 这是暴露漏删的关键操作
# ============================================================================
step "Step 5: 执行 DELETE FROM fp_test WHERE score = 0.1"

$GSQL -c "DELETE FROM fp_test WHERE score = 0.1;" 2>&1
info "DELETE 语句已执行（语句成功不代表行真被删，需校验 Iceberg）"

# ============================================================================
# Step 6: 校验 —— 这才是测试的核心断言
# ============================================================================
step "Step 6: 校验 id=1（score=0.1）是否真的被删除"

# 预期：若 DELETE 正确，id=1 应消失，剩余 {id=2, id=3}
ROWS=$(read_lake_rows)
REMAINING_IDS=$(echo "$ROWS" | python3 -c "import json,sys; r=json.loads(sys.stdin.read()); print(' '.join(str(x['id']) for x in sorted(r, key=lambda x:x['id'])))")
ROW_COUNT=$(echo "$ROWS" | python3 -c "import json,sys; print(len(json.loads(sys.stdin.read())))")

echo ""
echo "Iceberg 表剩余行："
echo "$ROWS" | python3 -c "
import json, sys
for r in sorted(json.loads(sys.stdin.read()), key=lambda x:x['id']):
    print(f'  id={r[\"id\"]}  score={r[\"score\"]!r}')
"
echo ""
echo "剩余 id: [${REMAINING_IDS}]   剩余行数: ${ROW_COUNT}"

echo ""
echo -e "${YELLOW}━━━ 结论 ━━━${NC}"
if [ "$ROW_COUNT" = "2" ] && echo "$REMAINING_IDS" | grep -qv "1"; then
    echo -e "${GREEN}[PASS] id=1 已正确删除（DELETE 浮点处理正确）${NC}"
    echo ""
    echo "  意外结果：当前实现的浮点 DELETE 未出现漏删。"
    echo "  可能 PyIceberg 对 float4 列做了宽松比较或类型提升。"
    exit 0
else
    echo -e "${RED}[FAIL] id=1 未被删除 —— 浮点漏删问题已复现！${NC}"
    echo ""
    echo "  根因："
    echo "    flush 写入 Iceberg 的 score 是 float4 精确值 0.1000000014901161..."
    echo "    DELETE 的 WHERE 经 %f 序列化为 '0.100000'，PyIceberg 解析为 0.1"
    echo "    0.1000000014901161 != 0.1 → 不匹配 → 该删的行没删掉"
    echo ""
    echo "  位置：fdw_modify.cpp::AppendFilterConst 的 FLOAT4OID 分支"
    echo "        appendStringInfo(buf, \"%f\", DatumGetFloat4(...))  ← 默认仅 6 位小数"
    exit 1
fi
