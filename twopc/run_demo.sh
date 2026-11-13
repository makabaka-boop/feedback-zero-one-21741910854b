#!/usr/bin/env bash
# 手动演示：启动三个独立进程（协调者 + db1 + db2），各自独立 SQLite 文件。
# 用法：
#   ./run_demo.sh            普通模式（兼容旧协议，无代）
#   ./run_demo.sh fenced     fenced 代模式（协调进程携带 epoch，分库代校验）
# 停止： kill $(cat run_demo/*.pid)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p run_demo
PY=${PYTHON:-python3}
MODE=${1:-normal}

if [ "$MODE" = "fenced" ]; then
  FENCED_FLAG="--fenced"
  EPOCH_FILE="run_demo/coord.epoch.g1"
  EPOCH_ARG="--epoch-file $EPOCH_FILE"
else
  FENCED_FLAG=""
  EPOCH_ARG=""
fi

$PY -I shard.py --name db1 --port 19011 --db run_demo/db1.sqlite $FENCED_FLAG \
    >run_demo/db1.log 2>&1 &
echo $! > run_demo/db1.pid
$PY -I shard.py --name db2 --port 19012 --db run_demo/db2.sqlite $FENCED_FLAG \
    >run_demo/db2.log 2>&1 &
echo $! > run_demo/db2.pid
sleep 0.5
# shellcheck disable=SC2086
$PY -I coordinator.py --port 19010 --db run_demo/coord.sqlite $EPOCH_ARG \
    --db1-url http://127.0.0.1:19011 --db2-url http://127.0.0.1:19012 $FENCED_FLAG \
    >run_demo/coord.log 2>&1 &
echo $! > run_demo/coord.pid
sleep 0.8

echo "三进程已启动（模式: $MODE）："
echo "  coordinator http://127.0.0.1:19010 (pid $(cat run_demo/coord.pid))"
echo "  db1         http://127.0.0.1:19011 (pid $(cat run_demo/db1.pid))"
echo "  db2         http://127.0.0.1:19012 (pid $(cat run_demo/db2.pid))"
if [ "$MODE" = "fenced" ]; then
cat <<'EOF'

fenced 模式示例（注意所有协议请求都要带 epoch，首个协调进程领到代 1）：
  curl -s 'localhost:19010/submit?epoch=1' -H 'Content-Type: application/json' -d '{
    "txid":"demo1","writes":{"db1":{"k":"a"},"db2":{"k":null}}}'
  curl -s 'localhost:19010/data?epoch=1'
  curl -s 'localhost:19010/txn/demo1/status?epoch=1'
  curl -s localhost:19011/debug/epoch
# 滚动接管：另起一个协调进程（共享 coord.sqlite，但 --epoch-file 必须不同），
# 它会领到代 2 并向两分库安装，旧代随即失效：
#   python3 -I coordinator.py --port 19020 --db run_demo/coord.sqlite \
#     --epoch-file run_demo/coord.epoch.g2 --fenced \
#     --db1-url http://127.0.0.1:19011 --db2-url http://127.0.0.1:19012
EOF
else
cat <<'EOF'

示例：
  curl -s localhost:19010/submit -H 'Content-Type: application/json' -d '{
    "txid":"demo1",
    "writes":{"db1":{"k":"a"},"db2":{"k":null}}
  }'
  curl -s localhost:19010/data
  curl -s localhost:19010/txn/demo1/status
EOF
fi
