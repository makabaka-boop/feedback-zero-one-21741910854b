#!/usr/bin/env bash
# 手动演示：启动三个独立进程（协调者 + db1 + db2），各自独立 SQLite 文件。
# 用法：
#   ./run_demo.sh            普通模式（兼容旧协议，无代）
#   ./run_demo.sh fenced     fenced 代模式（协调进程携带 epoch，分库代校验）
#   ./run_demo.sh feed       整批续采模式（--changefeed：/snapshot 与 /changes）
#   ./run_demo.sh fenced-feed 代 + 整批续采同时启用
# 停止： kill $(cat run_demo/*.pid)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p run_demo
PY=${PYTHON:-python3}
MODE=${1:-normal}

FENCED_FLAG=""
EPOCH_ARG=""
FEED_FLAG=""
case "$MODE" in
  fenced)
    FENCED_FLAG="--fenced"
    EPOCH_FILE="run_demo/coord.epoch.g1"
    EPOCH_ARG="--epoch-file $EPOCH_FILE"
    ;;
  feed)
    FEED_FLAG="--changefeed"
    ;;
  fenced-feed)
    FENCED_FLAG="--fenced"
    EPOCH_FILE="run_demo/coord.epoch.g1"
    EPOCH_ARG="--epoch-file $EPOCH_FILE"
    FEED_FLAG="--changefeed"
    ;;
  normal)
    ;;
  *)
    echo "未知模式: $MODE（可选 normal | fenced | feed | fenced-feed）" >&2
    exit 2
    ;;
esac

$PY -I shard.py --name db1 --port 19011 --db run_demo/db1.sqlite $FENCED_FLAG \
    >run_demo/db1.log 2>&1 &
echo $! > run_demo/db1.pid
$PY -I shard.py --name db2 --port 19012 --db run_demo/db2.sqlite $FENCED_FLAG \
    >run_demo/db2.log 2>&1 &
echo $! > run_demo/db2.pid
sleep 0.5
# shellcheck disable=SC2086
$PY -I coordinator.py --port 19010 --db run_demo/coord.sqlite $EPOCH_ARG \
    --db1-url http://127.0.0.1:19011 --db2-url http://127.0.0.1:19012 \
    $FENCED_FLAG $FEED_FLAG \
    >run_demo/coord.log 2>&1 &
echo $! > run_demo/coord.pid
sleep 0.8

echo "三进程已启动（模式: $MODE）："
echo "  coordinator http://127.0.0.1:19010 (pid $(cat run_demo/coord.pid))"
echo "  db1         http://127.0.0.1:19011 (pid $(cat run_demo/db1.pid))"
echo "  db2         http://127.0.0.1:19012 (pid $(cat run_demo/db2.pid))"
if [ "$MODE" = "fenced" ] || [ "$MODE" = "fenced-feed" ]; then
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
fi
if [ "$MODE" = "feed" ] || [ "$MODE" = "fenced-feed" ]; then
  EPOCH_SUFFIX=""
  [ "$MODE" = "fenced-feed" ] && EPOCH_SUFFIX="?epoch=1"
cat <<EOF

整批续采（changefeed）示例（fenced 下路径带 $EPOCH_SUFFIX 代参数）：
  curl -s localhost:19010/snapshot$EPOCH_SUFFIX
  curl -s localhost:19010/submit$EPOCH_SUFFIX -H 'Content-Type: application/json' -d '{
    "txid":"demo1","writes":{"db1":{"k":null},"db2":{"k":null}}}'
  curl -s localhost:19010/snapshot$EPOCH_SUFFIX
  curl -s localhost:19010/changes$EPOCH_SUFFIX -H 'Content-Type: application/json' -d '{"after":0}'
EOF
fi
if [ "$MODE" = "normal" ]; then
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
