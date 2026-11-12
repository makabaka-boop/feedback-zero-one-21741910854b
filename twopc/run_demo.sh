#!/usr/bin/env bash
# 手动演示：启动三个独立进程（协调者 + db1 + db2），各自独立 SQLite 文件。
# 用法： ./run_demo.sh        启动
#        停止： kill $(cat run_demo/*.pid)
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p run_demo
PY=${PYTHON:-python3}

$PY -I shard.py --name db1 --port 19011 --db run_demo/db1.sqlite \
    >run_demo/db1.log 2>&1 &
echo $! > run_demo/db1.pid
$PY -I shard.py --name db2 --port 19012 --db run_demo/db2.sqlite \
    >run_demo/db2.log 2>&1 &
echo $! > run_demo/db2.pid
sleep 0.5
$PY -I coordinator.py --port 19010 --db run_demo/coord.sqlite \
    --db1-url http://127.0.0.1:19011 --db2-url http://127.0.0.1:19012 \
    >run_demo/coord.log 2>&1 &
echo $! > run_demo/coord.pid
sleep 0.8

echo "三进程已启动："
echo "  coordinator http://127.0.0.1:19010 (pid $(cat run_demo/coord.pid))"
echo "  db1         http://127.0.0.1:19011 (pid $(cat run_demo/db1.pid))"
echo "  db2         http://127.0.0.1:19012 (pid $(cat run_demo/db2.pid))"
cat <<'EOF'

示例：
curl -s localhost:19010/submit -H 'Content-Type: application/json' -d '{
  "txid":"demo1",
  "writes":{"db1":{"k":"a"},"db2":{"k":null}}
}'
curl -s localhost:19010/data
curl -s localhost:19010/txn/demo1/status
EOF
