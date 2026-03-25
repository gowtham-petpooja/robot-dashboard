#!/bin/bash
# Robot P2P Bridge — Force Reset & Restart

echo ">>> [SYSTEM] Killing zombie bridge processes..."
pkill -f "node bridge.js" || true
pkill -f "ros_p2p_bridge.py" || true
fuser -k 8765/tcp || true

echo ">>> [SYSTEM] Waiting for sockets to clear (3s)..."
sleep 3

echo ">>> [SYSTEM] Starting Node Media Bridge..."
cd robot_bridge
node bridge.js > ../bridge.log 2>&1 &
NODE_PID=$!

echo ">>> [SYSTEM] Starting ROS2 P2P Bridge..."
cd ..
python3 ros_p2p_bridge.py > ros.log 2>&1 &
PY_PID=$!

echo ">>> [DONE] Systems restarted. Check logs: bridge.log, ros.log."
echo ">>> Node PID: $NODE_PID, Python PID: $PY_PID"
