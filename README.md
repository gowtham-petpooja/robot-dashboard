# Robot Dashboard - P2P WebRTC Core

A decentralised, high-performance robot dashboard using PeerJS (WebRTC) for direct browser-to-robot telemetry and control.

## Key Features
- **P2P Architecture**: No central server; direct encrypted DataChannels between operator and robot.
- **Static Frontend**: Fully compatible with GitHub Pages or any static host.
- **ROS 2 Integration**: Includes a Node.js bridge to relay ROS 2 topics over WebRTC.
- **Rich Telemetry**: Real-time support for Map, Path, Scan, Battery, and dual Image feeds.

## Quick Start

### 1. Robot Configuration & Start
1. Navigate to `robot_bridge/`.
2. Copy `.env.template` to `.env`.
3. Set your `ROBOT_ID=robot1` and a secure `ROBOT_PASSWORD`.
4. Run `npm install && npm start`.

### 2. ROS 2 Bridge
In a separate terminal with your ROS 2 environment sourced:
```bash
python3 ros_p2p_bridge.py
```

### 3. Operator Dashboard
1. Open `index.html` in a modern browser.
2. Enter the Robot Peer ID (`robot1`).
3. Enter your Access Password.
4. Click **Establish Link**.

## Directory Structure
- `index.html`: Optimized static operator dashboard.
- `robot_bridge/`: Node.js signaling and WebRTC shim for server-side operation.
- `ros_p2p_bridge.py`: Python-based ROS 2 topic aggregator and IPC client.