// ── Global Error Resilience ──
process.on('uncaughtException', (err) => {
    console.error('>>> [CRITICAL] Uncaught Exception:', err.message);
    if (err.stack) console.error(err.stack);
});
process.on('unhandledRejection', (reason, p) => {
    console.error('>>> [CRITICAL] Unhandled Rejection at:', p, 'reason:', reason);
});

const { JSDOM } = require('jsdom');
const dom = new JSDOM('', {
    url: 'http://localhost',
    userAgent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
});
const { window } = dom;

// --- Browser Globals for Node.js ---
global.window = window;
global.document = window.document;
global.navigator = window.navigator;
global.location = window.location;

const wrtc = require('@roamhq/wrtc');
const WS = require('ws');
global.WebSocket = WS;
window.WebSocket = WS; 

// Mirror WebRTC to window & global for PeerJS
const RTCPrototypes = [
    { name: 'RTCPeerConnection', val: wrtc.RTCPeerConnection },
    { name: 'RTCSessionDescription', val: wrtc.RTCSessionDescription },
    { name: 'RTCIceCandidate', val: wrtc.RTCIceCandidate }
];

RTCPrototypes.forEach(({ name, val }) => {
    window[name] = val;
    global[name] = val;
});

// ── Stability Shims for webrtc-adapter ──
wrtc.RTCIceCandidate.prototype.foundation = '';
wrtc.RTCIceCandidate.prototype.relayProtocol = '';
wrtc.RTCPeerConnection.prototype.connectionState = 'new';
wrtc.RTCPeerConnection.prototype.sctp = null;

const { Peer } = require('peerjs');
const WebSocket = require('ws');

// --- Configuration ---
const ROBOT_PEER_ID = process.env.ROBOT_ID || 'robot-petpooja-1';
const IPC_PORT = process.env.IPC_PORT || 8765;

// --- State ---
const browserConns = new Set(); // Support multiple concurrent dashboards
let pythonWs = null;   // The WebSocket connection to the ROS2 node

// ═══════════════════════════════════════════════════════════════
// PEERJS SETUP (P2P Link)
// ═══════════════════════════════════════════════════════════════
const peer = new Peer(ROBOT_PEER_ID, {
    host: '0.peerjs.com',
    port: 443,
    secure: true,
    path: '/',
    debug: 2 
});

peer.on('open', (id) => {
    console.log(`>>> [PEER] Robot Online. ID: ${id}`);
});

peer.on('connection', (conn) => {
    console.log(`>>> [PEER] New DASHBOARD_LINK established: ${conn.peer}`);
    browserConns.add(conn);
    
    // Request full initial state for the new connection
    if (pythonWs && pythonWs.readyState === WebSocket.OPEN) {
        pythonWs.send(JSON.stringify({ type: 'request_map' }));
    }

    conn.on('data', (raw) => {
        try {
            const msg = JSON.parse(raw);
            if (msg.type === 'ping') {
                conn.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
                return;
            }

            // Relay commands from ANY connected dashboard to Python
            if (pythonWs && pythonWs.readyState === WebSocket.OPEN) {
                pythonWs.send(raw);
            }
        } catch (e) { }
    });

    const cleanup = () => {
        if (browserConns.has(conn)) {
            console.log(`>>> [PEER] Connection removed: ${conn.peer}`);
            browserConns.delete(conn);
        }
    };

    conn.on('close', cleanup);
    conn.on('error', (err) => {
        console.error('>>> [PEER] Connection Error:', err.message);
        cleanup();
    });
});

peer.on('error', (err) => {
    if (err.type === 'network') {
        console.error('>>> [PEER] Network Error (DNS/Server Offline). Will retry...');
    } else {
        console.error('>>> [PEER] Global Error:', err.type, err.message);
    }
});

// ═══════════════════════════════════════════════════════════════
// WEBSOCKET IPC SETUP (Local Link to Python)
// ═══════════════════════════════════════════════════════════════
const wss = new WebSocket.Server({ 
    port: IPC_PORT,
    maxPayload: 10 * 1024 * 1024 // 10MB
});
console.log(`>>> [IPC] Internal Server Listening: ${IPC_PORT}`);

wss.on('connection', (ws) => {
    console.log('>>> [IPC] Python ROS2 Node Linked');
    pythonWs = ws;

    ws.on('message', (message) => {
        // Broadcast telemetry to ALL connected dashboards
        const rawMsg = message.toString();
        browserConns.forEach(conn => {
            if (conn.open) {
                try {
                    conn.send(rawMsg);
                } catch(e) {
                    browserConns.delete(conn);
                }
            } else {
                browserConns.delete(conn);
            }
        });
    });

    ws.on('close', () => {
        console.log('>>> [IPC] Python ROS2 Node Disconnected');
        pythonWs = null;
    });

    ws.on('error', (err) => {
        console.error('>>> [IPC] WebSocket Error:', err.message);
    });
});

// ═══════════════════════════════════════════════════════════════
// PROCESS LOGIC
// ═══════════════════════════════════════════════════════════════
process.on('SIGINT', () => {
    console.log('>>> [BRIDGE] Shutting down...');
    browserConns.forEach(c => c.close());
    peer.destroy();
    process.exit();
});
