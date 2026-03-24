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
// These bypass problematic shims that try to redefine non-configurable properties.
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
let browserConn = null; // The PeerJS connection to the dashboard
let pythonWs = null;   // The WebSocket connection to the ROS2 node

// ═══════════════════════════════════════════════════════════════
// PEERJS SETUP (P2P Link)
// ═══════════════════════════════════════════════════════════════
const peer = new Peer(ROBOT_PEER_ID, {
    host: '0.peerjs.com',
    port: 443,
    secure: true,
    path: '/',
    debug: 3 // Verbose logging
});

peer.on('open', (id) => {
    console.log(`>>> [PEER] Robot Online. ID: ${id}`);
});

peer.on('connection', (conn) => {
    if (browserConn) {
        console.log('>>> [PEER] New browser connection. Closing old one.');
        browserConn.close();
    }
    
    browserConn = conn;
    console.log(`>>> [PEER] Browser Connected: ${conn.peer}`);

    conn.on('data', (raw) => {
        try {
            const msg = JSON.parse(raw);
            
            // 1. Handle Internal Heartbeat
            if (msg.type === 'ping') {
                conn.send(JSON.stringify({ type: 'pong', ts: msg.ts }));
                return;
            }

            // 2. Relay to Python via IPC
            if (pythonWs && pythonWs.readyState === WebSocket.OPEN) {
                pythonWs.send(raw);
            } else {
                console.warn('>>> [BRIDGE] Received command but Python IPC is offline.');
            }
        } catch (e) {
            console.error('>>> [PEER] Bad Message Format:', raw);
        }
    });

    conn.on('close', () => {
        console.log('>>> [PEER] Browser Disconnected');
        browserConn = null;
    });

    conn.on('error', (err) => {
        console.error('>>> [PEER] Connection Error:', err);
    });
});

peer.on('error', (err) => {
    console.error('>>> [PEER] Global Error:', err);
});

// ═══════════════════════════════════════════════════════════════
// WEBSOCKET IPC SETUP (Local Link to Python)
// ═══════════════════════════════════════════════════════════════
const wss = new WebSocket.Server({ port: IPC_PORT });
console.log(`>>> [IPC] WebSocket Server listening on port ${IPC_PORT}`);

wss.on('connection', (ws) => {
    console.log('>>> [IPC] Python ROS2 Node Linked');
    pythonWs = ws;

    ws.on('message', (message) => {
        // Relay telemetry from Python to Browser
        if (browserConn && browserConn.open) {
            browserConn.send(message.toString());
        }
    });

    ws.on('close', () => {
        console.log('>>> [IPC] Python ROS2 Node Disconnected');
        pythonWs = null;
    });

    ws.on('error', (err) => {
        console.error('>>> [IPC] WebSocket Error:', err);
    });
});

// ═══════════════════════════════════════════════════════════════
// PROCESS LOGIC
// ═══════════════════════════════════════════════════════════════
process.on('SIGINT', () => {
    console.log('>>> [BRIDGE] Shutting down...');
    if (browserConn) browserConn.close();
    peer.destroy();
    process.exit();
});
