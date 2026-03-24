import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.qos import QoSProfile, ReliabilityPolicy, HistoryPolicy, DurabilityPolicy
from sensor_msgs.msg import CompressedImage, LaserScan
from nav_msgs.msg import OccupancyGrid, Path
from std_msgs.msg import String, Float32, Int32
from geometry_msgs.msg import Twist, PoseWithCovarianceStamped, Quaternion, PoseStamped
from rcl_interfaces.msg import Log
import tf2_ros
import base64
import cv2
import numpy as np
import math
import sys
import subprocess
import os
import signal
import csv
import json
import asyncio
import websockets
import threading
import time

# ── Configuration ──────────────────────────────────────────────────────────
NODE_BRIDGE_URL = "ws://localhost:8765"

# ── QoS Profiles ──────────────────────────────────────────────────────────

_sensor_qos = QoSProfile(
    reliability=ReliabilityPolicy.BEST_EFFORT,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

_map_qos = QoSProfile(
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.TRANSIENT_LOCAL,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

_log_qos = QoSProfile(
    depth=100
)

_volatile_map_qos = QoSProfile( # Added for incompatible publishers
    reliability=ReliabilityPolicy.RELIABLE,
    durability=DurabilityPolicy.VOLATILE,
    history=HistoryPolicy.KEEP_LAST,
    depth=1
)

# ── Async WebSocket Bridge ──────────────────────────────────────────────────
class AsyncBridge:
    def __init__(self):
        self.ws = None
        self.loop = asyncio.new_event_loop()
        self.queue = asyncio.Queue()
        self.on_message_callback = None
        self._running = True

    def set_on_message(self, callback):
        self.on_message_callback = callback

    async def _connect_and_listen(self):
        while self._running:
            try:
                print(f">>> [IPC] Connecting to Node Bridge at {NODE_BRIDGE_URL}...")
                async with websockets.connect(NODE_BRIDGE_URL, max_size=10*1024*1024) as websocket:
                    print(">>> [IPC] Connected to Node Bridge")
                    self.ws = websocket
                    
                    # Task for sending messages from the queue
                    send_task = asyncio.create_task(self._send_loop())
                    # Task for receiving messages
                    recv_task = asyncio.create_task(self._recv_loop())
                    
                    done, pending = await asyncio.wait(
                        [send_task, recv_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    for task in pending:
                        task.cancel()
                    
            except Exception as e:
                print(f"ERROR: [IPC] Connection failed ({e}). Retrying in 3s...")
                self.ws = None
                await asyncio.sleep(3)

    async def _send_loop(self):
        while self.ws:
            msg = await self.queue.get()
            try:
                await self.ws.send(msg)
            except:
                break

    async def _recv_loop(self):
        while self.ws:
            try:
                raw = await self.ws.recv()
                if self.on_message_callback:
                    self.on_message_callback(json.loads(raw))
            except:
                break

    def send(self, type, payload):
        if not self._running: return
        msg = json.dumps({"type": type, **payload})
        self.loop.call_soon_threadsafe(self.queue.put_nowait, msg)

    def start(self):
        threading.Thread(target=self._run_loop, daemon=True).start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect_and_listen())

    def stop(self):
        self._running = False
        self.loop.stop()

bridge = AsyncBridge()

# ── ROS2 Subscribers ────────────────────────────────────────────────────────────

class ImageSubscriber(Node):
    def __init__(self, node_name, topic_name, event_name, fps_limit=10):
        super().__init__(node_name)
        self.subscription = self.create_subscription(CompressedImage, topic_name, self.listener_callback, _sensor_qos)
        self.event_name = event_name
        self.latest_frame = None
        self._fps_limit = fps_limit
        self._running = True
        self._thread = threading.Thread(target=self.emit_loop, daemon=True)
        self._thread.start()

    def listener_callback(self, msg):
        self.latest_frame = bytes(msg.data)

    def emit_loop(self):
        interval = 1.0 / self._fps_limit
        while self._running:
            start_time = time.monotonic()
            if self.latest_frame:
                try:
                    frame_encoded = base64.b64encode(self.latest_frame).decode('utf-8')
                    bridge.send(self.event_name, {"data": frame_encoded})
                    self.latest_frame = None
                except: pass
            
            elapsed = time.monotonic() - start_time
            sleep_time = max(0, interval - elapsed)
            time.sleep(sleep_time)

class BatterySubscriber(Node):
    def __init__(self):
        super().__init__('web_battery_sub')
        self.percentage = 0.0
        self.is_charging = False
        self.subscription = self.create_subscription(Float32, '/robot2/battery_percentage', self.listener_callback, 10)
        self.charging_subscription = self.create_subscription(Int32, '/robot2/batt_ind', self.charging_callback, 10)

    def listener_callback(self, msg):
        self.percentage = round(float(msg.data), 1)
        self.emit_status()

    def charging_callback(self, msg):
        self.is_charging = (msg.data == 0)
        self.emit_status()

    def emit_status(self):
        bridge.send('battery', {
            'percentage': self.percentage,
            'charging': self.is_charging,
            'unplug_required': (self.percentage >= 100.0 and self.is_charging)
        })

class LogSubscriber(Node):
    def __init__(self):
        super().__init__('web_log_sub')
        self.levels = {10: "DEBUG", 20: "INFO", 30: "WARN", 40: "ERROR", 50: "FATAL"}
        self.subscription = self.create_subscription(Log, '/rosout', self.listener_callback, _log_qos)

    def listener_callback(self, msg):
        if "Nav2 Health" in msg.msg or "lifecycle_manager_navigation" in msg.msg:
            status = "OK" if "active" in msg.msg.lower() or "Health" in msg.msg else "ERROR"
            bridge.send('nav2_status', {'status': status, 'msg': msg.msg})

        log_data = {
            'time': time.strftime("%H:%M:%S", time.localtime()),
            'name': msg.name,
            'level': self.levels.get(msg.level, "UNKNOWN"),
            'msg': msg.msg
        }
        bridge.send('ros_log', log_data)

class NavDataSubscriber(Node):
    def __init__(self, tf_buffer):
        super().__init__('web_nav_sub')
        self.tf_buffer = tf_buffer
        self.last_map = None
        self.latest_local_cost = None
        self.latest_global_cost = None
        self.latest_scan = None
        
        self.map_sub = self.create_subscription(OccupancyGrid, '/robot2/map', self.map_callback, _map_qos)
        self.local_cost_sub = self.create_subscription(OccupancyGrid, '/robot2/local_costmap/costmap', self.local_costmap_callback, _map_qos)
        self.global_cost_sub = self.create_subscription(OccupancyGrid, '/robot2/global_costmap/costmap', self.global_costmap_callback, _map_qos)
        self.plan_sub = self.create_subscription(Path, '/robot2/plan', self.plan_callback, 10)
        self.scan_sub = self.create_subscription(LaserScan, '/robot2/scan', self.scan_callback, _sensor_qos)
        self.initial_pose_pub = self.create_publisher(PoseWithCovarianceStamped, '/robot2/initialpose', 10)
        self.goal_pub = self.create_publisher(PoseStamped, '/robot2/goal_pose', 10)
        self.map_name_sub = self.create_subscription(String, '/robot2/map_name', self.map_name_callback, _volatile_map_qos)
        
        self.current_map_name = "New_Map"
        
        self.create_timer(1.0/5.0, self.emit_latest_telemetry)
        self.pose_timer = self.create_timer(0.1, self.publish_pose)

    def process_occ_grid(self, msg, color_map, transparency=True):
        width, height = msg.info.width, msg.info.height
        if width == 0 or height == 0: return None
        data = np.array(msg.data).reshape((height, width))
        img = np.zeros((height, width, 4 if transparency else 3), dtype=np.uint8)
        if transparency:
            img[data == 0] = [color_map['free'][0], color_map['free'][1], color_map['free'][2], 0]
            img[data == 100] = [color_map['occupied'][0], color_map['occupied'][1], color_map['occupied'][2], 255]
            img[data == -1] = [color_map['unknown'][0], color_map['unknown'][1], color_map['unknown'][2], 0]
        else:
            img[data == 0] = color_map['free']
            img[data == 100] = color_map['occupied']
            img[data == -1] = color_map['unknown']
        img = cv2.flip(img, 0)
        _, buffer = cv2.imencode('.png', img)
        return base64.b64encode(buffer).decode('utf-8')

    def map_callback(self, msg):
        now = time.time()
        if hasattr(self, '_last_map_emit_time') and (now - self._last_map_emit_time < 3.0): return
            
        colors = {'free': [215, 215, 215], 'occupied': [45, 45, 45], 'unknown': [180, 180, 200]}
        img_b64 = self.process_occ_grid(msg, colors, transparency=False)
        if img_b64:
            self.last_map = {
                'image': img_b64, 'resolution': float(msg.info.resolution),
                'width': int(msg.info.width), 'height': int(msg.info.height),
                'origin_x': float(msg.info.origin.position.x), 'origin_y': float(msg.info.origin.position.y)
            }
            self._last_map_emit_time = now
            bridge.send('map_data', self.last_map)

    def emit_last_map(self):
        if self.last_map: bridge.send('map_data', self.last_map)
        self.load_map_goals(self.current_map_name)

    def map_name_callback(self, msg):
        new_map_name = msg.data.strip()
        if new_map_name != self.current_map_name:
            self.current_map_name = new_map_name
            self.load_map_goals(self.current_map_name)

    def load_map_goals(self, map_name):
        try:
            csv_path = f"/home/robot1/foodbot_ws/home_maps/{map_name}_goals.csv"
            goals = []
            if os.path.exists(csv_path):
                with open(csv_path, mode='r') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        goals.append({
                            'name': row['goal_name'],
                            'x': float(row['x']),
                            'y': float(row['y']),
                            'z': float(row['z']),
                            'w': float(row['w'])
                        })
                bridge.send('map_goals', {'map': map_name, 'goals': goals})
            else:
                bridge.send('map_goals', {'map': map_name, 'goals': [], 'error': 'FILE_NOT_FOUND'})
        except Exception as e:
            print(f"ERROR: Failed to load goals for {map_name}: {e}")

    def local_costmap_callback(self, msg): self.latest_local_cost = msg
    def global_costmap_callback(self, msg): self.latest_global_cost = msg
    def scan_callback(self, msg): self.latest_scan = msg

    def emit_latest_telemetry(self):
        if self.latest_local_cost:
            msg = self.latest_local_cost
            self.latest_local_cost = None
            colors = {'free': [0, 0, 0], 'occupied': [0, 0, 255], 'unknown': [0, 0, 0]}
            img_b64 = self.process_occ_grid(msg, colors, transparency=True)
            if img_b64:
                ox, oy = float(msg.info.origin.position.x), float(msg.info.origin.position.y)
                yaw = 0.0
                if self.tf_buffer and msg.header.frame_id != 'map':
                    try:
                        t = self.tf_buffer.lookup_transform('map', msg.header.frame_id, rclpy.time.Time())
                        ox, oy = ox + t.transform.translation.x, oy + t.transform.translation.y
                        q = t.transform.rotation
                        yaw = float(math.atan2(2*(q.w*q.z + q.x*q.y), 1 - 2*(q.y*q.y + q.z*q.z)))
                    except: pass
                bridge.send('local_costmap', {'image': img_b64, 'resolution': float(msg.info.resolution), 'width': int(msg.info.width), 'height': int(msg.info.height), 'origin_x': ox, 'origin_y': oy, 'yaw': yaw})

        if self.latest_global_cost:
            msg = self.latest_global_cost
            self.latest_global_cost = None
            colors = {'free': [0, 0, 0], 'occupied': [255, 0, 255], 'unknown': [0, 0, 0]}
            img_b64 = self.process_occ_grid(msg, colors, transparency=True)
            if img_b64:
                ox, oy = float(msg.info.origin.position.x), float(msg.info.origin.position.y)
                yaw = 0.0
                if self.tf_buffer and msg.header.frame_id != 'map':
                    try:
                        t = self.tf_buffer.lookup_transform('map', msg.header.frame_id, rclpy.time.Time())
                        ox, oy = ox + t.transform.translation.x, oy + t.transform.translation.y
                        q = t.transform.rotation
                        yaw = float(math.atan2(2*(q.w*q.z + q.x*q.y), 1 - 2*(q.y*q.y + q.z*q.z)))
                    except: pass
                bridge.send('global_costmap', {'image': img_b64, 'resolution': float(msg.info.resolution), 'width': int(msg.info.width), 'height': int(msg.info.height), 'origin_x': ox, 'origin_y': oy, 'yaw': yaw})

        if self.latest_scan:
            msg = self.latest_scan
            self.latest_scan = None
            points = []
            for i in range(0, len(msg.ranges), 8):
                r = msg.ranges[i]
                if msg.range_min < r < msg.range_max:
                    angle = msg.angle_min + i * msg.angle_increment + math.pi
                    points.append({'x': float(r * math.cos(angle)), 'y': float(r * math.sin(angle))})
            bridge.send('scan_data', {'points': points})

    def plan_callback(self, msg):
        bridge.send('path_data', {'path': [{'x': float(p.pose.position.x), 'y': float(p.pose.position.y)} for p in msg.poses]})

    def publish_pose(self):
        if not self.tf_buffer: return
        try:
            try: trans = self.tf_buffer.lookup_transform('map', 'robot2/base_link', rclpy.time.Time())
            except: trans = self.tf_buffer.lookup_transform('map', 'base_link', rclpy.time.Time())
            
            q = trans.transform.rotation
            yaw = float(math.atan2(2*(q.w*q.z + q.x*q.y), 1 - 2*(q.y*q.y + q.z*q.z)))
            bridge.send('robot_pose', {
                'x': float(trans.transform.translation.x), 
                'y': float(trans.transform.translation.y), 
                'yaw': yaw,
                'stamp': trans.header.stamp.sec + trans.header.stamp.nanosec * 1e-9
            })
        except:
            bridge.send('robot_pose', {'x': 0.0, 'y': 0.0, 'yaw': 0.0, 'stamp': None})

# ── Process Command Logic ───────────────────────────────────────────
teleop_pub = None
nav_node = None
terminal_process = None

def on_bridge_message(msg):
    global teleop_pub, nav_node, terminal_process
    mtype = msg.get('type')

    if mtype == 'cmd_vel':
        cmd = msg.get('command')
        if cmd and teleop_pub:
            t = Twist()
            speed = 0.3 if cmd in ['W','S'] else 0.6
            if cmd == 'W': t.linear.x = speed
            elif cmd == 'S': t.linear.x = -speed
            elif cmd == 'A': t.angular.z = speed
            elif cmd == 'D': t.angular.z = -speed
            teleop_pub.publish(t)

    elif mtype == 'request_map':
        if nav_node: nav_node.emit_last_map()

    elif mtype == 'send_goal':
        if nav_node:
            try:
                g = PoseStamped()
                g.header.stamp = nav_node.get_clock().now().to_msg()
                g.header.frame_id = 'map'
                g.pose.position.x = float(msg['x'])
                g.pose.position.y = float(msg['y'])
                g.pose.orientation.z = float(msg['z'])
                g.pose.orientation.w = float(msg['w'])
                nav_node.goal_pub.publish(g)
            except Exception as e: print(f"ERROR: send_goal failed: {e}")

    elif mtype == 'terminal_command':
        command = msg.get('command')
        if not command: return
        
        # KILL logic for terminal shell
        if terminal_process and terminal_process.poll() is None:
            try: os.killpg(os.getpgid(terminal_process.pid), signal.SIGTERM)
            except: pass

        if command == 'stop':
            bridge.send('terminal_output', {'data': "\n>>> PROCESS_TERMINATED\n"})
            return

        try:
            terminal_process = subprocess.Popen(
                f"bash -c '{command}'", shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, preexec_fn=os.setsid
            )
            def stream():
                for line in iter(terminal_process.stdout.readline, b''):
                    bridge.send('terminal_output', {'data': line.decode('utf-8', errors='replace')})
                terminal_process.stdout.close()
            threading.Thread(target=stream, daemon=True).start()
        except: pass

# ── ROS2 Runner ───────────────────────────────────────────────────

def start_ros2():
    global teleop_pub, nav_node
    try:
        if not rclpy.ok(): rclpy.init()
        node = rclpy.create_node('ros_p2p_bridge')
        teleop_pub = node.create_publisher(Twist, '/robot2/cmd_vel_cam_top', 10)
        tf_buffer = tf2_ros.Buffer()
        tf_listener = tf2_ros.TransformListener(tf_buffer, node)
        
        executor = MultiThreadedExecutor(num_threads=12)
        nav_node = NavDataSubscriber(tf_buffer)
        nodes = [node, 
                 ImageSubscriber('web_cam_top_sub', '/robot2/camera_top/camera_top/color/image_raw/compressed', 'camera_top', 6),
                 ImageSubscriber('web_cam_bottom_sub', '/robot2/camera_bottom/camera_bottom/color/image_raw/compressed', 'camera_bottom', 6),
                 BatterySubscriber(), 
                 nav_node, 
                 LogSubscriber()]
        for n in nodes: executor.add_node(n)
        
        print(">>> ROS 2 Bridge ONLINE")
        executor.spin()
    except Exception as e: print(f"ERROR: ROS2 died: {e}", file=sys.stderr)
    finally:
        if rclpy.ok(): rclpy.shutdown()

if __name__ == '__main__':
    bridge.set_on_message(on_bridge_message)
    bridge.start()
    start_ros2()