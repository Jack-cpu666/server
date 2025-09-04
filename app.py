from flask import Flask, request, jsonify
from flask_cors import CORS
from flask_socketio import SocketIO, emit, join_room, leave_room
import os
import json
import base64
import zlib
import threading
import time
import random
import string
from datetime import datetime
from collections import deque
import hashlib
import asyncio
from concurrent.futures import ThreadPoolExecutor

app = Flask(__name__)
app.config['SECRET_KEY'] = 'rdp-server-secret-key'
CORS(app, origins="*")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet', 
                   ping_timeout=5, ping_interval=2, 
                   compression_threshold=1024, 
                   max_http_buffer_size=16777216)

connected_clients = {}
active_sessions = {}
frame_buffers = {}
connection_stats = {}
token_to_session = {}  # Maps tokens to session IDs
session_tokens = {}    # Maps session IDs to tokens
disconnected_hosts = {} # Track disconnected hosts for reconnection grace period
executor = ThreadPoolExecutor(max_workers=50)

def generate_token():
    """Generate a unique 6-character alphanumeric token"""
    while True:
        token = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
        if token not in token_to_session:
            return token

class PerformanceMonitor:
    def __init__(self):
        self.frame_times = deque(maxlen=100)
        self.bandwidth_usage = deque(maxlen=100)
        self.last_frame_time = time.time()
        self.total_bytes = 0
        
    def log_frame(self, data_size):
        current_time = time.time()
        self.frame_times.append(current_time - self.last_frame_time)
        self.bandwidth_usage.append(data_size)
        self.total_bytes += data_size
        self.last_frame_time = current_time
        
    def get_stats(self):
        if not self.frame_times:
            return {'fps': 0, 'avg_latency': 0, 'bandwidth': 0}
        return {
            'fps': 1.0 / (sum(self.frame_times) / len(self.frame_times)) if self.frame_times else 0,
            'avg_latency': sum(self.frame_times) / len(self.frame_times) * 1000,
            'bandwidth': sum(self.bandwidth_usage) / len(self.bandwidth_usage) if self.bandwidth_usage else 0,
            'total_bytes': self.total_bytes
        }

performance_monitor = PerformanceMonitor()

@app.route('/')
def home():
    return "OK"

@app.route('/health')
def health():
    return jsonify({"status": "healthy", "timestamp": datetime.now().isoformat()})

@socketio.on('connect')
def on_connect():
    client_id = request.sid
    connected_clients[client_id] = {
        'id': client_id,
        'connected_at': datetime.now().isoformat(),
        'type': None,
        'last_ping': time.time(),
        'reconnect_count': 0,
        'quality_level': 'high'
    }
    connection_stats[client_id] = PerformanceMonitor()
    emit('connected', {'client_id': client_id, 'server_time': time.time()})
    print(f"Client {client_id} connected")
    
    # Start heartbeat for this client
    def heartbeat():
        while client_id in connected_clients:
            try:
                socketio.emit('heartbeat', {'timestamp': time.time()}, room=client_id)
                time.sleep(2)
            except:
                break
    threading.Thread(target=heartbeat, daemon=True).start()

@socketio.on('disconnect')
def on_disconnect():
    client_id = request.sid
    if client_id in connected_clients:
        client_type = connected_clients[client_id].get('type')
        if client_type == 'host':
            # Handle host disconnection with grace period for reconnection
            for session_id, session in active_sessions.items():
                if session.get('host_id') == client_id:
                    token = session.get('token')
                    
                    # Store disconnected host info for reconnection grace period
                    disconnected_hosts[token] = {
                        'session_id': session_id,
                        'session_data': session.copy(),
                        'disconnect_time': time.time(),
                        'frame_buffer': frame_buffers.get(session_id)
                    }
                    
                    # Keep token mappings alive for reconnection
                    print(f"Host {client_id} disconnected, preserving token {token} for 30 seconds")
                    
                    # Notify viewers host is temporarily disconnected
                    socketio.emit('host_temporarily_disconnected', {
                        'message': 'Host connection lost. Waiting for reconnection...',
                        'token': token
                    }, room=session_id)
                    
                    # Clean up active session but preserve token mappings
                    if session_id in frame_buffers:
                        del frame_buffers[session_id]
                    del active_sessions[session_id]
                    break
        
        elif client_type == 'viewer':
            # Remove viewer from session
            for session_id, session in active_sessions.items():
                if client_id in session.get('viewers', []):
                    session['viewers'].remove(client_id)
                    # Notify host about viewer leaving
                    socketio.emit('viewer_left', {
                        'viewer_id': client_id,
                        'viewer_count': len(session['viewers'])
                    }, room=session_id)
                    break
        
        if client_id in connected_clients:
            del connected_clients[client_id]
        if client_id in connection_stats:
            del connection_stats[client_id]
        print(f"Client {client_id} disconnected")

@socketio.on('heartbeat_response')
def on_heartbeat_response(data):
    client_id = request.sid
    if client_id in connected_clients:
        connected_clients[client_id]['last_ping'] = time.time()
        latency = time.time() - data.get('timestamp', 0)
        connected_clients[client_id]['latency'] = latency * 1000

@socketio.on('register_as_host')
def on_register_host(data):
    client_id = request.sid
    connected_clients[client_id]['type'] = 'host'
    connected_clients[client_id]['hostname'] = data.get('hostname', 'Unknown')
    connected_clients[client_id]['resolution'] = data.get('resolution', {'width': 1920, 'height': 1080})
    
    # Check if this is a reconnection of a previously disconnected host
    token = None
    session_id = f"host_{client_id}"
    
    # Look for existing disconnected host that can be restored (no time limit)
    for existing_token, host_info in disconnected_hosts.items():
        # Always restore the previous session with same token - no expiry
        token = existing_token
        session_id = host_info['session_id']
        
        # Restore frame buffer if available
        if host_info['frame_buffer']:
            frame_buffers[session_id] = host_info['frame_buffer']
            
        disconnect_duration = int(time.time() - host_info['disconnect_time'])
        print(f"Host reconnected after {disconnect_duration}s, restoring token {token}")
        break
    
    # If no reconnection, generate new token
    if not token:
        token = generate_token()
        
    # Update mappings
    token_to_session[token] = session_id
    session_tokens[session_id] = token
    
    active_sessions[session_id] = {
        'host_id': client_id,
        'viewers': [],
        'created_at': datetime.now().isoformat(),
        'frame_rate': data.get('frame_rate', 15),
        'quality': data.get('quality', 'high'),
        'compression_enabled': True,
        'token': token,
        'hostname': data.get('hostname', 'Unknown')
    }
    
    # Remove from disconnected hosts if it was there
    if token in disconnected_hosts:
        del disconnected_hosts[token]
    
    frame_buffers[session_id] = {
        'last_frame': None,
        'frame_hash': None,
        'delta_frames': deque(maxlen=10)
    }
    
    join_room(session_id)
    emit('host_registered', {
        'session_id': session_id, 
        'token': token,
        'compression_enabled': True
    })
    print(f"Host {client_id} registered with session {session_id} and token {token}")

@socketio.on('register_as_viewer')
def on_register_viewer(data):
    client_id = request.sid
    token = data.get('token', '').upper().strip()
    
    # Find session by token
    if token not in token_to_session:
        emit('error', {'message': f'Invalid token: {token}. Please check the token and try again.'})
        return
        
    session_id = token_to_session[token]
    
    if session_id not in active_sessions:
        emit('error', {'message': 'Host session is no longer active'})
        return
    
    connected_clients[client_id]['type'] = 'viewer'
    connected_clients[client_id]['connected_to_token'] = token
    active_sessions[session_id]['viewers'].append(client_id)
    
    join_room(session_id)
    
    session_info = active_sessions[session_id]
    emit('viewer_registered', {
        'session_id': session_id,
        'token': token,
        'hostname': session_info.get('hostname', 'Unknown'),
        'host_connected_at': session_info.get('created_at')
    })
    
    # Notify host about new viewer
    socketio.emit('viewer_joined', {
        'viewer_id': client_id, 
        'viewer_count': len(session_info['viewers'])
    }, room=session_id)
    
    print(f"Viewer {client_id} joined session {session_id} using token {token}")

@socketio.on('screen_data')
def on_screen_data(data):
    client_id = request.sid
    session_id = None
    
    # Find the session this host belongs to
    for sid, session in active_sessions.items():
        if session.get('host_id') == client_id:
            session_id = sid
            break
    
    if session_id:
        # Process frame with advanced compression
        processed_data = process_frame_data(session_id, data)
        
        # Log performance metrics
        data_size = len(json.dumps(processed_data).encode())
        performance_monitor.log_frame(data_size)
        
        # Forward optimized screen data to all viewers
        socketio.emit('screen_update', processed_data, room=session_id, include_self=False)

def process_frame_data(session_id, data):
    """Advanced frame processing with delta encoding and compression"""
    try:
        frame_buffer = frame_buffers.get(session_id)
        if not frame_buffer:
            return data
            
        current_frame = data.get('image', '')
        frame_hash = hashlib.md5(current_frame.encode()).hexdigest()
        
        # Check if frame is identical (no changes)
        if frame_buffer['frame_hash'] == frame_hash:
            return {'type': 'no_change', 'timestamp': time.time()}
        
        # Apply compression
        compressed_data = data.copy()
        if len(current_frame) > 1024:  # Only compress if worth it
            try:
                compressed_image = base64.b64encode(
                    zlib.compress(base64.b64decode(current_frame), level=6)
                ).decode()
                compressed_data['image'] = compressed_image
                compressed_data['compressed'] = True
                compressed_data['original_size'] = len(current_frame)
                compressed_data['compressed_size'] = len(compressed_image)
            except Exception as e:
                print(f"Compression error: {e}")
                compressed_data['compressed'] = False
        
        # Update frame buffer
        frame_buffer['last_frame'] = current_frame
        frame_buffer['frame_hash'] = frame_hash
        frame_buffer['delta_frames'].append({
            'hash': frame_hash,
            'timestamp': time.time(),
            'size': len(current_frame)
        })
        
        compressed_data['frame_id'] = frame_hash[:8]
        compressed_data['server_timestamp'] = time.time()
        
        return compressed_data
        
    except Exception as e:
        print(f"Frame processing error: {e}")
        return data

@socketio.on('mouse_event')
def on_mouse_event(data):
    client_id = request.sid
    session_id = data.get('session_id')
    
    if session_id in active_sessions:
        # Add timestamp for latency measurement
        data['client_timestamp'] = time.time()
        # Forward mouse event to host with minimal delay
        host_id = active_sessions[session_id]['host_id']
        socketio.emit('mouse_event', data, room=host_id)

@socketio.on('keyboard_event')
def on_keyboard_event(data):
    client_id = request.sid
    session_id = data.get('session_id')
    
    if session_id in active_sessions:
        # Add timestamp for latency measurement
        data['client_timestamp'] = time.time()
        # Forward keyboard event to host with minimal delay
        host_id = active_sessions[session_id]['host_id']
        socketio.emit('keyboard_event', data, room=host_id)

@socketio.on('quality_adjustment')
def on_quality_adjustment(data):
    client_id = request.sid
    session_id = data.get('session_id')
    quality = data.get('quality', 'medium')
    
    if client_id in connected_clients:
        connected_clients[client_id]['quality_level'] = quality
        
    if session_id in active_sessions:
        active_sessions[session_id]['quality'] = quality
        # Notify host about quality change
        host_id = active_sessions[session_id]['host_id']
        socketio.emit('quality_change', {'quality': quality}, room=host_id)

@socketio.on('request_keyframe')
def on_request_keyframe(data):
    client_id = request.sid
    session_id = data.get('session_id')
    
    if session_id in active_sessions:
        host_id = active_sessions[session_id]['host_id']
        socketio.emit('request_keyframe', {}, room=host_id)

@socketio.on('performance_report')
def on_performance_report(data):
    client_id = request.sid
    if client_id in connection_stats:
        stats = connection_stats[client_id]
        stats.log_frame(data.get('frame_size', 0))
        
        # Adaptive quality based on performance
        fps = data.get('fps', 0)
        latency = data.get('latency', 0)
        
        if fps < 15 or latency > 200:
            emit('quality_recommendation', {'quality': 'low', 'reason': 'performance'})
        elif fps > 25 and latency < 100:
            emit('quality_recommendation', {'quality': 'high', 'reason': 'performance'})

@app.route('/sessions')
def get_sessions():
    enhanced_sessions = {}
    for session_id, session in active_sessions.items():
        enhanced_sessions[session_id] = {
            **session,
            'viewer_count': len(session.get('viewers', [])),
            'uptime': (datetime.now() - datetime.fromisoformat(session['created_at'])).total_seconds(),
            'performance': performance_monitor.get_stats(),
            'token': session.get('token', 'N/A')  # Include token in API response
        }
    return jsonify(enhanced_sessions)

@app.route('/validate_token/<token>')
def validate_token(token):
    token = token.upper().strip()
    if token in token_to_session:
        session_id = token_to_session[token]
        if session_id in active_sessions:
            session = active_sessions[session_id]
            return jsonify({
                'valid': True,
                'hostname': session.get('hostname', 'Unknown'),
                'created_at': session.get('created_at'),
                'viewer_count': len(session.get('viewers', [])),
                'quality': session.get('quality', 'high')
            })
    return jsonify({'valid': False, 'message': 'Invalid or expired token'})

@app.route('/active_tokens')
def get_active_tokens():
    """Debug endpoint to see all active tokens"""
    tokens_info = {}
    for token, session_id in token_to_session.items():
        if session_id in active_sessions:
            session = active_sessions[session_id]
            tokens_info[token] = {
                'session_id': session_id,
                'hostname': session.get('hostname', 'Unknown'),
                'viewers': len(session.get('viewers', [])),
                'created_at': session.get('created_at'),
                'status': 'active'
            }
        elif token in disconnected_hosts:
            host_info = disconnected_hosts[token]
            tokens_info[token] = {
                'session_id': host_info['session_id'],
                'hostname': host_info['session_data'].get('hostname', 'Unknown'),
                'viewers': 0,
                'created_at': host_info['session_data'].get('created_at'),
                'status': 'disconnected',
                'disconnect_time': host_info['disconnect_time']
            }
    return jsonify(tokens_info)

@app.route('/clients')
def get_clients():
    enhanced_clients = {}
    for client_id, client in connected_clients.items():
        stats = connection_stats.get(client_id, PerformanceMonitor()).get_stats()
        enhanced_clients[client_id] = {
            **client,
            'performance': stats,
            'connection_quality': 'excellent' if stats.get('fps', 0) > 20 else 'good' if stats.get('fps', 0) > 10 else 'poor'
        }
    return jsonify(enhanced_clients)

@app.route('/performance')
def get_performance():
    return jsonify({
        'server_stats': performance_monitor.get_stats(),
        'active_connections': len(connected_clients),
        'active_sessions': len(active_sessions),
        'server_uptime': time.time(),
        'memory_usage': get_memory_usage()
    })

def get_memory_usage():
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return {
            'rss': process.memory_info().rss,
            'vms': process.memory_info().vms,
            'cpu_percent': process.cpu_percent()
        }
    except ImportError:
        return {'error': 'psutil not available'}

def cleanup_stale_connections():
    """Clean up stale connections and sessions"""
    while True:
        try:
            current_time = time.time()
            stale_clients = []
            
            for client_id, client in connected_clients.items():
                if current_time - client.get('last_ping', current_time) > 30:
                    stale_clients.append(client_id)
            
            for client_id in stale_clients:
                print(f"Cleaning up stale client: {client_id}")
                if client_id in connected_clients:
                    del connected_clients[client_id]
                if client_id in connection_stats:
                    del connection_stats[client_id]
            
            # Clean up very old disconnected hosts (after 24 hours for memory management)
            very_old_hosts = []
            for token, host_info in disconnected_hosts.items():
                if current_time - host_info['disconnect_time'] > 86400:  # 24 hours
                    very_old_hosts.append(token)
            
            for token in very_old_hosts:
                disconnect_hours = int((current_time - disconnected_hosts[token]['disconnect_time']) / 3600)
                print(f"Cleaning up very old disconnected host token: {token} (disconnected {disconnect_hours}h ago)")
                host_info = disconnected_hosts[token]
                session_id = host_info['session_id']
                
                # Remove from memory after 24 hours
                if token in token_to_session:
                    del token_to_session[token]
                if session_id in session_tokens:
                    del session_tokens[session_id]
                    
                del disconnected_hosts[token]
            
            # Clean up empty sessions and associated tokens
            empty_sessions = []
            for session_id, session in active_sessions.items():
                if session.get('host_id') not in connected_clients:
                    empty_sessions.append(session_id)
            
            for session_id in empty_sessions:
                print(f"Cleaning up empty session: {session_id}")
                
                # Only clean up tokens if not in disconnected_hosts (grace period)
                token = session_tokens.get(session_id)
                if token and token not in disconnected_hosts:
                    if token in token_to_session:
                        del token_to_session[token]
                    if session_id in session_tokens:
                        del session_tokens[session_id]
                
                # Clean up session and buffers
                del active_sessions[session_id]
                if session_id in frame_buffers:
                    del frame_buffers[session_id]
            
            time.sleep(10)  # Run cleanup every 10 seconds
        except Exception as e:
            print(f"Cleanup error: {e}")
            time.sleep(10)

if __name__ == '__main__':
    # Start cleanup thread
    cleanup_thread = threading.Thread(target=cleanup_stale_connections, daemon=True)
    cleanup_thread.start()
    
    port = int(os.environ.get('PORT', 5000))
    socketio.run(app, host='0.0.0.0', port=port, debug=False, 
                use_reloader=False, log_output=True)