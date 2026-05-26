import socket
import threading
import json
import time

HOST = '0.0.0.0'
PORT = 5000

class BootstrapServer:
    def __init__(self):
        self.peers = {}   # (ip, port) -> last_heartbeat
        self.groups = {}  # group_name -> (leader_ip, leader_port)
        self.lock = threading.Lock()

    def recv_full(self, conn):
        """Fix lỗi 2: Đọc toàn bộ dữ liệu cho đến khi parse JSON thành công"""
        chunks = []
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            chunks.append(chunk)
            try:
                json.loads(b''.join(chunks).decode('utf-8'))
                break
            except json.JSONDecodeError:
                continue
        return b''.join(chunks).decode('utf-8')

    def handle_client(self, conn, addr):
        try:
            data = self.recv_full(conn)
            if not data:
                return
            msg = json.loads(data)

            peer_port = msg.get('port')
            peer_addr = (addr[0], peer_port)

            if msg.get('type') == 'CREATE_GROUP':
                group_name = msg.get('group')
                with self.lock:
                    if group_name in self.groups:
                        conn.send(json.dumps({
                            'type': 'CREATE_GROUP_ERROR',
                            'reason': f"Nhóm '{group_name}' đã tồn tại."
                        }).encode('utf-8'))
                        return
                    self.groups[group_name] = peer_addr
                    self.peers[peer_addr] = time.time()

            elif msg.get('type') in ('REGISTER', 'HEARTBEAT'):
                with self.lock:
                    self.peers[peer_addr] = time.time()

            active_peers, active_groups = self.get_active_peers_and_groups()
            conn.send(json.dumps({
                'type': 'PEER_LIST',
                'peers': active_peers,
                'groups': active_groups
            }).encode('utf-8'))

        except Exception as e:
            print(f"Lỗi khi xử lý {addr}: {e}")
        finally:
            conn.close()

    def get_active_peers_and_groups(self):
        current_time = time.time()
        active = []
        active_groups = {}
        with self.lock:
            stale_peers = [p for p, ts in self.peers.items() if current_time - ts > 15]
            for p in stale_peers:
                del self.peers[p]
                stale_groups = [g for g, leader in self.groups.items() if leader == p]
                for g in stale_groups:
                    new_leader = next((peer for peer in self.peers if peer != p), None)
                    if new_leader:
                        self.groups[g] = new_leader
                        print(f"[~] Nhóm '{g}' chuyển leader sang {new_leader}")
                    else:
                        del self.groups[g]
                        print(f"[-] Nhóm '{g}' đã bị xóa do không còn thành viên online.")
                print(f"[-] Peer {p} đã offline (timeout).")

            for p in self.peers:
                active.append({'ip': p[0], 'port': p[1]})
            active_groups = {g: [l[0], l[1]] for g, l in self.groups.items()}
        return active, active_groups

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((HOST, PORT))
        server.listen(10)
        print(f"[*] Bootstrap Server đang lắng nghe tại {HOST}:{PORT}")
        while True:
            conn, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()

if __name__ == '__main__':
    BootstrapServer().start()