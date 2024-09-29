# redis_server.py
import socket
import threading
from db import SimpleRedis

class RedisServer(SimpleRedis):
    def __init__(self, host='127.0.0.1', port=6379):
        super().__init__()
        self.host = host
        self.port = port

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        print(f"Server listening on {self.host}:{self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"Connection from {addr} established.")
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

    def handle_client(self, client_socket):
        while True:
            request = client_socket.recv(1024).decode('utf-8')
            if not request:
                break
            
            command, *args = request.split()
            response = self.execute_command(command.lower(), args)
            client_socket.send(response.encode('utf-8'))

        client_socket.close()

    def execute_command(self, command, args):
        try:
            if command == 'set':
                key, value, *expiration = args
                expiration = int(expiration[0]) if expiration else None
                self.set(key, value, expiration)
                return "OK"
            elif command == 'get':
                key = args[0]
                value = self.get(key)
                return value if value is not None else "nil"
            elif command == 'delete':
                key = args[0]
                self.delete(key)
                return "OK"
            else:
                return "ERROR: Unknown command"
        except Exception as e:
            return f"ERROR: {str(e)}"

if __name__ == "__main__":
    server = RedisServer()
    server.start_server()
