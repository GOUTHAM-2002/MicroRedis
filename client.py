
#sample client code
import socket

def redis_client():
    host = '127.0.0.1'
    port = 6379

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        client_socket.connect((host, port))
        
        while True:
            command = input("Enter command (SET, GET, DELETE) or 'exit' to quit: ")
            if command.lower() == 'exit':
                break

            client_socket.send(command.encode('utf-8'))
            response = client_socket.recv(1024).decode('utf-8')
            print(f"Response: {response}")

if __name__ == "__main__":
    redis_client()
