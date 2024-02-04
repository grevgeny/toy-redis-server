# Uncomment this to pass the first stage
import socket
import threading


def handle_connection(conn: socket.socket) -> None:
    PONG = "+PONG\r\n".encode()

    while conn:
        # Receive data from the client
        data = conn.recv(1024)
        print(f"Received {data}")

        if not data:
            continue

        # Send data back to the client
        conn.send(PONG)


def main() -> None:
    # Create a TCP server socket that listens on the localhost address and port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    # Start listening for incoming connections
    while True:
        # Accept a connection from a client
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        # Handle the connection in a new thread
        thread = threading.Thread(target=handle_connection, args=(conn,))
        thread.start()


if __name__ == "__main__":
    main()
