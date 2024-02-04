# Uncomment this to pass the first stage
import socket


def main() -> None:
    # Create a TCP server socket that listens on the localhost address and port 6379
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    # Accept a connection from a client
    conn, addr = server_socket.accept()
    with conn:
        print(f"Connected by {addr}")

        while True:
            # Receive data from the client
            data = conn.recv(1024)
            print(f"Received {data}")

            if not data:
                break

            # Send data back to the client
            pong_response = "+PONG\r\n"
            conn.send(pong_response.encode())


if __name__ == "__main__":
    main()
