import socket
import threading
from sql_parser import parse
import write
# create a socket object
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# get local machine name
host = socket.gethostname()

# bind the socket to a public host and a well-known port
server_socket.bind((host, 9999))

# Right now just handles 5 clients , change later
server_socket.listen(5)

# list to keep track of client sockets
client_sockets = []

# list to keep track of client threads
threads = []

pipe = write.init_pipe()
def handle_client(client_socket, addr):
    while True:
        # receive the message from the client
        query = client_socket.recv(1024)

        if query:
            query = query.decode('ascii')
            print("Recieved : ",query)

            # Call  Function for Query Processing Here
            response = str(parse(query,pipe))
            # send a response to the client
            response = f"Received: {query}. Response: {response}"
            client_socket.send(response.encode('ascii'))
        else:
            # close the client socket
            client_socket.close()
            print(f"Client {addr} disconnected")
            break

while True:
    # wait for a client to connect
    client_socket, addr = server_socket.accept()

    print(f"Got a connection from {addr}")

    # create a new thread to handle the client
    thread = threading.Thread(target=handle_client, args=(client_socket, addr))
    thread.start()

    # add the new thread to the list of client threads
    threads.append(thread)
