import socket

# create a socket object
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# get local machine name
host = socket.gethostname()

# connect to the server on the port
client_socket.connect((host, 9999))

while True:
    # get user input
    query = input(">> ")
    
    # send the message to the server
    client_socket.send(query.encode('ascii'))

    # receive the response from the server
    response = client_socket.recv(1024)

    # print the response to the console
    print("Reply from server : ",response.decode('ascii'))

# close the client connection
client_socket.close()