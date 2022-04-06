import socket
import sys
import os
import time
import random

# grand root defines the root of all client folders
grand_root = os.path.abspath('../../networksEx2')

# class that represents all data relating to client - id, change history array, the root folder he belongs to
class Client:
  def __init__(self, version, change_hist, root_folder):
    self.version = version
    self.change_hist = change_hist
    self.root_folder = root_folder


#  seperate and send file by chunks to client
def read_and_send_file(absPath, socket):
    file = open(absPath, "rb")
    # chunksize
    sizeOfRead = 100000
    currentRead = file.read(sizeOfRead)
    while currentRead != b"":
        try:
            socket.send(currentRead)
        except:
            pass
        currentRead = file.read(sizeOfRead)
    file.close()


# read a file from the local fileSystem in chunks
def read_file(file):
    sizeOfRead = 10000
    read = b""
    currentRead = file.read(sizeOfRead)
    while currentRead != b"":
        read += currentRead
        currentRead = file.read(sizeOfRead)
    return read



# returns the string of the length of a file in the 20 bytes format
def lenFileStr(file):
    return ("0"*(20 - len(str(len(file))))) + str(len(file))


# send the entire directory and all sub directories to client
def sendFolder(rootpath, socket):
    rootdir = os.path.realpath(rootpath)
    rootFolderName = os.path.split(rootdir)[1]
    emptyList = []
    pathList = []
    folderList = []
    lenOfPathData = 0

    # recursive scrolling across the folder and create lists with all the folder paths and the file paths
    for root, subFolder, files in os.walk(rootdir):
        unRealroot = os.path.relpath(root, rootpath)
        if unRealroot == ".":
            unRealroot = ""
        for folder in subFolder:
            folderList.append(os.path.join(unRealroot, folder))
        for file in files:
            # I mean the root without the name of the root folder and all the prefix before
            pathList.append(os.path.join(root, file))
    # calculate the len of all the paths for the protocol whit the server
    if len(folderList) == 0:
        lenOfPathData = 0
    else:
        for file in folderList:
            lenOfPathData += len(file.encode("utf-8")) + len("//")
        lenOfPathData -= len("//")

    # sent all the paths without the root file
    message = ("0" * (20 - len(str(lenOfPathData)))) + str(lenOfPathData)
    socket.send(message.encode("utf-8"))

    massage = ""
    for file in folderList:
        massage += file
        massage += "//"
    # cut the last "//"
    massage = massage[:-2]
    socket.send(massage.encode("utf-8"))

    # finish to send the paths and start to send the files
    for file in pathList:
        massage = ""
        f = open(file, "rb")
        path = os.path.relpath(file, rootpath)
        readFile = read_file(f)
        massage = lenPathStr(path) + path + lenFileStr(readFile)
        socket.send(massage.encode("utf-8"))
        read_and_send_file(os.path.join(rootpath, path), socket)


# returns the the string representation of a path in the 10 bytes format
def lenPathStr(path):
    return ("0"*(10 - len(str(len(path.encode('utf-8')))))) + str(len(path.encode('utf-8')))


# translates path from windows to linux or vice versa
def generate_path(path):
    if sys.platform.startswith('linux'):
        ret_path = path.replace('\\', os.sep)
    elif sys.platform.startswith('win32'):
        ret_path = path.replace('/', os.sep)
    return ret_path


def recvfile(sock, dirpath):
    try:
        path_size = sock.recv(10)
    except:
        return
    # receive files from client until client is finished sending
    while path_size != b'':
        # check path size
        path_size = int(path_size.decode('utf-8'))
        path = generate_path(sock.recv(path_size).decode('utf-8'))
        # check file size
        file_size = int(sock.recv(20).decode('utf-8'))
        # pull the file from the TCP buffer into file
        file = pull_file(file_size, sock)
        file_path = os.path.abspath(os.path.join(grand_root, dirpath, path))
        if not os.path.isfile(file_path):
            f = open(file_path, 'wb')
            f.write(file)
            f.close()
        try:
            path_size = sock.recv(10)
        except:
            return


# receive all folders from a client
def recv_fldrs(sock, dirpath):
    global grand_root
    path_arr_size = int(sock.recv(20).decode('utf-8'))
    folderData = sock.recv(path_arr_size).decode('utf-8').split('//')
    folder_paths = []
    # iterate over received data and add to arrays
    for folder in folderData:
        full_folder = os.path.abspath(os.path.join(dirpath, generate_path(folder)))
        folder_paths.append(full_folder)
    # iterate over folders and add to client root folder
    for folder in folder_paths:
        if folder != '' and not os.path.isdir(folder):
            os.mkdir(folder)
    # receive the files
    recvfile(sock, dirpath)
    return


# generate random id for a client
def get_rand_id():
    upper_chars = {chr(i) for i in (range(65, 91))}
    lower_chars = {chr(i) for i in (range(97, 123))}
    nums = {'1', '2', '3', '4', '5', '6', '7', '8', '9', '0'}
    id_chars = nums.union(upper_chars).union(lower_chars)
    rand_id = ''
    for i in range(128):
        rand_id += random.choice(list(id_chars))
    return rand_id


# initiate full sync with client - send the entire folder and its sub-folders and files
def full_sync(client_id, socket):
    global client_database
    sendFolder(client_database[client_id].root_folder, socket)


# update the client- send relevant missing commands to client and receive client updates if relevant
def update(client_id, curr_client, bi_way_flag, socket, received_ver):
    # check for differences between the client version and the sever version
    if received_ver != curr_client.version:
        # iterate over the remaining changes the client does not have and send them- command + path + file
        for i in range(int(received_ver), int(curr_client.version)):
            # parse the command and path
            command = curr_client.change_hist[i][0:7]
            path_size = curr_client.change_hist[i][7:17]
            file_path = curr_client.change_hist[i][17:]
            # check the type of command and send to client accordingly
            if command == "addFile" or command == "chgFile":
                if os.path.isfile(os.path.join(grand_root, client_id, file_path)):
                    f = open(os.path.join(grand_root, client_id, file_path), 'rb')
                    file_send = read_file(f)
                    file_len = lenFileStr(file_send)
                    socket.send(command.encode('utf-8'))
                    socket.send(path_size.encode('utf-8'))
                    socket.send(file_path.encode('utf-8'))
                    socket.send(file_len.encode('utf-8'))
                    read_and_send_file(os.path.join(grand_root, client_id, file_path), socket)
                    f.close()
            else:
                socket.send(command.encode('utf-8') + path_size.encode('utf-8') + file_path.encode('utf-8'))
        socket.send("doneSnd".encode('utf-8'))
        # check if the client has changes to send after he received the update
        if int(bi_way_flag):
            receive(client_id, curr_client, socket)
    # if the client is updated and wanted to send the sever updates
    elif int(bi_way_flag):
        socket.send("doneSnd".encode('utf-8'))
        receive(client_id, curr_client, socket)
    #  either way send that the server is finished sending
    elif not int(bi_way_flag):
        socket.send("doneSnd".encode('utf-8'))


# receive updates to folder from client according to a command - "addFile", "delFile", etc.
def receive(client_id, client, socket):
    try:
        data = socket.recv(17)
    except:
        return
    # parse the command from the data
    command = data[0:7].decode('utf-8')
    # parse path size received in ten bytes
    path_size = int(data[7:17])
    # pull path for the command, decode and translate it to current OS
    path = generate_path(socket.recv(path_size).decode('utf-8'))
    path_size = lenPathStr(path)
    # increase the clients version on the server by one according to the format
    client.version = increment_version(client.version)
    # add the command to the change history array
    client.change_hist.append(data.decode('utf-8') + path)
    # final absoulute path of the command- where to execute
    final_path = os.path.join(grand_root, client_id, path)
    relpath = os.path.join(path)
    # add a file
    if command == "addFile":
        # pull the file size sent in 20 bytes then pull the file
        file_size = int(socket.recv(20))
        file = pull_file(file_size, socket)
        # checking if the file already exists
        if not os.path.isfile(final_path):
            f = open(final_path, 'wb')
            f.write(file)
            f.close()
    # change contents of a file
    elif command == "chgFile":
        # file size over 20 bytes
        file_size = int(socket.recv(20))
        # pull the file according to size
        file = pull_file(file_size, socket)
        # checking if the file exists
        if os.path.isfile(final_path):
            g = open(final_path, 'rb')
            # if the file the client wishes to change is the same as the file he sent, do nothing
            if read_file(g) == file:
                g.close()
                return
            g.close()
            os.remove(final_path)
            f = open(final_path, 'wb')
            f.write(file)
            f.close()
    # add a new folder
    elif command == "addFold":
        if not os.path.isdir(final_path):
            os.mkdir(final_path)
    # delete a file
    elif command == "delFile":
        # checking if the file exists
        if os.path.isfile(final_path):
            os.remove(final_path)
        # if the file to delete is actually a folder delete the folder and change the client history accordingly
        # caused by bugs in the watchdog program running on windows and creating wrongly-typed events
        elif os.path.isdir(final_path):
            delete_Folder(final_path)
            client.change_hist.pop()
            client.change_hist.append("delFold" + data[7:17].decode('utf-8') + path)
    # delete a folder
    elif command == "delFold":
        if os.path.isdir(final_path):
            delete_Folder(final_path)
    # move a file
    elif command == "movFile":
        # the command movFile will be followed by the destination path after the dest path size
        dst_bytes = int(socket.recv(10).decode('utf-8'))
        dst_path = generate_path(socket.recv(dst_bytes).decode('utf-8'))
        # check if the file exists
        if os.path.isfile(os.path.join(grand_root, client_id, path)):
            replaceFile(relpath, dst_path, os.path.join(grand_root, client_id))
        # change the cient history according to the received dst paths
        client.change_hist.pop()
        client.change_hist.append(data.decode('utf-8') + path + lenPathStr(dst_path) + dst_path)
    # move a folder
    elif command == "movFold":
        dst_bytes = int(socket.recv(10).decode('utf-8'))
        dst_path = generate_path(socket.recv(dst_bytes).decode('utf-8'))
        if not os.path.isdir(os.path.join(grand_root, client_id, dst_path)):
            os.makedirs(os.path.join(grand_root, client_id, dst_path))
        if os.path.isdir(os.path.join(grand_root, client_id, path)):
            replaceFolder(relpath, dst_path, os.path.join(grand_root, client_id))
        client.change_hist.pop()
        client.change_hist.append(data.decode('utf-8') + path + lenPathStr(dst_path) + dst_path)


# removes a folder iteratively
def removeFolder(sourcePath, rootPath):
    emptyList = []
    pathList = []
    folderList = []
    # walk in src directory
    for root, subFolder, files in os.walk(os.path.join(rootPath, sourcePath)):
        for folder in subFolder:
            folderList.append(os.path.join(root, folder))
        for file in files:
            pathList.append(os.path.join(root, file))
    # for every file and folder(reversed) delete
    for path in pathList:
        os.remove(os.path.abspath(path))
    for path in reversed(folderList):
        os.rmdir(os.path.abspath(path))
    if os.path.isdir(os.path.join(rootPath, sourcePath)):
        os.rmdir(os.path.join(rootPath, sourcePath))


# moves a folder from one location to another
def replaceFolder(sourcePath, destPath, rootPath):
    emptyList = []
    pathList = []
    folderList = []
    # walk in the source directory
    for root, subFolder, files in os.walk(os.path.join(rootPath, sourcePath)):
        unRealroot = os.path.relpath(root, os.path.join(rootPath, sourcePath))
        if unRealroot == ".":
            unRealroot = ""
        for folder in subFolder:
            folderList.append(os.path.join(unRealroot, folder))
        for file in files:
            pathList.append(os.path.join(unRealroot, file))
    # create new dst directory and subfiles and subfolders
    if not os.path.isdir(os.path.join(rootPath, destPath)):
        os.mkdir(os.path.join(rootPath, destPath))
    for relPath in folderList:
        if not os.path.isdir(os.path.join(rootPath, destPath, relPath)):
            os.mkdir(os.path.join(rootPath, destPath, relPath))
    for path in pathList:
        replaceFile(os.path.join(sourcePath, path), os.path.join(destPath, path), rootPath)
    removeFolder(sourcePath, rootPath)


# moves a file by opening a copy in dst and deleting the original
def replaceFile(sourcePath, destPath, rootpath):
    destFile = open(os.path.join(rootpath, destPath), "wb")
    sFile = open(os.path.join(rootpath, sourcePath), "rb")
    destFile.write(read_file(sFile))
    destFile.close()
    sFile.close()
    os.remove(os.path.join(rootpath, sourcePath))


# receives data via TCP in chunks of 200 bytes to ensure no data is lost
def pull_file(num_bytes, sock):
    # chunksize
    recv_size = 1024
    # if the number of bytes to receive is smaller than the chunk just pull it from the socket
    if num_bytes < recv_size:
        return sock.recv(num_bytes)
    data = b''
    # pulling bytes from the socket until we reach the limit
    while len(data) < num_bytes:
        # if you have less than recv_size bytes left to pull pull the remaining bytes and return the data
        if num_bytes - len(data) < recv_size:
            data += sock.recv(num_bytes - len(data))
            return data
        data += sock.recv(recv_size)
    return data


# deletes a folder recursively
def delete_Folder(path):
    if not os.listdir(path):
        os.rmdir(path)
        return
    for item in os.listdir(path):
        if os.path.isfile(os.path.join(path, item)):
            os.remove(os.path.join(path, item))
        else:
            delete_Folder(os.path.join(path, item))
    os.rmdir(path)


# increase the client version by 1
def increment_version(version):
    # version is stored as string, and must be represented as 10 bytes exactly
    i = int(version)
    i += 1
    newVersion = str(i)
    version = ("0"*(10 - len(newVersion))) + newVersion
    return version


# take initial 140 bytes of sync data and decide what function is to be called
def sync_client(client_data, database, socket):
    # client id will be sent in the first 128 bytes
    client_id = client_data[0:128].decode('utf-8')
    # client version in the following 10 bytes
    client_version = client_data[128:138].decode('utf-8')
    # c_flag is 00 if client has no updates to push to server and 11 otherwise
    c_flag = client_data[138:]
    # if a client addresses server with id 0, the server will assign him one and send it back with version 1
    # then creates a new client in the Client database
    if client_id == '0'*128:
        # client change history array includes the command and the paths relevant to the command
        change_hist = [str]
        # new id is generated randomly
        new_id = get_rand_id()
        # create a new client with his folder in the D.B.
        root_folder = os.path.join(grand_root, new_id)
        curr_client = Client(increment_version(client_version), change_hist, root_folder)
        # add new client to the D.B.
        database[new_id] = curr_client
        #make a new directory for the new client
        os.mkdir(root_folder)
        print(new_id)
        # receive the clients folder in full
        recv_fldrs(socket, root_folder)
        socket.send((new_id + '0000000001' + '00').encode('utf-8'))

    # client exists and is connecting for first time
    elif client_id in database and int(client_version) == 0:
        # send the appropriate message back to client with current version and id
        message_back = client_id + database[client_id].version + c_flag.decode('utf-8')
        client_socket.send(message_back.encode('utf-8'))
        # initiate full sync with client - send all contents of local to directory to client
        full_sync(client_id, client_socket)

    # client exists and is updated and has no changes to send, no action is needed except sending back the message
    elif client_id in database and client_version == database[client_id].version and not int(c_flag):
        message_back = client_id + database[client_id].version + c_flag.decode('utf-8')
        client_socket.send(message_back.encode('utf-8'))
        # sending doneSnd to signify the end of transmission
        client_socket.send("doneSnd".encode('utf-8'))

    # client exists and is not updated and has changes to send
    elif client_id in database:
        message_back = client_id + database[client_id].version + c_flag.decode('utf-8')
        client_socket.send(message_back.encode('utf-8'))
        # update the client according to the flag
        update(client_id, database[client_id], c_flag, client_socket, client_version)


client_database = {}
port = int(sys.argv[1])
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.bind(('', port))
server.listen(5)
dirPaths = []
i = 0
# main server loop
while True:
    # receive client
    client_socket, client_address = server.accept()
    client_socket.settimeout(2)
    # first 140 bytes are a unique protocol containing the client id , version, and an update flag
    data = client_socket.recv(140)
    # sync client handles choosing the correct course of action for the current client
    sync_client(data, client_database, client_socket)





