import socket
import os
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler, FileSystemEvent
import sys
####################################################################################################################
#the function synchronize the folder to the server at the first time when the client run without id
def sendFolder(rootpath, socket):
    rootdir = os.path.realpath(rootpath)
    pathList = []
    folderList = []
    lenOfPathData = 0

    #recursive scrolling across the folder and create lists with all the folder paths and the file paths
    for root, subFolder, files in os.walk(rootdir):
        #the relative path from the file that the client synchronize
        unRealroot = os.path.relpath(root, rootpath)
        #fix the situation the root is empty and return a dot.
        if unRealroot == ".":
            unRealroot = ""
        for folder in subFolder:
            folderList.append(os.path.join(unRealroot, folder))
        for file in files:
            # I mean the root without the name of the root folder and all the prefix before
            pathList.append(os.path.join(root, file))
    # calculate the len of all the paths for the protocol whit the server
    #if the folder is empty
    if len(folderList) == 0:
        lenOfPathData = 0
    else:
        for file in folderList:
            lenOfPathData += len(file.encode("utf-8")) + len("//")
        #sub the last "//"
        lenOfPathData -= len("//")

    # sent all the paths without the root file
    message = ("0" * (20 - len(str(lenOfPathData)))) + str(lenOfPathData)
    #send the len of all the path folders in 20 bytes.
    socket.send(message.encode("utf-8"))
    #built the massage with all the path folders seperate by "//"
    massage = ""
    for file in folderList:
        massage += file
        massage += "//"
    # cut the last "//"
    massage = massage[:-2]
    #send the message
    socket.send(massage.encode("utf-8"))

    # finish to send the paths and start to send the files
    # for each file - sent the len of the path in 10 bytes,
    # the path, the len of the file in 20 bytes and the file.
    for file in pathList:
        massage = ""
        f = open(file, "rb")
        path = os.path.relpath(file, rootpath)
        readFile = read_file(f)
        massage = lenPathStr(path) + path + lenFileStr(readFile)
        socket.send(massage.encode("utf-8"))
        read_and_send_file(file, socket)
#####################################################################################################################
def read_file(file):
    sizeOfRead = 10000
    read = b""
    currentRead = file.read(sizeOfRead)
    while currentRead != b"":
        read += currentRead
        currentRead = file.read(sizeOfRead)
    return read
#####################################################################################################################
def read_and_send_file(absPath, socket):
    file = open(absPath, "rb")
    sizeOfRead = 100000
    currentRead = file.read(sizeOfRead)
    while currentRead != b"":
        try:
            socket.send(currentRead)
        except:
            pass
        currentRead = file.read(sizeOfRead)
    file.close()
#####################################################################################################################
#the function recieve all the files from the server at the first synchronize when the client connect with id
#(the client first recieve all the path for the folders in function recv_fldrs and this function calls recvfiles)
def recvfiles(sock, dirpath):
    global rootPath
    sock.settimeout(2)
    try:
        #recieve the len of the path
        data = sock.recv(10)
    except:
        return
    #while the len of the path is not empty- there are more paths to recieve
    while data != b'':
        path_size = int(data.decode('utf-8'))
        #recieve the path
        path = generate_path(sock.recv(path_size).decode('utf-8'))
        #recieve the len of the file
        file_size = int(sock.recv(20).decode('utf-8'))
        #recieve the file
        file = recv_file(file_size, sock)
        #join the rel path to the folder in the client computer
        file_path = os.path.abspath(os.path.join(rootPath, path))
        #create the file in the client folder
        if not os.path.isfile(file_path):
            f = open(file_path, 'wb')
            f.write(file)
            f.close()
        try:
            #recieve the next path, if its exists, else (when we have timeout) -  return
            data = sock.recv(10)
        except:
            return
#####################################################################################################################
#the function recieve all the folders from the server at the first synchronize when the clients connect with id
def recv_fldrs(sock, dirpath):
    global rootPath
    #recieve the len of all the paths
    path_arr_size = int(sock.recv(20).decode('utf-8'))
    #create list with all the paths by split
    folderData = sock.recv(path_arr_size).decode('utf-8').split('//')
    folder_paths = []
    #create folder with all the absolut paths
    if len(folderData) != 0:
        for folder in folderData:
            if folder != "":
                full_folder = os.path.abspath(os.path.join(dirpath, generate_path(folder)))
                folder_paths.append(full_folder)
    #for each path create the folder
    if len(folder_paths) != 0:
        for folder in folder_paths:
            if folder != '' and not os.path.isdir(folder):
                os.mkdir(folder)
    #create all the files
    recvfiles(sock, dirpath)
#####################################################################################################################
#the purpose of this function is to reviece big file, instead to recieve all the file, the client read 200 bytes
# every iteration
def recv_file(num_bytes, sock):
    recv_size = 1024
    if num_bytes < recv_size:
        return sock.recv(num_bytes)
    data = b''
    while len(data) < num_bytes:
        if num_bytes - len(data) < recv_size:
            data += sock.recv(num_bytes - len(data))
            return data
        data += sock.recv(recv_size)
    return data
#####################################################################################################################
def generate_path(path):
    if sys.platform.startswith('linux'):
        ret_path = path.replace('\\', os.sep)
    elif sys.platform.startswith('win32'):
        ret_path = path.replace('/', os.sep)
    else:
        ret_path = path
    return ret_path
####################################################################################################################
#this function is called when the client connect at the first time without id, the function create the socket,
#gets id, current version and calls "sendFolder" that synchronize the folder
def firstConnectionNoID(IP, portNumber, rootPath):
    global idNumber
    global version
    #this massege to the server is the protocol that notify the clients connect in the first time without id
    massageToServer = ("0"*140).encode("utf-8")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((IP, portNumber))
    s.send(massageToServer)
    sendFolder(rootPath, s)
    #recieve id and versoin from the server
    data = s.recv(140)
    idNumber = data[0:128].decode("utf-8")
    version = data[128:138].decode("utf-8")
    s.close()
    return
#####################################################################################################################
#this function is called when the client connect at the first time wit id, the function create the socket,
#gets the current version and calls "revc_foldrs" that synchronize the folder from the server to the client.
def firstConnectionWithID(IP, portNumber, rootPath):
    global version
    #this massage notify to the server the client connect with id at the first time from the current computer
    massageToServer = (idNumber + ("0"*12)).encode("utf-8")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((IP, portNumber))
    s.send(massageToServer)
    data = s.recv(140)
    version = data[128:138].decode("utf-8")
    recv_fldrs(s, rootPath)
    s.close()
#####################################################################################################################
#this function return the len of the path in string at format of 10 bytes (the path in bytes with encode utf-8)
def lenPathStr(path):
    return ("0"*(10 - len(str(len(path.encode("utf-8")))))) + str(len(path.encode("utf-8")))
#####################################################################################################################
##this function return the len of the file in string at format of 20 bytes
def lenFileStr(file):
    return ("0"*(20 - len(str(len(file))))) + str(len(file))
#####################################################################################################################
#the watchdog function on_created:
#the function is called by watchdog when file or directory is created
def created(event):
    global version
    #if the src_path, the path that is created, contains ".goutputstream", the temporary file in linux
    #ignore
    if ".goutputstream-" in event.src_path:
        return
    #if the path is not exists, ignore (fix mistakes of the os)
    if not os.path.isfile(event.src_path) and not os.path.isdir(event.src_path):
        return
    #before the client modify about the change, the client ask the server for changes
    #the s is the socket!
    #the parameter "11" notify to the server that i want to send a change
    s = askForChanges(IP, portNumber, rootPath, "11")
    increamentVersion()
    #the relative path
    path = os.path.relpath(event.src_path, rootPath)
    if event.is_directory:
        #send to the server that folder was creates, the len of the path and the path
        message = "addFold" + lenPathStr(path) + path
        s.send(message.encode("utf-8"))
    else:
         #open the file that is created and save its in variable
         f = open(os.path.join(rootPath, path), "rb")
         file = read_file(f)
         #send to the server that file was created, len of path, path, len of file and the file
         message = "addFile" + lenPathStr(path) + path + lenFileStr(file)
         s.send(message.encode("utf-8"))
         read_and_send_file(os.path.join(rootPath, path), s)
         time.sleep(0.01)
         f.close()
    #close the socket
    s.close()
#####################################################################################################################
#the watchdog function on_deleted:
#the function is called by watchdog when file or directory is deleted
def deleted(event):
    global version
    global deletedList
    # before the client modify about the change, the client ask the server for changes
    # the s is the socket!
    # the parameter "11" notify to the server that i want to send a change
    s = askForChanges(IP, portNumber, rootPath, "11")
    #the rel paths
    path = os.path.relpath(event.src_path, rootPath)
    #increment the version
    increamentVersion()
    #save the path, the purpose is to prevents loops between clients
    deletedList.append(event.src_path)
    if event.is_directory:
        #send that folder was deleted, the len of the path and the path
        message = "delFold" + lenPathStr(path) + path
    else:
        #the event is file
        #sent that file was deleted, the len of the path and the path
        message = "delFile" + lenPathStr(path) + path
    s.send(message.encode("utf-8"))
    s.close()
#####################################################################################################################
#the watchdog function on_modified:
#the function is called by watchdog when file or directory is modified
def modified(event):
    global version
    #if the opereting system modified a temporary file - ignore
    if ".goutputstream-" in event.src_path:
        return
    #if the path is not exsists - ignore
    if not os.path.isdir(event.src_path) and not os.path.isfile(event.src_path):
        return
    #if folder was modified - ignore
    if event.is_directory:
        return
    # before the client modify about the change, the client ask the server for changes
    # the s is the socket!
    # the parameter "11" notify to the server that i want to send a change
    s = askForChanges(IP, portNumber, rootPath, "11")
    #increments the version
    increamentVersion()
    #the relative path
    path = os.path.relpath(event.src_path, rootPath)
    f = open(os.path.join(rootPath, path), "rb")
    file = read_file(f)
    #sent the file is changed, the len of path, the path, the len of the file and the file
    message = "chgFile" + lenPathStr(path) + path + lenFileStr(file)
    s.send(message.encode("utf-8"))
    read_and_send_file(os.path.join(rootPath, path), s)
    f.close()
    s.close()
#####################################################################################################################
#the watchdog function on_moved:
#the function is called by watchdog when file or directory is moved
def moved(event):
    global version
    #if the operating system moved file, i want fo modified the destination file was changed
    if ".goutputstream-" in event.src_path:
        modified(FileSystemEvent(event.dest_path))
        return
    # before the client modify about the change, the client ask the server for changes
    # the s is the socket!
    # the parameter "11" notify to the server that i want to send a change
    s = askForChanges(IP, portNumber, rootPath, "11")
    #increment the version
    increamentVersion()
    #the relative source path
    sPath = os.path.relpath(event.src_path, rootPath)
    #the relative destination path
    dPath = os.path.relpath(event.dest_path, rootPath)
    message = ""
    if event.is_directory:
        message += "movFold"
    else:
        message += "movFile"
    #sent that file/folder was moved the len of source path,
    #the source path, the len of destination path and the destination path
    message += lenPathStr(sPath) + sPath + lenPathStr(dPath) + dPath
    s.send(message.encode("utf-8"))
    s.close()
#####################################################################################################################
#this function replaces the file from source path to destination path
#the first two parameters are relative
def replaceFile(sourcePath, destPath, rootpath):
    destFile = open(os.path.join(rootpath, destPath), "wb")
    sFile = open(os.path.join(rootpath, sourcePath), "rb")
    destFile.write(read_file(sFile))
    destFile.close()
    sFile.close()
    os.remove(os.path.join(rootpath, sourcePath))
#####################################################################################################################
#remove folder recursively
def removeFolder(sourcePath, rootPath):
    pathList = []
    folderList = []
    #create the lists with the folders paths and the files paths
    for root, subFolder, files in os.walk(os.path.join(rootPath, sourcePath)):
        for folder in subFolder:
            folderList.append(os.path.join(root, folder))
        for file in files:
            pathList.append(os.path.join(root, file))
    #remove all the files
    for path in pathList:
        os.remove(os.path.abspath(path))
    #remove all the folders
    for path in reversed(folderList):
        os.rmdir(os.path.abspath(path))
    #remove the folder
    if os.path.isdir(os.path.join(rootPath, sourcePath)):
        os.rmdir(os.path.join(rootPath, sourcePath))
#####################################################################################################################
#replace folder from source path to destination path recursively
#the parameters paths are relative
def replaceFolder(sourcePath, destPath, rootPath):
    pathList = []
    folderList = []

    for root, subFolder, files in os.walk(os.path.join(rootPath, sourcePath)):
        unRealroot = os.path.relpath(root, os.path.join(rootPath, sourcePath))
        if unRealroot == ".":
            unRealroot = ""
        for folder in subFolder:
            folderList.append(os.path.join(unRealroot, folder))
        for file in files:
            pathList.append(os.path.join(unRealroot, file))

    if not os.path.isdir(os.path.join(rootPath, destPath)):
        os.mkdir(os.path.join(rootPath, destPath))
    for relPath in folderList:
        if not os.path.isdir(os.path.join(rootPath, destPath, relPath)):
            os.mkdir(os.path.join(rootPath, destPath, relPath))
    for path in pathList:
        replaceFile(os.path.join(sourcePath, path), os.path.join(destPath, path), rootPath)
    removeFolder(sourcePath, rootPath)
#####################################################################################################################
#ask the server to changes and will do its
#the suffix is "00" if there is no changes from whachdog to the server, else the suffix is "11"
def askForChanges(IP, portNumber, rootPath, suffix):
    global version
    global idNumber
    global deletedList
    massage = idNumber + version + suffix
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((IP, portNumber))
    s.send(massage.encode("utf-8"))
    #reviece the current version by the server
    data = s.recv(140)
    version = data[128:138].decode("utf-8")
    data = b''
    #read command
    data = s.recv(7)
    #while there is more changes from the server
    #for each possible command, there is protocol
    while data.decode("utf-8") != "doneSnd":
        data = data.decode("utf-8")

        #add file
        if "addFile" == data:
            pathSize = int((s.recv(10)).decode("utf-8"))
            path = generate_path((s.recv(pathSize)).decode("utf-8"))
            fileSize = int((s.recv(20)).decode("utf-8"))
            file = recv_file(fileSize, s)
            #if I deleted the same file at last two times, ignore
            #the purpose is to prevent loops between clients
            if len(deletedList) > 2 and path in deletedList[-1] and path in deletedList[-2]:
                deletedList = []
            #if the file if already exist - ignore
            elif os.path.isfile(os.path.join(rootPath, path)):
                pass
            else:
                #create the file
                f = open(os.path.join(rootPath, path), "wb")
                f.write(file)
                f.close()

        #add folder
        elif "addFold" == data:
            pathSize = int((s.recv(10)).decode("utf-8"))
            path = generate_path((s.recv(pathSize)).decode("utf-8"))
            # if I deleted the same folder at last two times, ignore
            # the purpose is to prevent loops between clients
            if len(deletedList) >= 2 and path in str(deletedList[-1]) and path in str(deletedList[-2]):
                deletedList = []
            #if the folder already exists - ignore
            elif os.path.isdir(os.path.join(rootPath, path)):
                pass
            else:
                #create the directory
                os.mkdir(os.path.join(rootPath, path))

        #delete file
        elif "delFile" == data:
            pathSize = int((s.recv(10)).decode("utf-8"))
            path = generate_path((s.recv(pathSize)).decode("utf-8"))
            #if the file is not exists - ingore
            if not os.path.isfile(os.path.join(rootPath, path)):
                pass
            else:
                #remove the file
                os.remove(os.path.join(rootPath, path))

        #delete folder
        elif "delFold" == data:
            pathSize = int((s.recv(10)).decode("utf-8"))
            path = generate_path((s.recv(pathSize)).decode("utf-8"))
            #if the folder is not exists - ignore
            if not os.path.isdir(os.path.join(rootPath, path)):
                pass
            else:
                removeFolder(path, rootPath)

        #move file
        elif "movFile" == data:
            sourcePathSize = int((s.recv(10)).decode("utf-8"))
            sourcePath = generate_path((s.recv(sourcePathSize)).decode("utf-8"))
            destPathSize = int((s.recv(10)).decode("utf-8"))
            destPath = generate_path((s.recv(destPathSize)).decode("utf-8"))
            if os.path.isfile(os.path.join(rootPath, sourcePath)):
                replaceFile(sourcePath, destPath, rootPath)

        #move folfer
        elif "movFold" == data:
            sourcePathSize = int((s.recv(10)).decode("utf-8"))
            sourcePath = generate_path((s.recv(sourcePathSize)).decode("utf-8"))
            destPathSize = int((s.recv(10)).decode("utf-8"))
            destPath = generate_path((s.recv(destPathSize)).decode("utf-8"))
            if not os.path.isdir(os.path.join(rootPath, destPath)):
                os.makedirs(os.path.join(rootPath, destPath))
            if os.path.isdir(os.path.join(rootPath, sourcePath)):
                replaceFolder(sourcePath, destPath, rootPath)

        #change file
        elif "chgFile" == data:
            pathSize = int((s.recv(10)).decode("utf-8"))
            path = generate_path((s.recv(pathSize)).decode("utf-8"))
            fileSize = int((s.recv(20)).decode("utf-8"))
            file = recv_file(fileSize, s)
            preFile = open(os.path.join(rootPath, path), "rb")
            readFile = read_file(preFile)
            preFile.close()
            if file == readFile:
                pass
            else:
                if os.path.isfile(os.path.join(rootPath, path)):
                    f = open(os.path.join(rootPath, path), "wb")
                    f.write(file)
                    f.close()

        #read the next command
        data = s.recv(7)

    #if the suffix is "00", the main was called the function
    #the meaning is that there is no changes and we close the socket
    if suffix == "00":
        s.close()
    #return the socket
    return s
#####################################################################################################################
#the function increments the version
#i saved the version as string in 10 bytes
def increamentVersion():
    global version
    i = int(version)
    i += 1
    newVersion = str(i)
    version = ("0"*(10 - len(newVersion))) + newVersion
#####################################################################################################################
#main of the process
#global arguments
IP = sys.argv[1]
portNumber = int(sys.argv[2])
rootPath = generate_path(sys.argv[3])
timeToConnect = int(sys.argv[4])
idNumber = "0"*128
version = "0"*10
deletedList = []

#create monitor and observer
monitor = PatternMatchingEventHandler(["*"], None, False, True)
monitor.on_created = created
monitor.on_deleted = deleted
monitor.on_modified = modified
monitor.on_moved = moved

goRecursively = True
observer = Observer()
observer.schedule(monitor, rootPath, goRecursively)
#first connection
#is there is no id
if len(sys.argv) == 5:
    firstConnectionNoID(IP, portNumber, rootPath)
else:
    #there is id
    idNumber = sys.argv[5]
    firstConnectionWithID(IP, portNumber, rootPath)

#srart monitoring
observer.start()

while True:
    try:
        #sleep and ask the server for changes every "timeToConnect"
        time.sleep(timeToConnect)
        askForChanges(IP, portNumber, rootPath, "00")
    except:
        pass