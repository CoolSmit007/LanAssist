from backend.Socket import socketClass
from backend.ServerSocket import serverSocketClass
from backend.FileManager import fileManager
from models.Command import Command
from models.enums.DataType import DataType
from models.enums.CommandType import CommandType

import queue
import threading as th
import json
import msgpack

from log_init import LOGGER
from config import CONFIGS

class connection:
    __receiveLock = th.Event()
    receiveThread=None
    __sendLock = th.Event()
    sendThread = None
    def __init__(self):
        self.socket = socketClass()
        self.server = serverSocketClass()
        self.fileManager = fileManager()
        self.folderQueue = queue.Queue()
        self.screenQueue = queue.Queue()
        self.audioQueue = queue.Queue()
        self.keyboardQueue = queue.Queue()
        self.mouseQueue = queue.Queue()
        self.sendingQueue = queue.Queue()
        
    def __receive(self):
        while self.__receiveLock.is_set():
            try:
                data = self.socket.receiveQueue.get(timeout=1.0)
            except queue.Empty:
                continue
            
            if(not isinstance(data, bytes)):
                LOGGER.error("Data is not a bytearray")  
                continue
            
            try:
                decodedData = data.rstrip(b'\x00').decode()
            except UnicodeDecodeError as error:
                LOGGER.error("Unicode Decode error %s",error)  
                continue
            
            if(not decodedData[-1]=='\n'):
                LOGGER.error("Data doesn't contain \\n as the last character")  
                continue
            
            try:
                jsonData = json.loads(decodedData[:-1])
            except json.JSONDecodeError as error:
                LOGGER.error("JSON Decode error %s",error)  
                continue
            
            bytesDataLength = jsonData.get("data_length")
            paddedLength = jsonData.get("padding_length")
            typeOfData = jsonData.get("type")
            
            if not bytesDataLength:
                LOGGER.error("No attribute 'data_length' found in JSON")  
                continue
            
            if not paddedLength:
                LOGGER.error("No attribute 'padded_length' found in JSON")  
                continue
            
            if not typeOfData:
                LOGGER.error("No attribute 'type' found in JSON")  
                continue
                
            finalData=bytes()
            while bytesDataLength>len(finalData):
                finalData += self.socket.receiveQueue.get()
                
            if(bytesDataLength!=len(finalData)):
                LOGGER.error("JSON final bytes length doesn't match data_length, data_length=%d, len of final bytes=%d",bytesDataLength,len(finalData))  
                continue
            
            if not all(bytesValue==0 for bytesValue in finalData[-paddedLength:]):
                LOGGER.error("Not all padded bytes are null bytes for padding_length=%d",paddedLength)  
                continue
            
            finalData = finalData[:-paddedLength] if paddedLength > 0 else finalData
                
            match typeOfData:
                case DataType.COMMAND.value:
                    self.__bifurcateCommands(finalData)
                case DataType.AUDIO.value:
                    self.audioQueue.put(finalData)
                case DataType.SCREEN.value:
                    self.screenQueue.put(finalData)
                case DataType.FILE.value:
                    self.fileManager.queue.put(finalData)
                case DataType.FOLDER.value:
                    self.folderQueue.put(finalData)
                case DataType.KEYBOARD.value:
                    self.keyboardQueue.put(finalData)
                case DataType.MOUSE.value:
                    self.mouseQueue.put(finalData)
            
            try:
                self.socket.receiveQueue.task_done()
            except ValueError:
                LOGGER.error("Marking more tasks as done than get in socket receive queue")
            
    def __bifurcateCommands(self,data):
        command: Command = Command.from_dict(msgpack.unpackb(data))
        if not command.type:
            LOGGER.error("Couldn't find command type in command data")
            return
        match command.type:
            case (CommandType.FILE_REQUEST.value | CommandType.ACCEPT_FILE.value 
                  | CommandType.REJECT_FILE.value | CommandType.ACK_FILE.value 
                  | CommandType.ERROR_FILE.value):
                self.fileManager.executeCommand(command)
                return
        
    def __startReceiveThread(self):
        self.__receiveLock.set()
        self.receiveThread=th.Thread(target=self.__receive)
        self.receiveThread.start()
        
    def __stopReceiveThread(self):
        self.__receiveLock.clear()
        if(self.receiveThread):
            self.receiveThread.join(timeout=10.0)
            if(self.receiveThread.is_alive()):
                return False
        return True
    
    def __send(self,type,data):
        if not isinstance(data,bytes):
            LOGGER.error("Data is not of type bytes")  
            raise Exception("Data is not of type bytes")
        
        if not isinstance(type,DataType):
            LOGGER.error("Type is not an enum of type DataType")  
            raise Exception("Type is not an enum of type DataType")
        
        bufferSize = CONFIGS.socketBufferSize
        padLength = (bufferSize - (len(data)%bufferSize))%bufferSize
        jsonData = {"type":type.value, "data_length":len(data)+padLength,"padding_length":padLength}
        encodedJSONData = (json.dumps(jsonData)+'\n').encode().ljust(bufferSize, b'\x00')
        paddedData = data.ljust(len(data) + padLength, b'\x00')
        self.socket.sendData(encodedJSONData)
        self.socket.sendData(paddedData)
    
    def __mainSend(self):
        while self.__sendLock.is_set():
            try:
                data = self.sendingQueue.get(timeout=1.0)
            except queue.Empty:
                continue
            
            if not isinstance(data,dict):
                LOGGER.error("Data is not a dictionary can't send")
                continue
            
            if not (data.get("type") and data.get("data")):
                LOGGER.error("Type or data not found in dictonary")
                continue
            
            try:
                self.__send(data.get("type"),data.get("data"))
            except Exception as error:
                LOGGER.error("An error occured while sending the data: %s",str(error))
                
            try:
                self.sendingQueue.task_done()
            except ValueError:
                LOGGER.error("Marking more tasks as done then get in connection send queue")
            
            callback = data.get("callback")
            if callable(callback):
                callback()
    
    def __startSendingThread(self):
        self.__sendLock.set()
        self.sendThread=th.Thread(target=self.__mainSend)
        self.sendThread.start()
        
    def __stopSendingThread(self):
        self.__sendLock.clear()
        if(self.sendThread):
            self.sendThread.join(timeout=10.0)
            if(self.sendThread.is_alive()):
                return False
        return True
            
    def connect(self,ip,port):
        LOGGER.info("Connecting to ip:%s and port %d:",ip,port)
        self.socket.connect(ip,port)
        self.socket.startReceiveThread()
        self.__startReceiveThread()
        self.__startSendingThread()
    
    def acceptConnectionAndStartThreads(self,socket):
        if not isinstance(socket,socketClass):
            LOGGER.error("Socket variabled is of class %s instead of socketClass",str(type(socket)))
            return
        LOGGER.info("Accepting connection from ip:%s and port %d:",socket.ip,socket.port)
        self.socket=socket 
        self.socket.startReceiveThread()
        self.__startReceiveThread()
        self.__startSendingThread()
        
    def startServer(self,ip,port,callbackFunction):
        LOGGER.info("Starting server bound to ip:%s and port %d:",ip,port)
        self.server.listen(ip,port)
        self.server.startWaitingConnectionThread(callbackFunction)
        
    def closeConnection(self):
        if(self.__stopReceiveThread() and self.__stopSendingThread()):
            socketClosed =  self.socket.closeSocket()
            serverClosed = self.server.closeServer()
            return socketClosed and serverClosed
        return False