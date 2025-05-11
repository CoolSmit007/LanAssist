import queue
import threading as th
import uuid
import msgpack
import os

from models.Command import Command
from models.enums.CommandType import CommandType
from models.enums.DataType import DataType
from models.enums.FileStatus import FileStatus
from backend.File import file
from models.file.FileRequest import FileRequest

from util.FileUtil import decodeReceivingPackage
from log_init import LOGGER

class fileManager:
    __receiveLock = th.Event()
    __receiveThread = None
    def __init__(self, sendingQueue:queue.Queue):
        self.queue = queue.Queue()
        self.sendingQueue:queue.Queue = sendingQueue
        self.fileMap: dict[str,file] = {}
        self.__receiveLock.set()
        self.__startReceiveThread()
        self.autoAccept = False
        self.downloadLocation = None
        
    def executeCommand(self,command:Command):
        match command.type:
            # handle all command types.
            case CommandType.FILE_REQUEST.value:
                fileRequest = FileRequest.from_dict(command.data)
                
            case CommandType.ACK_FILE.value:
                if not command.data.get("uuid"):
                    LOGGER.error("No uuid found in command with type: %s",CommandType.ACK_FILE.name)
                    return
                uuid = command.data.get("uuid")
                if not self.fileMap.get(uuid):
                    LOGGER.error("No file object found with uuid: %s",uuid)
                    return
                self.fileMap.get(uuid).unblockSend()  
            case CommandType.ERROR_FILE.value:
                if not command.data.get("uuid"):
                    LOGGER.error("No uuid found in command with type: %s",CommandType.ERROR_FILE.name)
                    return
                if not command.data.get("error_code"):
                    LOGGER.error("No error_code found in command with type: %s",CommandType.ERROR_FILE.name)
                    return
                self.handleErrors(command.data.get("uuid"), command.data.get("error_code"))
                
    def handleErrors(self, id:str, errorCode:str):
        match errorCode:
            case "id_already_exists":
                LOGGER.error("Receiver already had the same uuid mapped to different file.")
                self.fileMap[id].error = "Some error occured while sending this file try again"
                self.fileMap[id].closeFile()
            case _:
                LOGGER.error("No handling of error code: %s",errorCode)
        
    def __receive(self):
        while self.__receiveLock.is_set():
            try:
                data = self.queue.get(timeout=1.0)
            except queue.Empty:
                continue
            unpackedData: dict = decodeReceivingPackage(data)
            uuid = unpackedData.get("uuid")
            if not self.fileMap.get(uuid):
                    LOGGER.error("No file object found with uuid: %s",uuid)
                    return
            self.fileMap.get(uuid).receive(unpackedData.get("data"))
                
    
    def __startReceiveThread(self):
        self.__receiveLock.set()
        self.__receiveThread=th.Thread(target=self.__receive)
        self.__receiveThread.start()
        
    def __stopReceiveThread(self):
        self.__receiveLock.clear()
        if(self.__receiveThread):
            self.__receiveThread.join(timeout=10.0)
            if(self.__receiveThread.is_alive()):
                return False
        return True
    
    def createFileObject(self, fileName:str, fileLocation:str, fileSizeInBytes: int, send:bool, id:str="") -> str:
        id:str = str(uuid.uuid4()) if send else id
        if(send):
            while(id in self.fileMap.keys()):
                id = str(uuid.uuid4())
        else:
            if(id in self.fileMap.keys()):
                error = msgpack.packb(Command(CommandType.ERROR_FILE, {"uuid":id, "error_code":"id_already_exists"}).to_dict())
                self.sendingQueue.put({"type":DataType.COMMAND, "data": error})
                return None
        f:file = file(fileName,fileLocation, fileSizeInBytes, self.sendingQueue, send, id, FileStatus.PROCESSING if not send and self.autoAccept else None )
        self.fileMap[id]=f
        return id
        
    def sendFile(self, fileDirectory):
        fileDirectory = fileDirectory
        fileName = fileDirectory.split("/")[-1]
        fileSize =  os.path.getsize(fileDirectory)
        id = self.createFileObject(fileName, fileDirectory, fileSize, True, "")
        request = msgpack.packb(Command(CommandType.FILE_REQUEST, FileRequest(fileName, fileSize, id).to_dict()))
        self.sendingQueue.put({"type":DataType.COMMAND, "data":request})
        
    def receiveFile(self, fileName:str, fileSize:int, id:str):
        id = self.createFileObject(fileName, "", fileSize, False, id)
        if(id == None):
            return
        if(self.autoAccept):
            self.acceptReceive(id, self.downloadLocation)
    
    def acceptReceive(self, id:str, downloadLocation: str):
        # //send accept receive command        
        self.fileMap[id].prepareReceive(downloadLocation)
        
    def rejectReceive(self, id:str):
        # //send reject receive command
        self.fileMap[id].rejectReceive()
            
    def enableAutoAccept(self, downloadLocation=None):
        if(downloadLocation):
            self.downloadLocation = downloadLocation
        self.autoAccept=True
        
    def stopFilemanager(self):
        self.__stopReceiveThread()
        for x,y in self.fileMap.items():
            y.closeFile()