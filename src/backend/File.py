import queue
import threading as th
import msgpack

from log_init import LOGGER
from config import CONFIGS

from models.enums.DataType import DataType
from models.enums.CommandType import CommandType
from models.enums.FileStatus import FileStatus
from models.Command import Command

from util.FileUtil import createSendingPackage

class file:
    __sendLock = th.Event()
    __ackLock = th.Event()
    __sendThread = None
    error = None
    status:FileStatus = None
    
    # __receiveLock = th.Event()
    # __receiveThread = None
    def __init__(self, fileName: str, fileLocation: str, fileSizeInBytes: int, sendingQueue: queue.Queue, send: bool, uniqueId: str, status:FileStatus=None):
        self.receivingQueue = queue.Queue(1)
        self.fileName: str = fileLocation.split("\\")[-1] if send==True else fileName
        self.fileLocation: str = fileLocation
        self.fileSizeInBytes: int = fileSizeInBytes
        self.sendingQueue: queue.Queue = sendingQueue
        self.progress: float = 0.0
        self.bytesProcessed: int = 0
        self.file = None
        self.__sendLock.set()
        # self.__receiveLock.set()
        self.uuid: str = uniqueId
        self.status = status if status else FileStatus.WAITING_FOR_ACCEPTANCE if send else FileStatus.WAITING_FOR_USER_INPUT
        
    def unblockSend(self):
        self.__ackLock.set()
        
    def __send(self):
        try:
            while self.__sendLock.is_set():
                data=self.file.read(CONFIGS.fileConfigs.chunkSize)
                if not data:
                    self.status = FileStatus.PROCESSED
                    break
                self.sendingQueue.put(createSendingPackage(data,self.uuid))
                self.__ackLock.clear()
                while self.__sendLock.is_set():
                    self.__ackLock.wait(timeout = 1.0)
                if not self.__ackLock.is_set():
                    self.error = "Sending was interrupted"
                    break    
                self.bytesProcessed+=len(data)
                self.progress = float(self.bytesProcessed)/float(self.fileSizeInBytes)
        finally:
            self.file.close()
    
    # def __receive(self,data):
    #     try:
    #         while self.__receiveLock.is_set():
    #             try:
    #                 data = self.receivingQueue.get(timeout=1.0)
    #             except queue.Empty:
    #                 continue
    #             self.file.write(data)
    #             self.bytesProcessed+=len(data)
    #             self.progress = float(self.bytesProcessed)/float(self.fileSizeInBytes)
    #             ack = msgpack.packb(Command(CommandType.ACK_FILE, {"uuid":self.uuid}).to_dict())
    #             self.connection.sendingQueue.put({"type":DataType.COMMAND, "data": ack})
    #     finally:
    #         self.file.close()
            
    def receive(self,data):
        self.file.write(data)
        self.bytesProcessed+=len(data)
        self.progress = float(self.bytesProcessed)/float(self.fileSizeInBytes)
        ack = msgpack.packb(Command(CommandType.ACK_FILE, {"uuid":self.uuid}).to_dict())
        self.sendingQueue.put({"type":DataType.COMMAND, "data": ack})
        if(self.bytesProcessed == self.fileSizeInBytes):
            self.status = FileStatus.PROCESSED
            
    def startSendThread(self):
        self.file = open(self.fileLocation,"rb")
        self.status = FileStatus.PROCESSING
        self.__sendLock.set()
        self.__sendThread=th.Thread(target=self.__send)
        self.__sendThread.start()
        
    def __stopSendThread(self):
        self.__sendLock.clear()
        if(self.__sendThread):
            self.__sendThread.join(timeout=10.0)
            if(self.__sendThread.is_alive()):
                return False
        return True
    
    def prepareReceive(self, fileLocation):
        self.fileLocation = fileLocation
        self.file = open(self.fileLocation, "wb")
        self.status = FileStatus.PROCESSING
        
    def rejectReceive(self):
        self.status = FileStatus.REJECTED
    # def startReceiveThread(self):
    #     self.__receiveLock.set()
    #     self.__receiveThread=th.Thread(target=self.__receive)
    #     self.__receiveThread.start()
        
    # def __stopReceiveThread(self):
    #     self.__receiveLock.clear()
    #     if(self.__receiveThread):
    #         self.__receiveThread.join(timeout=10.0)
    #         if(self.__receiveThread.is_alive()):
    #             return False
    #     return True
    
    def closeFile(self):
        # if(self.__stopSendThread() and self.__stopReceiveThread()):
        if(self.__stopSendThread()):
            if(self.file):
                self.file.close()
            return True
        return False
    
    def __del__(self):
        if(self.file):
            self.file.close()