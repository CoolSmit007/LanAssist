import queue
import threading as th

from backend.Connection import connection
from models.Command import Command
from models.enums.CommandType import CommandType
from backend.File import file

from backend.FileUtil import decodeReceivingPackage
from log_init import LOGGER
class fileManager:
    __receiveLock = th.Event()
    __receiveThread = None
    def __init__(self):
        self.queue = queue.Queue()
        self.fileMap: dict[str,file] = {}
        self.__receiveLock.set()
        
    def executeCommand(self,command:Command):
        match command.type:
            case CommandType.ACK_FILE.value:
                if not command.data.get("uuid"):
                    LOGGER.error("No uuid found in command with type: %s",CommandType.ACK_FILE.name)
                    return
                uuid = command.data.get("uuid")
                if not self.fileMap.get(uuid):
                    LOGGER.error("No file object found with uuid: %s",uuid)
                    return
                self.fileMap.get(uuid).unblockSend()     
        
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
                
    
    def startReceiveThread(self):
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