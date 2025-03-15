class FileRequest():
    def __init__(self,fileName, fileSize, uuid):
        self.uuid = uuid
        self.fileName = fileName
        self.fileSize = fileSize
    
    def to_dict(self):
        return {
            "uuid":self.uuid,
            "file_name":self.fileName,
            "file_size":self.fileSize
        }
        
    @staticmethod
    def from_dict(dictionary: dict):
        return FileRequest(dictionary.get("file_name"),dictionary.get("file_size"),dictionary.get("uuid"))