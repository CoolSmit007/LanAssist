from models.enums.CommandType import CommandType

class Command():
    def __init__(self, type:CommandType, data):
        self.type: CommandType = type
        self.data: dict = data
        
    def to_dict(self):
        return {
            "type":self.type.value,
            "data":self.data
        }
        
    @staticmethod
    def from_dict(dictionary: dict):
        return Command(dictionary.get("type"),dictionary.get("data"))