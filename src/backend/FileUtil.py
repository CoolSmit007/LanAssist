from models.enums.DataType import DataType
import msgpack

def createSendingPackage(data,uuid):
    toSendData = msgpack.packb({"uuid":uuid,"data":data})
    return {"type":DataType.FILE, "data":toSendData}

def decodeReceivingPackage(data):
    unpackedData = msgpack.unpackb(data)
    return unpackedData