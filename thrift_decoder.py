import sys
import json
from thrift.Thrift import TType
from thrift.protocol import TBinaryProtocol, TCompactProtocol, TProtocol
from thrift.transport import TTransport

typeNameMap = {
  0: "stop",
  1: "void",
  2: "bool",
  3: "byte",
  4: "double",
  6: "i16",
  8: "i32",
  10: "i64",
  11: "string",
  12: "struct",
  13: "map",
  14: "set",
  15: "list",
  16: "enum",
}


class ThriftDecoder(object):
  def __init__(self, protocolFactory) -> None:
    self.protocolFactory = protocolFactory
  
  def decode(self, dataBytes):
    transport = TTransport.TMemoryBuffer(dataBytes)
    self.protocol: TProtocol.TProtocolBase = self.protocolFactory.getProtocol(transport)
    self.protocol.readStructBegin()
    return self.readFields()
  
  def readFields(self):
    fields = []
    while True:
      (_, type, fid) = self.protocol.readFieldBegin()
      if type == TType.STOP:
        break
      fields.append({"id": fid, "type": typeNameMap[type], "value": self.readFieldValue(type)})
      self.protocol.readFieldEnd()
    return fields

  def readFieldValue(self, type):
    if type == TType.BOOL:
      return self.protocol.readBool()
    elif type == TType.BYTE:
      return self.protocol.readByte()
    elif type == TType.DOUBLE:
      return self.protocol.readDouble()
    elif type == TType.I16:
      return self.protocol.readI16()
    elif type == TType.I32:
      return self.protocol.readI32()
    elif type == TType.I64:
      return self.protocol.readI64()
    elif type == TType.STRING:
      binary = self.protocol.readBinary()
      try:
        return binary.decode("utf-8")
      except UnicodeDecodeError as e:
        return f"UnicodeDecodeError. binary({bytes.hex(binary)})" 
    elif type == TType.STRUCT:
      return self.readStruct() 
    elif type == TType.MAP:
      return self.readMap()
    elif type == TType.SET:
      return self.readSet()
    elif type == TType.LIST:
      return self.readList()
    else:
      raise Exception(f'unknown type: {type}')

  def readStruct(self):
    self.protocol.readStructBegin()
    fields = self.readFields()
    self.protocol.readStructEnd()
    return fields

  def readMap(self):
    kvMap = {}
    (ktype, vtype, size) = self.protocol.readMapBegin()
    for _ in range(0, size):
      key = self.readFieldValue(ktype)
      value = self.readFieldValue(vtype)
      kvMap[key] = value
    self.protocol.readMapEnd()
    return {"keyType:": typeNameMap[ktype], "valueType": typeNameMap[vtype], "map": kvMap}

  def readSet(self):
    vSet = set()
    type, size = self.protocol.readSetBegin()
    for _ in range(0, size):
      vSet.add(self.readFieldValue(type))
    self.protocol.readSetEnd()
    return {"type": typeNameMap[type], "set": vSet}

  def readList(self):
    vList = list()
    type, size = self.protocol.readListBegin()
    for _ in range(0, size):
      vList.append(self.readFieldValue(type))
    self.protocol.readListEnd()
    return {"type": typeNameMap[type], "list": vList}


def main():
  if len(sys.argv) != 2:
    print("usage: python thrift_decoder.py hexDataString")
    return

  input = bytes.fromhex(sys.argv[1])
  decoder = ThriftDecoder(protocolFactory=TCompactProtocol.TCompactProtocolFactory())
  output = decoder.decode(input)
  print(json.dumps(output, indent=2))


if __name__ == "__main__":
  main()
