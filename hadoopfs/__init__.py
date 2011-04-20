#! /usr/bin/env python
#
# HDFS client library for Thrift api.
#
# @author Sreejith K <sreejithemk@gmail.com>
# Created on 18th Apr 2011

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from gen_py.ThriftHadoopFileSystem import Client

class HDFSClient(Client):
  def __init__(self, host, port, timeout_ms = 300000, do_open = 1):
    socket = TSocket.TSocket(host, port)
    socket.setTimeout(timeout_ms)
    self.transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(self.transport)
    Client.__init__(self, protocol)

    if do_open:
      self.open(timeout_ms)

  def open(self, timeout_ms):
    self.transport.open()
    self.do_close = 1

  def close(self):
    if self.do_close:
      self.transport.close()


def test():
  from gen_py.ttypes import Pathname
  client = HDFSClient('127.0.0.1', 10101)
  path = Pathname('/test')
  print client.exists(path)
  client.close()

if __name__ == '__main__':
  test()
