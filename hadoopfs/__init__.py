#! /usr/bin/env python
#
# HDFS client library for Thrift api.
#
# @author Sreejith K <sreejithemk@gmail.com>
# Created on 18th Apr 2011

import os
from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from gen_py.ThriftHadoopFileSystem import Client
from gen_py.ttypes import Pathname


class HDFSError(Exception):
  """Base Exception class for HDFS errors
  """


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

  def close_connection(self):
    if self.do_close:
      self.transport.close()


class Hfile(object):
  """A File-like interface for HDFS files.
  """
  def __init__(self, hostname, port, filename, mode='r', buffer_size=0,
          replication=1, block_size=0):
    self._pathname = Pathname(filename)
    if mode == 'r':
      flags = os.O_RDONLY
    elif mode == 'w':
      flags = os.O_WRONLY
    else:
      raise HDFSError('%s mode not supported' %mode)
    self._fs = HDFSClient(hostname, port)
    self._buffer_size = buffer_size
    self._replication = replication
    self._block_size = block_size
    self._fh = self._fs.createFile(self._pathname, flags, True, 
                  self._buffer_size, self._replication, 
                  self._block_size)

  def read(self, size=None):
    stat = self._fs.stat(self._pathname)
    if not size:
      size = stat.length
    return self._fs.read(self._fh, 0, size)
    
  def write(self, data):
    return self._fs.write(self._fh, data)
    
  def close(self):
    self._fs.close(self._fh)
    del self._fh
    self._fh = None
    self._fs.close_connection()


def test():
  client = HDFSClient('127.0.0.1', 10101)
  path = Pathname('/test')
  print client.exists(path)
  client.close_connection()
  
  hfile = Hfile('127.0.0.1', 10101, '/test', 'w')
  hfile.write('test\n')
  hfile.close()

  hfile = Hfile('127.0.0.1', 10101, '/test', 'r')
  data = hfile.read()
  hfile.close()
  print data
  assert data == 'test\n'

if __name__ == '__main__':
  test()
