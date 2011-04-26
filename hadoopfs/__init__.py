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
      self.open_connection(timeout_ms)

  def open_connection(self, timeout_ms):
    self.transport.open()
    self.do_close = 1

  def close_connection(self):
    if self.do_close:
      self.transport.close()


class Hfile(object):
  """A File-like interface for HDFS files.
  """
  def __init__(self, hostname, port, filename, mode='r')
    self._pathname = Pathname(filename)
    self._fs = HDFSClient(hostname, port)
    if mode == 'r':
      self._fh = self._fs.open(self._pathname)
    elif mode == 'w':
      self._fh = self._fs.create(self._pathname)
    else:
      raise HDFSError('Invalid mode: %s' %mode)
    self._seek_pos = 0

  def seek(self, offset):
    self._seek_pos = offset

  def read(self, size=None):
    stat = self._fs.stat(self._pathname)
    if not size:
      size = stat.length
    return self._fs.read(self._fh, self._seek_pos, size)
    
  def write(self, data):
    return self._fs.write(self._fh, data)
    
  def close(self):
    self._fs.close(self._fh)
    del self._fh
    self._fs.close_connection()
    del self._fs


def test():
  import sys
  if len(sys.argv) < 2:
    print '%s <port>' %sys.argv[0]
    sys.exit(1)

  port = int(sys.argv[1])

  client = HDFSClient('127.0.0.1', port)
  path = Pathname('/test')
  print client.exists(path)
  client.close_connection()
  
  hfile = Hfile('127.0.0.1', port, '/test', 'w')
  hfile.write('test\n')
  hfile.close()

  hfile = Hfile('127.0.0.1', port, '/test', 'r')
  data = hfile.read()
  hfile.close()
  print data
  assert data == 'test\n'

if __name__ == '__main__':
  test()
