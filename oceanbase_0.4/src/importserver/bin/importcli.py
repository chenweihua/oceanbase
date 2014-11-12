#!/usr/bin/python

import optparse
import logging
import struct
import pickle
import socket
import sys
import re
import platform

def ParseArgs():
  usage = ('usage: %prog [options] -t OVERWRITE[input_path1 table_name1 input_path2 table_name2 ...]\n'
           '       %prog [options] -t OVERWRITE_GENERATE[input_path1 table_name1 input_path2 table_name2 ...]\n'
           '       %prog [options] -t OVERWRITE task_id\n'
           '       %prog [options] -t MAJOR_FREEZE\n'
           '       %prog [options] -t STATE task_id (or ALL, DOING, KILLED, DONE, ERROR)\n'
           '       %prog [options] -t KILL task_id\n'
           '       %prog [options] -t CREATE_BYPASS_CONFIG table_name [raw_data_field_count=1] [delim=1] [null_flag=2] [column_infos=1001-0,1002-1...]\n'
           '       %prog [options] -t DELETE_BYPASS_CONFIG table_name\n'
           '       %prog [options] -t PRINT_BYPASS_CONFIG table_name\n')
  parser = optparse.OptionParser(usage)
  parser.add_option('-s', '--server',
      help='Import server address, default is localhost',
      dest='server', default='localhost')
  parser.add_option('-p', '--port',
      help='Import server port, default is 2900',
      dest='port', default='2900')
  parser.add_option('-t', '--cmd_type',
      help='Type of operation: OVERWRITE, OVERWRITE_GENERATE, STATE , KILL or MAJOR_FREEZE',
      dest='cmd_type', default='STATE')
  (options, args) = parser.parse_args()
  cmd_type = options.cmd_type
  inputs = []
  if cmd_type == 'CREATE_BYPASS_CONFIG':
    if len(args) < 1 or len(args) >4:
      parser.print_help()
      sys.exit(1)
    else:
      for i in range(0,len(args)):
        inputs.append(args[i])
  elif cmd_type in ('DELETE_BYPASS_CONFIG', 'PRINT_BYPASS_CONFIG'):
    if len(args)!=1:
      parser.print_help()
    else:
      inputs.append(args[0])
  elif cmd_type == 'MAJOR_FREEZE':
    pass
  elif cmd_type in ('KILL', 'STATE'):
    if len(args) == 1:
      task_id = args[0]
      logging.debug('task_id      => ' + str(task_id))
      globals()['task_id'] = task_id
    else:
      parser.print_help()
      sys.exit(1)
  elif cmd_type == 'OVERWRITE' and len(args) == 1:
    task_id = args[0]
    logging.debug('task_id      => ' + str(task_id))
    globals()['task_id'] = task_id
  elif len(args) % 2 != 0 or len(args) == 0:
    parser.print_help()
    sys.exit(1)
  elif cmd_type in ('OVERWRITE', 'OVERWRITE_GENERATE'):
    for i in range(0, len(args), 2):
      inputs.append((args[i], args[i+1]))
  else:
    parser.print_help()
    sys.exit(1)

  logging.debug('server      => ' + str(options.server))
  logging.debug('port        => ' + str(options.port))
  logging.debug('inputs      => ' + str(inputs))
  logging.debug('cmd_type => ' + str(cmd_type))
  globals()['server']      = options.server
  globals()['port']        = options.port
  globals()['inputs']      = inputs
  globals()['cmd_type'] = cmd_type

class SocketManager:
  def __init__(self, address):
    self.address = address
  def __enter__(self):
    self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.sock.connect(self.address)
    return self.sock
  def __exit__(self, *ignore):
    self.sock.close()

def SendPacket(sock, pkg_type, data):
  '''
  packet format
      ---------------------------------------------------
      |    Packet Type (8B)    |    Data Length (8B)    |
      ---------------------------------------------------
      |                      Data                       |
      ---------------------------------------------------
  '''
  buf = pickle.dumps(data)
  packetheader = struct.pack('!ll', pkg_type, len(buf))
  sock.sendall(packetheader)
  sock.sendall(buf)

def Receive(sock, length):
  result = ''
  while len(result) < length:
    data = sock.recv(length - len(result))
    if not data:
      break
    result += data
  if len(result) == 0:
    return None
  if len(result) < length:
    raise Exception('Do not receive enough data: '
        'length={0} result_len={1}'.format(length, len(result)))
  return result

def ReceivePacket(sock):
  '''
  receive a packet from socket
  return type is a tuple (PacketType, Data)
  '''
  packetformat = '!ll'
  packetheaderlen = struct.calcsize(packetformat)
  packetheader = Receive(sock, packetheaderlen)
  if packetheader is not None:
    (packettype, datalen) = struct.unpack(packetformat, packetheader)
    try:
      data = pickle.loads(Receive(sock, datalen))
      return (packettype, data)
    except Exception, err:
      print("failed to receive packet: " + str(err));
      return (packettype, "failed to receive packet")
  return (None, None)

def SendRequest():
  try:
    with SocketManager((server, int(port))) as sock:
      if cmd_type.upper() == 'OVERWRITE':
        if 'task_id' in globals():
          SendPacket(sock, OVERWRITE_CONTINUE_TASK_PKG, task_id)
        else:
          SendPacket(sock, SUBMIT_OVERWRITE_TASK_PKG, inputs)
      elif cmd_type.upper() == 'OVERWRITE_GENERATE':
        SendPacket(sock, OVERWRITE_GENERATE_TASK_PKG, inputs)
      elif cmd_type.upper() == 'KILL':
        SendPacket(sock, SUBMIT_KILL_TASK_PKG, task_id)
      elif cmd_type.upper() == 'STATE':
        SendPacket(sock, SUBMIT_STATE_TASK_PKG, task_id)
      elif cmd_type.upper() == 'MAJOR_FREEZE':
        SendPacket(sock, SUBMIT_MAJOR_FREEZE_PKG, inputs)
      elif cmd_type.upper() == 'CREATE_BYPASS_CONFIG':
        SendPacket(sock, CREATE_BYPASS_CONFIG, inputs)
      elif cmd_type.upper() == 'DELETE_BYPASS_CONFIG':
        SendPacket(sock, DELETE_BYPASS_CONFIG, inputs)
      elif cmd_type.upper() == 'PRINT_BYPASS_CONFIG':
        SendPacket(sock, PRINT_BYPASS_CONFIG, inputs)
      else:
        raise Exception('Wrong import type: {0}'.format(cmd_type))
      while True:
        (packettype, data) = ReceivePacket(sock)
        if packettype is None:
          if data is None:
            print "ERROR!!! packettype and data is None"
            sys.exit(1)
          print(data)
        elif packettype == END_PKG:
          print(data)
          if data == 'SUCCESSFUL\n':
            sys.exit(0)
          else:
            sys.exit(1)
          break
        if len(data) == 0:
          print('The server returned nothing before closing the connection...')
          sys.exit(1)
        else:
          sys.stdout.write(data)
  except socket.error as err:
    print('Network error: {0}'.format(err))
    sys.exit(1)

def CheckPythonVersion():
  version = platform.python_version()
  m = re.match(r'^\d+.\d+', version)
  version = m.group(0)
  if version < 2.6:
    print "python version should not less than 2.6"
    sys.exit(1)

SUBMIT_OVERWRITE_TASK_PKG     = 102
SUBMIT_STATE_TASK_PKG         = 106
SUBMIT_KILL_TASK_PKG          = 108
OVERWRITE_GENERATE_TASK_PKG   = 113
OVERWRITE_CONTINUE_TASK_PKG   = 114

SUBMIT_MAJOR_FREEZE_PKG       = 120
CREATE_BYPASS_CONFIG          = 121
DELETE_BYPASS_CONFIG          = 122
PRINT_BYPASS_CONFIG           = 123

RESPONSE_PKG                  = 199
ERROR_PKG                     = 200
END_PKG                       = 201

if __name__ == '__main__':
  CheckPythonVersion()
  ParseArgs()
  SendRequest()

