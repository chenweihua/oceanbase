#!/usr/bin/python

import time
import SocketServer
import optparse
import threading
import pickle
import struct
import copy
import ConfigParser
import os
import sys
import datetime
import re
import subprocess
from subprocess import *
import Queue
import select
import platform
import random
import shutil
from collections import deque
from xml.etree.ElementTree import ElementTree

log_send_lock = threading.RLock()
update_conf_lock= threading.RLock()

def JoinPath(*path_list):
  first = True
  full_path = ""
  for path in path_list:
    if first == True:
      full_path = path
      first = False
    else:
      if full_path.endswith('/'):
        full_path += path.lstrip('/')
      else:
        full_path += '/' + path.lstrip('/')
  return full_path

class TaskTrace:
  def __init__(self):
    self.trace = dict()  # (table_name=XXX, True/False)
    self.table_in_op = dict() # (task_id=XXX, table_name_list=XXX, status)
    self.check_table_lock = threading.RLock()
    self.doing_replicate_task = False

  def _set_table_in_op(self, table_name_list):
    msg = ''
    self.check_table_lock.acquire()
    try:
      ret = 0
      for table_name in table_name_list:
        if table_name in self.table_in_op and self.table_in_op[table_name] == True:
          ret = 1
          msg = '[task trace] table "{0}" is already in op, cannot be set twice'.format(table_name)
          log.error(msg)
          break
      if ret == 0:
        for table_name in table_name_list:
          self.table_in_op[table_name] = True
          msg = '[task trace] set "{0}" in op'.format(table_name)
          log.info(msg)
    except Exception as err:
      msg = err
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret, msg

  def _reset_table_in_op(self, table_name_list):
    self.check_table_lock.acquire()
    try:
      ret = 0
      for table_name in table_name_list:
        if table_name not in self.table_in_op or self.table_in_op[table_name] == False:
          ret = 1
          msg = '[task trace] table "{0}" is not in op, should not be reset'.format(table_name)
          log.error(msg)
        else:
          self.table_in_op[table_name] = False
          msg = '[task trace] set table "{0}" not in op'.format(table_name)
          log.info(msg)
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret

  def add_task(self, task_id, task_type, table_name_list, inputs):
    msg = ''
    ret = 0
    self.check_table_lock.acquire()
    try:
      # check replicate task
      if task_type == 'REPLICATE':
        if len(self.trace) > 0:
          msg = "other import op is started, no replicate op is allowd. check -t STATE ALL for more info."
          ret = 1
        else:
          self.doing_replicate_task = True
      elif self.doing_replicate_task == True:
        msg = "no other import op is allowed during replicating cluster"
        ret = 1

      if ret == 0:
        #clean old task state with DONE or KILLED
        to_remove_count = len(self.trace) - keep_task_state_count + 1
        if to_remove_count > 0:
          log.info('[task trace] recent trace count={0}, need remove {1} DONE, KILLED or ERROR trace'.format(
            keep_task_state_count, to_remove_count))
          for k in sorted(self.trace.keys()):
            task = self.trace[k]
            if task['type'] in ('IMPORT_GENERATE', 'OVERWRITE_GENERATE'):
              continue
            elif task['status'] in ('DONE', 'KILLED', 'ERROR'):
              del self.trace[k]
              log.info('[task trace] del trace: task id={0} type={1}, table_list={2}, state={3} inputs={4}'.format(
                task_id, task['type'], task['table_name_list'], task['status'], task['inputs']))
              to_remove_count -= 1
              if to_remove_count <= 0:
                break
        # do add task
        if task_id in self.trace:
          ret = 1
          task = self.trace[task_id]
          msg = "[task trace] task with task id={0} is already exist, type={1}, table_list={2}, state={3} inputs={4}".format(
                task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
        else:
          ret, msg = self._set_table_in_op(table_name_list)
          if ret == 0:
            task = dict()
            task['table_name_list'] = table_name_list
            task['status'] = 'DOING'
            task['type'] = task_type
            task['inputs'] = inputs
            self.trace[task_id] = task
            msg = "[task trace] update task trace: task_id={0}, type={1} table_list={2}, state={3} inputs={4}".format(
                task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
          else:
            log.error(msg)
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret, msg

  def end_task(self, task_id, result):
    self.check_table_lock.acquire()
    try:
      ret = 0
      msg = ""
      if task_id not in self.trace:
        ret = 1
        msg = "[task trace] task id {0} not exist, should not end..".format(task_id)
      else:
        task = self.trace[task_id]
        table_name_list = task['table_name_list']
        ret = self._reset_table_in_op(table_name_list)
        if task['status'] == 'NEED_KILL':
          task['status'] = 'KILLED'
          msg = "[task trace] task with task_id={0} is killed: type={1}, table_list={2}, state={3} inputs={4}".format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
        elif result != 0:
          task['status'] = 'ERROR'
          msg = "[task trace] failed to do the task: task_id={0}, type={1}, table_list={2}, state={3} inputs={4}".format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
        elif ret == 0 and task['status'] == 'DOING':
          task['status'] = 'DONE'
          msg = "[task trace] update task trace: task_id={0}, type={1}, table_list={2}, state={3} inputs={4}".format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
        else:
          msg = "[task trace] error of task trace: task_id={0}, type={1}, table_list={2}, last state={3} inputs={4}".format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
          task['status'] += 'ERROR'
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret, msg

  def kill_task(self, task_id):
    self.check_table_lock.acquire()
    try:
      task = None
      msg = ""
      if task_id not in self.trace:
        msg = "[task trace] task id {0} is not exist, cannot be killed!"
      else:
        task = self.trace[task_id]
        if task['status'] != 'DOING':
          msg = "[task trace] task '{0}' is {1}, should not be killed".format(task_id, task['status'])
          task = None
        else:
          task['status'] = 'NEED_KILL'
          msg = "[task trace] start to kill task with task_id={0}: table_list={1}, state={2} inputs={3}".format(
                task_id, task['table_name_list'], task['status'], task['inputs'])
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return task, msg

  def is_need_kill(self, task_id):
    self.check_table_lock.acquire()
    try:
      ret = 0
      msg = ""
      if task_id not in self.trace:
        ret = 1
        msg = "[task trace] task id {0} is not exist, should not check is_need_kill."
      else:
        task = self.trace[task_id]
        if task['status'] == 'NEED_KILL':
          ret = 2
          msg = "[task trace] task {0}:{1} status is NEED_KILL".format(task_id, task['table_name_list'])
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret, msg

  def get_task_stat(self, task_id):
    ret = 0
    msg = ''
    self.check_table_lock.acquire()
    try:
      task = None
      if task_id == 'ALL':
        msg = '[task trace] task count={0} keep_task_state_count={1}'.format(len(self.trace), keep_task_state_count)
        for task_id in sorted(self.trace.keys()):
          task = self.trace[task_id]
          msg += '\ntask id={0}: type={1}, table_list={2}, state={3}, inputs={4}'.format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
      elif task_id in ('DOING', 'DONE', 'NEED_KILL', 'KILLED', 'ERROR'):
        status = task_id
        count = 0
        for task_id in self.trace:
          task = self.trace[task_id]
          if task['status'] == status:
            msg += '\ntask id={0}: type={1}, table_list={2}, state={3}, inputs={4}'.format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
            count += 1
        msg = '[task trace] task count={0}{1}'.format(count, msg)
      elif task_id not in self.trace:
        ret = 1
        msg = "[task trace] task id {0} is not exist, no stat can be got.".format(task_id)
      else:
        task = self.trace[task_id]
        msg = "[task trace] task id={0}: type={1}, table_list={2}, state={3}, inputs={4}".format(
              task_id, task['type'], task['table_name_list'], task['status'], task['inputs'])
    except Exception as err:
      log.exception(err)
    finally:
      self.check_table_lock.release()
    return ret, msg

  def set_task_inputs(self, task_id, inputs, table_dict):
    ret = 0
    msg = ''
    self.check_table_lock.acquire()
    try:
      if task_id not in self.trace:
        ret = 1
        msg = "[task trace] task id {0} is not exist, cannot set inputs.".format(task_id)
      else:
        self.trace[task_id]['inputs'] = inputs
        self.trace[task_id]['table_dict'] = table_dict
    except Exception as err:
      log.exception(err)
      ret = 1
    finally:
      self.check_table_lock.release()
    return ret, msg

  def get_task_inputs(self, task_id):
    ret = None
    msg = ''
    if task_id not in self.trace:
      ret = 1
      msg = "[task trace] task id {0} is not exist, cannot get inputs.".format(task_id)
    else:
      ret = self.trace[task_id]['inputs']
    return ret, msg

  def continue_task(self, task_id, packettype):
    ret = 0
    task = None
    self.check_table_lock.acquire()
    try:
      msg = ''
      if task_id not in self.trace:
        ret = 1
        msg = "[task trace] task id {0} is not exist, cannot be continued.".format(task_id)
      else:
        task = self.trace[task_id]
        if task['status'] == 'DONE' and task['type'] not in ('IMPORT_GENERATE', 'OVERWRITE_GENERATE'):
          ret = 1
          msg = '{0} task[{1}] is done, should not be continued'.format(task['type'], task_id)
          log.error(msg)
        elif task['status'] != 'DONE' and task['type'] in ('IMPORT_GENERATE', 'OVERWRITE_GENERATE'):
          ret = 1
          msg = '{0} task[{1}] is {2}, cannot be continued'.format(task['type'], task_id, task['status'])
          log.error(msg)
        elif packettype == IMPORT_CONTINUE_TASK_PKG and task['type'] in ('IMPORT_GENERATE', 'IMPORT'):
          pass
        elif packettype == OVERWRITE_CONTINUE_TASK_PKG and task['type'] in ('OVERWRITE_GENERATE', 'OVERWRITE'):
          pass
        else:
          ret = 1
          msg = 'cannot continue task[{0}]: packettype={1} but task type={2} status={3}'.format(
              task_id, packettype, task['type'], task['status'])
          log.error(msg)

      if ret == 0:
        table_list = task['table_name_list']
        ret, msg = self._set_table_in_op(table_list)
        if ret != 0:
          log.error(msg)
        else:
          task['status'] = 'DOING'
          if task['type'] == 'IMPORT_GENERATE':
            task['type'] = 'IMPORT'
          elif task['type'] == 'OVERWRITE_GENERATE':
            task['type'] = 'OVERWRITE'
    except Exception as err:
      log.exception(err)
      ret = 1
    finally:
      self.check_table_lock.release()
    return ret, task, msg

def R(cmd, local_vars):
  G = copy.copy(globals())
  G.update(local_vars)
  return cmd.format(**G)

def CheckPid(pid_file):
  try:
    with open(pid_file, 'r') as fd:
      pid = int(fd.read().strip())
  except IOError:
    pid = None
  if pid:
    try:
      os.kill(pid, 0)
    except OSError:
      pid = None
  if pid:
    message = "program has been exist: pid={0}\n".format(pid)
    sys.stderr.write(message)
    sys.exit(1)

def WritePid():
  try:
    pid = str(os.getpid())
    with open(pid_file, 'w') as fd:
      fd.write("{0}\n".format(pid))
  except Exception as err:
    log.exception('ERROR: {0}'.format(err))

def Daemonize():
  try:
    pid = os.fork()
    if pid > 0:
      # exit first parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #1 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # decouple from parent environment
  #os.chdir("/")
  os.setsid()
  os.umask(0)

  # do second fork
  try:
    pid = os.fork()
    if pid > 0:
      # exit from second parent
      sys.exit(0)
  except OSError, e:
    sys.stderr.write("fork #2 failed: %d (%s)\n" % (e.errno, e.strerror))
    sys.exit(1)

  # redirect standard file descriptors
  sys.stdout.flush()
  sys.stderr.flush()
  si = file('/dev/zero', 'r')
  so = file('/dev/null', 'a+')
  se = file('/dev/null', 'a+', 0)
  os.dup2(si.fileno(), sys.stdin.fileno())
  os.dup2(so.fileno(), sys.stdout.fileno())
  os.dup2(se.fileno(), sys.stderr.fileno())

  WritePid()

class ExecutionError(Exception): pass

class Shell:
  @classmethod
  def popen(cls, cmd, host=None, username=None):
    '''Execute a command locally, and return
    >>> Shell.popen('ls > /dev/null')
    ''
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh  -oStrictHostKeyChecking=no {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh  -oStrictHostKeyChecking=no {host} '{cmd}'".format(**locals())
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    output = p.communicate()[0]
    err = p.wait()
    if err:
        output = 'Shell.popen({0})=>{1} Output=>"{2}"'.format(cmd, err, output)
        raise ExecutionError(output)
    return output

  @classmethod
  def call(cls, cmd):
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    output = p.communicate()
    err = p.wait()
    return err, output

  @classmethod
  def sh(cls, cmd, host=None, username=None):
    '''Execute a command locally or remotely
    >>> Shell.sh('ls > /dev/null')
    0
    >>> Shell.sh('ls > /dev/null', host='10.232.36.29')
    0
    '''
    if host is not None:
      if username is not None:
        cmd = "ssh  -oStrictHostKeyChecking=no {username}@{host} '{cmd}'".format(**locals())
      else:
        cmd = "ssh  -oStrictHostKeyChecking=no {host} '{cmd}'".format(**locals())
    ret = os.system(cmd)
    if ret != 0:
      err_msg = 'Shell.sh({0}, host={1})=>{2}\n'.format(
          cmd, host, ret);
      sys.stderr.write(err_msg)
    return ret

  @classmethod
  def sh_and_print(cls, cmd,conn=None):
    ret = 0
    try:
      log.debug('cmd command: {0}'.format(cmd))
      p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
      while p.poll() == None:
        (rlist, wlist, xlist) = select.select([p.stderr, p.stdout], [], [])
        for rpipe in rlist:
          l = rpipe.readline()
          if l == '':
            continue
          if rpipe == p.stderr:
            log.info('cmd output: {0}'.format(l.rstrip()))
            if conn != None:
              SendPacket(conn, RESPONSE_PKG, l)
          else:
            log.debug('cmd output: {0}'.format(l.rstrip()))
      (stdout, stderr) = p.communicate()
      if stderr != '':
        log.info('cmd output: {0}'.format(stderr))
        if conn != None:
          SendPacket(conn, RESPONSE_PKG, stderr)
      if stdout != '':
        log.debug('cmd output: {0}'.format(stdout))
      err = p.returncode
      if err != 0:
        ret = 1
        output = 'failed to run {0}: {1}'.format(cmd, err)
        log.error(output)
    except Exception as err:
      output = 'ERROR: ' + str(err)
      log.exception(output)
      ret = 1
    return ret

  @classmethod
  def mkdir(cls, path, host=None):
    '''make directory locally or remotely
    >>> Shell.mkdir('test', host='10.232.36.29')
    0
    >>> Shell.mkdir('test')
    0
    '''
    if host is None:
      os.path.exists(path) or os.mkdir(path)
      return 0
    else:
      return Shell.sh('mkdir -p {0}'.format(path), host)

class ShellWorker:
  def __init__(self, conn):
    self.process_list = []
    self.conn = conn
    self.ret = 0

  def add(self, cmd, description, log_path):
    try:
      log_file = open(log_path, 'a')
      cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
      log_file.write('{2}: run cmd={0}, description={1}\n'.format(cmd, description, cur_time))
      p = subprocess.Popen(cmd, shell=True, stdout=log_file, stderr=log_file)
      self.process_list.append((p, description, log_file))
      msg = 'ShellWorker: description={1}, run cmd={0}'.format(cmd, description)
      log.info(msg)
      if self.conn != None:
        SendPacket(self.conn, RESPONSE_PKG, msg + '\n')
    except Exception as e:
      self.ret = 1
      log.exception(e)
    return self.ret

  def wait(self):
    try:
      is_finished = False
      while (is_finished == False):
        cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        time.sleep(10)
        is_finished = True
        count = 0
        for (p, description, log_file) in self.process_list:
          if p.poll() == None:
            is_finished = False
            count += 1
            msg = '{0}: [{1}] {2} is running'.format(cur_time, count, description)
            log.info(msg)
            if self.conn != None:
              SendPacket(self.conn, RESPONSE_PKG, msg + '\n')

      for (p, description, log_file) in self.process_list:
        log_file.close()
        if p.poll() != 0:
          self.ret = 1
          msg = '{0}:{1} failed'.format(count, description)
          log.error(msg)
          if self.conn != None:
            SendPacket(self.conn, RESPONSE_PKG, msg + '\n')
    except Exception as e:
      self.ret = 1
      log.exception(e)

    return self.ret

class WorkerPool:
  class Worker(threading.Thread):
    def __init__(self, task_queue):
      threading.Thread.__init__(self)
      self.task_queue = task_queue
      self.__stop = 0
      self.err = None
      self.ret = 0

    def run(self):
      while not self.__stop:
        try:
          task = self.task_queue.get(timeout=1)
          self.ret += task()
          self.task_queue.task_done()
        except Queue.Empty:
          pass
        except BaseException as e:
          self.ret += 1
          self.task_queue.task_done()
          if self.err is None:
            self.err = e
          log.exception('thread ' + str(self.ident) + ' ' + str(e))
        except Exception as e:
          self.ret += 1
          log.exception('thread ' + str(self.ident) + ' ' + str(e))

      if self.err is not None:
        raise self.err

    def stop(self):
      self.__stop = True

  def __init__(self, num):
    self.task_queue = Queue.Queue()
    self.n = num
    self.workers = [None] * num
    # sum of all ret of every task run in thread pool, 0 means success
    self.ret = 0
    for i in range(num):
      self.workers[i] = WorkerPool.Worker(self.task_queue);
      self.workers[i].start()

  def add_task(self, task):
    self.task_queue.put(task)

  def wait(self):
    self.task_queue.join()
    for w in self.workers:
      self.ret += w.ret
      w.stop()
    return self.ret

def SendPacket(sock, pkg_type, data):
  '''
  packet format
      ---------------------------------------------------
      |    Packet Type (8B)    |    Data Length (8B)    |
      ---------------------------------------------------
      |                      Data                       |
      ---------------------------------------------------
  '''
  log_send_lock.acquire()
  try:
    buf = pickle.dumps(data)
    packetheader = struct.pack('!ll', pkg_type, len(buf))
    sock.sendall(packetheader)
    sock.sendall(buf)
  except Exception as e:
    log.error("failed to send data[{0}], buf[{1}]".format(data, buf))
    log.exception(e)
  log_send_lock.release()

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
    data = pickle.loads(Receive(sock, datalen))
    return (packettype, data)
  else:
    return (None, None)

def GenerateDispatchCmd(connection, inputs, options, timestamp):
  dispatch_cmd = None
  import_list = []
  dispatch_clusters = 0
  for i in inputs:
    input_dir = i[0]
    table_name = i[1]
    #get sstables from data source and dispatch to chunkserver
    if len(i) == 4:
      dispatch_clusters = 1
      table_id = i[2]
      rs_ip_port = i[3]
      ic = R('{conf_dir}/{app_name}/{table_name}/data_syntax.ini,'
        '{input_dir},{table_id},{rs_ip_port}', locals())
    #generate sstables in hadoop for chunkserver import sstable
    elif len(i) == 3:
      table_id = i[2]
      ic = R('{conf_dir}/{app_name}/{table_name}/data_syntax.ini,'
        '{input_dir},{table_id}', locals())
    #generate sstables in hadoop for updateserver import sstable
    else:
      ic = R('{conf_dir}/{app_name}/{table_name}/data_syntax.ini,{input_dir}',
          locals())
    import_list.append(ic)
  dpch_arg = ' '.join(import_list)
  log.debug('dpch_arg is {0}'.format(dpch_arg))
  dispatch_cmd = '{0}/dispatch.sh -c "{1}" -t {2} {3}'.format(bin_dir,
      dpch_arg, timestamp, options)
  log.debug('dispatch_cmd is {0}'.format(dispatch_cmd))
  return dispatch_cmd

class ImportServer(SocketServer.ThreadingMixIn,
    SocketServer.TCPServer):
  pass

class RequestHandler(SocketServer.StreamRequestHandler):
  GlobalLock = threading.Lock()
  task_trace = TaskTrace()

  def ups_copy_sstable(self, task_id, inputs):
    file_num = 0
    ret = 0
    wp = WorkerPool(copy_sstable_concurrency)
    try:
      ups = GetUpsList(dict(ip = rs_ip, port = rs_port))
      ups_master = ups['master']
      l = 'ups master is ' + str(ups_master)
      log.info(l)
      SendPacket(self.connection, RESPONSE_PKG, l + '\n')
      ups_slaves = ups['slaves']
      l = 'ups slave list is ' + str(ups_slaves)
      log.info(l)
      SendPacket(self.connection, RESPONSE_PKG, l + '\n')
      obi_list = []
      for rs in obi_rs:
        ups = GetUpsList(rs)
        if ups['master'] != '':
          obi_list.append(ups)
      for obi in obi_list:
        l = 'remote cluster master is ' + obi['master']
        log.info(l)
        SendPacket(self.connection, RESPONSE_PKG, l + '\n')
        l = 'remote cluster slave list is ' + str(obi['slaves'])
        log.info(l)
        SendPacket(self.connection, RESPONSE_PKG, l + '\n')
      for path in GenUpsBypassDirList(ups_bypass_dir):
        ups_list = copy.copy(ups_slaves)
        ups_list.append(ups_master)
        for obi in obi_list:
          ups_list.append(obi['master'])
          ups_list += obi['slaves']
        log.info('clear ups_list: {0}, path: {1}'.format(ups_list, path))
        ClearBypassDir(path, ups_list)
      globals()['ups_master'] = ups_master
      globals()['ups_slaves'] = ups_slaves
      globals()['obi_list'] = obi_list
      for i in inputs:
        table_name = i[1]
        data_dir = hdfs_data_dir
        if data_dir is None:
          log.error('Can\'t get HADOOP_DATA_DIR')
          ret = 1
        else:
          current_date = task_id
          ups_list = ' '.join(ups_slaves)
          hadoop_input_dir = R('{data_dir}/{app_name}/{current_date}/'
              '{table_name}/', locals())
          file_num += UpsCopySSTable(self.connection, wp, table_name, hadoop_input_dir,
              GenUpsBypassDirList(ups_bypass_dir))
    except Exception as err:
      output = 'ERROR: ' + str(err)
      log.exception(output)
      ret = 1
    finally:
      ret = wp.wait()
      if ret != 0:
        log.error('Copy sstable failed, failed count: {0}'.format(ret))
      for i in inputs:
        table_name = i[1]
        data_dir = hdfs_data_dir
        current_date = task_id
        hadoop_input_dir = R('{data_dir}/{app_name}/{current_date}/'
            '{table_name}/', locals())
        err = DeletePath(hadoop_input_dir, self.connection)
        if err:
          log.error('Delete output data error: {0}'.format(
            hadoop_input_dir))
          ret = 1
    return (ret, file_num)

  def ups_load_bypass(self, import_num):
    ret = 0
    loaded_num = 0
    try:
      ups_admin_load_cmd = R('{bin_dir}/ups_admin -a {rs_ip} -p {rs_port} '
          '-o master_ups -t minor_load_bypass', locals())
      log.debug(R('ups_admin_load_cmd is {ups_admin_load_cmd}', locals()))
      output = Shell.popen(ups_admin_load_cmd)
      log.debug(R('ups_admin_load_cmd output is {output}', locals()))
      loaded_num = ParseLoadedNum(output)
      if import_num != loaded_num:
        err_msg = ('loaded error, the number of sstable is incorrect, '
            'loaded {0} and correct number should be {1}').format(
            loaded_num, import_num)
        log.error(err_msg)
        SendPacket(self.connection, RESPONSE_PKG, err_msg + '\n')
        ret = 1
      else:
        if skip_major_freeze == 1:
          msg = 'skip major freeze'
          log.info(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
        else:
          ups_admin_freeze_cmd = R('{bin_dir}/ups_admin -a {rs_ip} -p {rs_port} '
              '-o master_ups -t major_freeze', locals())
          log.debug(R('ups_admin_freeze_cmd is {ups_admin_freeze_cmd}', locals()))
          output = Shell.popen(ups_admin_freeze_cmd)
          log.debug(R('ups_admin_freeze_cmd output is {output}', locals()))
          if output != '[major_freeze] err=0\n':
            ret = 1
            msg = 'ups_admin_freeze_cmd={0} failed, output={1}'.format(ups_admin_freeze_cmd, output)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    except Exception as err:
      output = 'ERROR in ups load bypass: ' + str(err)
      log.exception(output)
      ret = 1
    return (ret, loaded_num)

  def major_freeze(self):
    ret = 0
    timer = 0
    try:
      while not self.GlobalLock.acquire(False):
        SendPacket(self.connection, RESPONSE_PKG,
            'Waiting for another import task, {0} seconds\n'.format(timer))
        time.sleep(10)
        timer += 10
      ups_admin_freeze_cmd = R('{bin_dir}/ups_admin -a {rs_ip} -p {rs_port} '
          '-o master_ups -t major_freeze', locals())
      log.debug(R('ups_admin_freeze_cmd is {ups_admin_freeze_cmd}', locals()))
      output = Shell.popen(ups_admin_freeze_cmd)
      log.debug(R('ups_admin_freeze_cmd output is {output}', locals()))
      SendPacket(self.connection, RESPONSE_PKG, 'send major freeze cmd, output={0}\n'.format(output))
    except Exception as err:
      output = 'ERROR: ' + str(err)
      log.exception(output)
      ret = 1
    finally:
      try:
        if self.GlobalLock is not None:
          self.GlobalLock.release()
      except Exception:
        pass
    return ret

  def get_table_conf_param(self, table_name):
    ret = 0
    rowkey_desc = None
    delim = 1
    partition_file = None

    try:
      #parse rowkey_desc, delim
      data_syntax = JoinPath(conf_dir, app_name,table_name,  "data_syntax.ini")
      config = ConfigParser.RawConfigParser()
      config.read(data_syntax)
      delim = config.getint("public", "delim")
      config_table_name = config.get("public", "table_name")
      if table_name != config_table_name:
        ret = 1
        log.error("get table conf param for table {0}, but ths table_name in conf({1}) is {2}".format(
          table_name, data_syntax, config_table_name))
      rowkey_desc = config.get(table_name, "rowkey_desc")

      # parse partition_file, if not set, use default in map_reduce_table
      if ret == 0:
        conf_path = JoinPath(conf_dir, app_name, table_name, "configuration.xml")
        tree = ElementTree()
        tree.parse(conf_path)
        root = tree.getroot()
        for child in root:
          name = child.find('name').text
          value = child.find('value').text
          if name == 'mrsstable.partition.file':
            partition_file = value.strip()
            if partition_file == "" or partition_file == "/":
              ret = 1
              log.error("invalid param partition_file={0} in conf {1}".format(partition_file, conf_path))

    except Exception as err:
      ret = 1
      output = 'ERROR: ' + str(err)
      log.exception(output)

    return ret, rowkey_desc, partition_file, delim

  def pre_mr_table(self, task_id, table_name, table_id, hadoop_input_path, rs_ip, rs_port):
    ret = 0
    map_reduce_cmd = None

    try:
      ret, rowkey_desc, partition_file, delim = self.get_table_conf_param(table_name)
      if ret != 0:
        log.error("failed to get table conf for table_name {0}".format(table_name))

      if ret == 0:
        self.clean_old_hadoop_data(table_name, keep_hadoop_data_days)

      if ret == 0:
        job_name = app_name + "-" + table_name + "-" + table_id + "-" + task_id;
        table_etc_dir = JoinPath(conf_dir, app_name, table_name)
        schema = JoinPath(table_etc_dir, "schema.ini")
        mr_configuration = JoinPath(table_etc_dir, "configuration.xml")
        data_syntax = JoinPath(table_etc_dir, "data_syntax.ini")
        achived_conf = JoinPath(table_etc_dir, "config_" + task_id + ".jar")
        mr_table_log = JoinPath(log_dir, app_name, table_name + "_mr.log")

        hadoop_path = JoinPath(hdfs_data_dir, app_name, task_id, table_name)
        hadoop_output_path = JoinPath(hadoop_path, "sstables")
        hadoop_conf_path = JoinPath(hdfs_name + "/" + hadoop_path, "config_" + task_id + ".jar")
        if partition_file == None:
          hadoop_partition_path = JoinPath(hadoop_path, "partition_file_" + task_id)
        else:
          hadoop_partition_path = partition_file

        jar_cmd = JoinPath(java_home, "bin", "jar")
        hadoop_cmd = JoinPath(hadoop_bin_dir, "hadoop")
        mrsstable_jar = JoinPath(bin_dir, "mrsstable.jar")

        achive_conf_cmd = R('cd {table_etc_dir} && {jar_cmd} cvf {achived_conf} data_syntax.ini schema.ini', locals())
        put_conf_cmd = R('cd {table_etc_dir} && {hadoop_cmd} fs -mkdir {hadoop_path} && {hadoop_cmd} fs -put {achived_conf} {hadoop_conf_path}', locals())
        rm_achive_conf_cmd = R('rm -f {achived_conf}', locals())
        map_reduce_cmd = R('{hadoop_cmd} jar {mrsstable_jar} com.taobao.mrsstable.MrsstableRunner -conf {mr_configuration} -archives {hadoop_conf_path} -table_id {table_id} -rowkey_desc {rowkey_desc} -job_name {job_name} -partition_file {hadoop_partition_path} -delim {delim} {hadoop_input_path} {hadoop_output_path}', locals())
        make_log_dir_cmd = 'mkdir -p {0}'.format(JoinPath(log_dir, app_name))

      if ret == 0:
        # 1. update schema
        ret = UpdateSchema(rs_ip, rs_port, schema)
        if ret !=0:
          log.error("failed to update schema, rs_ip={0} rs_port={1} schema={2}".format(rs_ip, rs_port, schema))
        else:
          # 2. achive conf
          output = Shell.popen(achive_conf_cmd)
          log.debug('achive_conf_cmd: {0}, output: {1}'.format(achive_conf_cmd, output))

          # 3. put conf to hadoop
          output = Shell.popen(put_conf_cmd)
          log.debug('put_conf_cmd: {0}, output: {1}'.format(put_conf_cmd, output))

          # 4. rm achived conf
          output = Shell.popen(rm_achive_conf_cmd)
          log.debug('rm_achive_conf_cmd: {0}, output: {1}'.format(rm_achive_conf_cmd, output))

          # 5. mkdir log dir
          output = Shell.popen(make_log_dir_cmd)
          log.debug('make_log_dir_cmd: {0}, output: {1}'.format(make_log_dir_cmd, output))

          # 6. print map_reduce_cmd
          log.debug('map_reduce_cmd: {0}'.format(map_reduce_cmd))
    except Exception as err:
      ret = 1
      log.exception('ERROR: failed to do map_reduce_table table_id={0} table_name={1} hadoop_input_path={2}'.format(table_id, table_name, hadoop_input_path))
    return ret, map_reduce_cmd, mr_table_log

  def overwrite_load_table(self, rs_ip, rs_port, table_name, table_id, hadoop_path):
    ret = 0
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} import {table_name} {table_id} '
        ' proxy://hadoop:/{hadoop_path}', locals())
    log.debug(rs_admin_cmd)
    for retry in range(start_load_table_retry):
      try:
        msg = "try to call rs to load table {0}: {1}".format(retry, rs_admin_cmd)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
        output = Shell.popen(rs_admin_cmd)
        log.debug(msg + ":\n" + output)
        ret = 0
        log.debug("succeed to call rs to load table {0} {1}".format(table_name, table_id))
        break
      except Exception as err:
        log.exception('ERROR: failed to call rs_admin to import table {0}: {1}'.format(retry, err))
        ret = 1
        time.sleep(30)

    return ret

  def overwrite_op_copy_and_load(self, task_id, table_dict):
    ret = ReloadRsAndGateway(self.connection)
    rs_list = []
    if ret == 0:
      rs_list.append(dict(ip=rs_ip, port=rs_port))
      for rs in obi_rs:
        rs_list.append(dict(ip=rs['ip'], port=rs['port']))

      ret, msg = self.task_trace.is_need_kill(task_id)
      if ret != 0:
        msg = "{0} is killed".format(task_id)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg+"\n")

    rs_count = len(rs_list)

    # copy sstable to cs
    if ret == 0:
      ret = self.wait_time_range(task_id)
      if ret != 0:
        log.error('wait_time_range failed')
      else:
        try:
          log.info("start to load tables {0} to {1}".format(table_dict, rs_list))
          for table_name in table_dict:
            table_id = table_dict[table_name]
            hadoop_sstable_path = JoinPath(hdfs_data_dir, app_name, task_id, table_name, "sstables")
            if 0 != self.overwrite_load_table(rs_ip, rs_port, table_name, table_id, hadoop_sstable_path):
              ret = 1
              msg = "failed to call ob to import table: rs_ip={0} rs_port={1} table_name={2} table_id={3} hadoop_sstable_path={4}".format(
                  rs['ip'], rs['port'], table_name, table_id, hadoop_sstable_path)
              SendPacket(self.connection, RESPONSE_PKG, msg + "\n")
              log.error(msg)

          #check
          if ret == 0:
            wp = WorkerPool(128)
            for table_name in table_dict:
              table_id = table_dict[table_name]
              wp.add_task(OverwriteCheckLoadTable(self.connection, rs_ip, table_name, table_id, self.task_trace, task_id))
            ret = wp.wait()
            if ret != 0:
              msg = "failed to load all tables"
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + "\n")

        except Exception as err:
          output = 'ERROR: ' + str(err)
          log.exception(output)
          SendPacket(self.connection, RESPONSE_PKG, "Exception in cs copy sstable for task " + task_id)
          ret = 1

    # print result
    if ret == 0:
      log.info('overwrite_op success: ' + task_id)
    else:
      log.error('overwrite_op failed: ' + task_id)

    return ret

  def overwrite_op(self, task_id, inputs):
    ret = 0
    if ret == 0:
      ret, table_dict = self.overwrite_op_generate_sstable(task_id, inputs)
      if ret != 0:
        msg = 'overwrite_op_generate failed with task_id={0} inputs={1}'.format(task_id, inputs)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg +"\n")

    if ret == 0:
      ret = self.overwrite_op_copy_and_load(task_id, table_dict)
      if ret != 0:
        msg = 'overwrite_op_copy_and_load failed tith task_id={0}'.format(task_id)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg +"\n")
    return ret

  def clean_old_hadoop_data(self, table_name, keep_hadoop_data_days):
    ret = 0
    try:
      if keep_hadoop_data_days < 1:
        keep_hadoop_data_days = 1
        msg = "keep_hadoop_data_days should not less than 1, use 1 for table {0}.".format(table_name)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg +"\n")

      hadoop_app_dir = JoinPath(hdfs_data_dir, app_name)
      ls_cmd = "{0}hadoop fs -ls {1} | awk  '/^d/ {{print $8}}'".format(hadoop_bin_dir, hadoop_app_dir)
      dir_list = Shell.popen(ls_cmd).split('\n')
      log.debug("old_data: {0}\n" .format(dir_list))
      if ret != 0:
        msg = "failed to ls hadoop table dir for clean: table dir = {0}".format(hadoop_app_dir)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg +"\n")

      if ret == 0:
        last_keep_data_time = datetime.datetime.now() + datetime.timedelta(days=-keep_hadoop_data_days)
        msg = "start clean old data in {0} before {1} ({2} days ago)".format(
            hadoop_app_dir, last_keep_data_time.strftime('%Y-%m-%d %H:%M:%S'), keep_hadoop_data_days)
        log.info(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg +"\n")

        for dir_path in dir_list:
          if " " in dir_path:
            msg = "hadoop path should not contains ' ', path={0}".format(dir_path)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg +"\n")
            continue
          if dir_path == "":
            continue
          dir_name = os.path.basename(dir_path)
          m = re.match("^(\d{14})-(\d{6})$", dir_name) #task_id format
          if m == None:
            msg = "unknown dir={0}, skip it.".format(dir_path)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg +"\n")
          else:
            dir_date_time = datetime.datetime.strptime(dir_name, '%Y%m%d%H%M%S-%f')
            if dir_date_time < last_keep_data_time:
              msg = "delete old data dir: {0}".format(dir_path)
              log.info(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg +"\n")
              rm_cmd = "{0}hadoop fs -rmr {1}".format(hadoop_bin_dir, dir_path)
              log.debug(rm_cmd)
              Shell.popen(rm_cmd)
    except Exception as err:
      ret = 1
      output = 'ERROR: ' + str(err)
      log.exception(output)
    return ret

  def overwrite_op_generate_sstable(self, task_id, inputs):
    ret = ReloadRsAndGateway(self.connection)
    table_names = []
    table_dict = dict()

    # get table id
    if ret == 0:
      for i in inputs:
        table_names.append(i[1])
      rs = dict(ip = rs_ip, port = rs_port)
      table_dict = GetCSBypassTableIdList(rs, table_names)
      if len(table_dict) == 0:
        log.error('failed to get temp table id from {0}'.format(rs))
        ret = 1
      if ret == 0:
        ret, msg = self.task_trace.set_task_inputs(task_id, inputs, table_dict)
        if ret != 0:
          log.error(msg)

    # generate
    if ret == 0:
      shell_worker = ShellWorker(self.connection)

      for i in inputs:
        hadoop_input_path = i[0]
        table_name = i[1]
        table_id = table_dict[table_name]
        if table_id == None:
          ret = 1
          log.error('table_id for table {0} must not none', table_name)
          break

        if ret == 0:
          ret, mr_cmd, log_file = self.pre_mr_table(task_id, table_name, table_id, hadoop_input_path, rs_ip, rs_port)
          if ret != 0:
            log.error('failed prepare mr table, table_name={0} table_id={1} hadoop_input_path={2} rs_ip={3} rs_port={4}'.format(
              table_name, table_id, hadoop_input_path, rs_ip, rs_port))
            break

        if ret == 0:
          description = "mapreduce {0}".format(table_name)
          ret = shell_worker.add(mr_cmd, description, log_file)
          if ret != 0:
            log.error('failed to run mr_cmd: {0}'.format(mr_cmd))
            break

      if shell_worker.wait() != 0:
        ret = 1
        log.error("failed to do overwrite_op_generate_sstable task_id={0} inputs={1} ret={2} table_dict={3}".format(task_id, inputs, ret, table_dict))
      else:
        log.info("success to do overwrite_op_generate_sstable task_id={0} inputs={1} ret={2} table_dict={3}".format(task_id, inputs, ret, table_dict))

    # list generated file
    if ret == 0:
      for table_name in table_names:
        table_sstable_path = JoinPath(hdfs_data_dir, app_name, task_id, table_name, "sstables")
        ls_cmd = "{0}hadoop fs -ls {1}".format(hadoop_bin_dir,table_sstable_path)
        dir_list = Shell.popen(ls_cmd)
        log.debug("hadoop fs -ls {0}\n{1}".format(table_sstable_path, dir_list))
    return ret, table_dict

  def import_op(self, task_id, inputs):
    ret = self.import_op_generate_sstable(task_id, inputs)
    if ret != 0:
      msg = 'failed to import_op_generate_sstable, task_id={0}, inputs={1}'.format(task_id, inputs)
      log.error(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

    if ret == 0:
      ret = self.import_op_copy_and_load(task_id, inputs)
      if ret != 0:
        msg = 'failed to import_op_copy_and_load, task_id={0}, inputs={1}'.format(task_id, inputs)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    return ret

  def import_op_generate_sstable(self, task_id, inputs):
    ret = 0

    #get tableids
    table_names = []
    for i in inputs:
      table_names.append(i[1])
    rs = dict(ip = rs_ip, port = rs_port)
    table_dict = GetUpsBypassTableIdList(rs, table_names)
    if table_dict == None:
      log.error('failed to get table id from {0}'.format(rs))
      ret = 1
    if ret == 0:
      tmp_inputs = []
      for i in inputs:
        table_id = table_dict[i[1]]
        if table_id is not None:
          tmp_inputs.append(i + (table_id,))
        else:
          ret = 1
      if ret == 0:
        inputs = tmp_inputs
        ret, msg = self.task_trace.set_task_inputs(task_id, inputs, table_dict)
        if ret != 0:
          log.error(msg)

    if ret == 0:
      #TODO: generate mr sstable
      pass

    return ret

  def import_op_copy_and_load(self, task_id, inputs):
    ret = 0
    if ret == 0:
      try:
        ret = self.wait_time_range(task_id)
        if ret == 0:
          timer = 0
          while not self.GlobalLock.acquire(False):
            SendPacket(self.connection, RESPONSE_PKG,
                'Waiting for another import task, {0} seconds\n'.format(timer))
            time.sleep(10)
            timer += 10
        if ret == 0:
          ret = self.wait_time_range(task_id)
          if ret == 0:
            ret, import_num = self.ups_copy_sstable(task_id, inputs)
            log.debug('copy_sstable returned {0}, import_num = {1}'.format(
              ret, import_num))
        if ret == 0:
          ret = self.wait_time_range(task_id)
          if ret == 0:
            ret, loaded_num = self.ups_load_bypass(import_num)
            log.debug('ups_load_bypass returned {0}'.format(ret))
      finally:
        try:
          if self.GlobalLock is not None:
            self.GlobalLock.release()
        except Exception:
          pass
    if ret == 0:
      log.info('import_op success: ' + task_id)
    else:
      log.error('import_op failed: ' + task_id)

    return ret

  def get_task_stat(self, task_id):
    ret, msg = self.task_trace.get_task_stat(task_id)
    if ret != 0:
      log.error(msg)
    SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    return ret

  def kill_task(self, task_id):
    ret = 0
    task, msg = self.task_trace.kill_task(task_id)
    if task == None: #failed
      ret = 1
      log.error(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    else:
      SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
      if task['type'] == 'REPLICATE':
        ret = 1
        msg = "replicate op is not allow killed. Please kill all the process and clean data manually."
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
      elif task['type'] in ('OVERWRITE', 'IMPORT'):
        # import process will check own taskid's status
        # it will exit if the status is NEED_KILL
        pass

    return ret

  def wait_time_range(self, task_id):
    '''
    if the application config file specify the 'DISPATCH_TIME_RANGE',
    wait the current time in the dispatch time range, and then do
    dispatch sstable work.
    check the application config file once per 10 seconds, so it support
    modifing the config dynamically.
    '''
    try:
      ret = 0
      while True:
        ret, msg = self.task_trace.is_need_kill(task_id)
        if ret == 0:
          pass
        elif ret == 1:
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
        elif ret == 2:
          msg = 'the state of task[{0}] is NEED_KILL, exit!'.format(task_id)
          log.info(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          break
        else:
          log.error("unknown return code " + msg)
          SendPacket(self.connection, RESPONSE_PKG, "unknown return code " + msg + '\n')

        ReloadDispatchTimeRange()
        if dispatch_time_range is None:
          break
        else:
          cur_second = time.time()
          start_second_str = datetime.datetime.now().strftime("%Y-%m-%d ") + part[0]
          start_second = time.mktime(time.strptime(start_second_str, '%Y-%m-%d %H:%M'))
          end_second = start_second + int(part[1]) * 3600
          #miss the dispatch time of today, dispatch on the next day
          if cur_second >= end_second:
            start_second += 24 * 3600
            end_second += 24 * 3600
          #in dispatch time range, just break
          if cur_second >= start_second:
            if cur_second <= end_second:
              break
          log.info("dispatch_time_range is set as {0}, task {1} waiting..."
              .format(dispatch_time_range, task_id))
          time.sleep(10)
    except Exception as err:
      output = 'ERROR: ' + str(err)
      log.exception(output)
      if ret == 0:
        ret = 1
    finally:
      return ret

  def print_bypass_config(self, data):
    ret = 0

    try:
      update_conf_lock.acquire()
      if len(data) != 1:
        ret = 1
        log.error("wrong table name {0}".format(data))
      else:
        table_name = data[0]
        table_dir = JoinPath(conf_dir, app_name, table_name)

      if ret == 0:
        if os.path.isdir(table_dir) == False:
          ret = 1
          msg = "{0} not exists, can't print".format(table_dir)
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

      if ret == 0:
        # print schema
        schema = JoinPath(table_dir, "schema.ini")
        ret = UpdateSchema(rs_ip, rs_port, schema)
        if ret !=0:
          log.error("failed to update schema, rs_ip={0} rs_port={1} schema={2}".format(rs_ip, rs_port, schema))
        else:
          SendPacket(self.connection, RESPONSE_PKG, "schema:\n")
          table_scetion_name = "[{0}]".format(table_name)
          table_section = False
          with open(schema) as f:
            for line in f:
              line = line.strip()
              if line == table_scetion_name:
                table_section = True
                continue
              elif line.startswith("[") and table_section == True:
                table_section = False
                break
              elif table_section == False:
                continue
              else:
                SendPacket(self.connection, RESPONSE_PKG, line + '\n')
            SendPacket(self.connection, RESPONSE_PKG, "\n-----------------------------------\n")

        if ret == 0:
          #print data syntax
          SendPacket(self.connection, RESPONSE_PKG, "data syntax:\n")
          with open(JoinPath(conf_dir, app_name, table_name, "data_syntax.ini")) as f:
            for line in f:
              line = line.strip()
              SendPacket(self.connection, RESPONSE_PKG, line + '\n')
          SendPacket(self.connection, RESPONSE_PKG, "\n-----------------------------------\n")

        if ret == 0:
          #print configuration
          SendPacket(self.connection, RESPONSE_PKG, "xml configuration:\n")
          conf_path = JoinPath(conf_dir, app_name, table_name, "configuration.xml")
          tree = ElementTree()
          tree.parse(conf_path)
          root = tree.getroot()
          for child in root:
            name = child.find('name').text
            value = child.find('value').text
            msg = "{0}: {1}".format(name, value)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

    except Exception as err:
      ret = 1
      msg = "exception happended during print_bypass_config: " + str(err)
      log.exception(msg)
    finally:
      update_conf_lock.release()

    return ret

  def delete_bypass_config(self, data):
    ret = 0

    try:
      update_conf_lock.acquire()
      if len(data) != 1:
        ret = 1
        log.error("wrong table name {0}".format(data))
      else:
        table_name = data[0]
        table_dir = JoinPath(conf_dir, app_name, table_name)

      if ret == 0:
        if os.path.isdir(table_dir) == False:
          ret = 1
          msg = "{0} not exists, can't delete".format(table_dir)
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

      if ret == 0:
        backup_dir_postfix = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        trash_dir = JoinPath(conf_dir, app_name, "_trash")
        if os.path.isdir(trash_dir) == False:
          os.mkdir(trash_dir)

        log.debug("rename {3} {0}/{1}_{2}".format(trash_dir, table_name, backup_dir_postfix, table_dir))
        os.rename(table_dir,  "{0}/{1}_{2}".format(trash_dir, table_name, backup_dir_postfix))

    except Exception as err:
      ret = 1
      msg = "exception happended during delete_bypass_config: " + str(err)
      log.exception(msg)
    finally:
      update_conf_lock.release()

    return ret

  def create_bypass_config(self, data):
    ret = 0
    raw_data_field_count = 0
    delim = 1
    null_flag = 2
    column_infos = None

    try:
      if len(data) < 1 or len(data) > 4:
        ret = 1
        msg = "wrong param for create_bypass_config: {0}".format(data)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
      else:
        table_name = data[0]
        table_dir = JoinPath(conf_dir, app_name, table_name)
        schema = JoinPath(table_dir, "schema.ini")
        for i in range(1, len(data)):
          param = data[i]
          m1 = re.match('^delim=(\d+)$', param)
          m2 = re.match('^column_infos=(\S+)$', param)
          m3 = re.match('^raw_data_field_count=(\d+)$', param)
          m4 = re.match('^null_flag=(\d+)$', param)
          if m1 != None and len(m1.groups()) == 1:
            delim = int(m1.group(1))
            if delim < 1 or delim > 255:
              ret = 1
              msg = "delim must between [1,255], but cur delim is {0}".format(delim)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
              break
          elif m2 != None and len(m2.groups()) == 1:
            column_infos = m2.group(1)
          elif m3 != None and len(m3.groups()) == 1:
            raw_data_field_count = int(m3.group(1))
            if raw_data_field_count <= 0:
              ret = 1
              msg = "raw_data_field_count must larger than 0, but cur is {0}".format(raw_data_field_count)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
              break
          elif m4 != None and len(m4.groups()) == 1:
            null_flag = int(m4.group(1))
            if null_flag < 1 or null_flag > 255:
              ret = 1
              msg = "null_flag must between [1,255], but cur null_flag is {0}".format(null_flag)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
              break
          else:
            ret = 1
            msg = "wrong format of param: {0}, use raw_data_field_count=N or delim=N or or null_flag=N or column_infos=XXX".format(param)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
            break
    except Exception as err:
      ret = 1
      msg = "exception happended during create_bypass_config: " + str(err)
      log.exception(msg)
    
    if null_flag == delim:
      ret = 1
      msg = "null_flag={0} must not equal with delim={1}".format(null_flag, delim)
      log.error(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

    try:
      update_conf_lock.acquire()
      if ret == 0:
        if os.path.exists(table_dir):
          ret = 1
          msg = "{0} already exists, can't create new config for this table".format(table_dir)
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
        else:
          os.makedirs(table_dir)

      if ret == 0:
        # update schema
        ret = UpdateSchema(rs_ip, rs_port, schema)
        if ret !=0:
          log.error("failed to update schema, rs_ip={0} rs_port={1} schema={2}".format(rs_ip, rs_port, schema))

      if ret == 0:
        #copy configuration.xml
        shutil.copy(JoinPath(conf_dir, "configuration.xml"), table_dir)

      if ret == 0:
        # generate data_syntax.ini
        ret = self.generate_data_syntax(table_name, raw_data_field_count, delim, null_flag, column_infos)
        if ret != 0:
          msg = "failed to generate data_syntax.ini for table {0}".format(table_name)
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    except Exception as err:
      ret = 1
      msg = "exception happended during create_bypass_config: " + str(err)
      log.exception(msg)
    finally:
      update_conf_lock.release()

    return ret

  def read_cloumn_infos(self, table_name):
    ret = 0
    table_scetion_name = "[{0}]".format(table_name)
    table_section = False
    column_infos_result = [] #column_id, raw_data_idx, name, type, length, matched
    try:
      with open(JoinPath(conf_dir, app_name, table_name, "schema.ini")) as f:
          for line in f:
            line = line.strip()
            if line == table_scetion_name:
              table_section = True
              continue
            elif line.startswith("[") and table_section == True:
              table_section = False
              break
            elif table_section == False:
              continue
            elif line.startswith("column_info=") == False:
              continue
            #handle column_info=
            items = line.lstrip("column_info=").split(',')
            if len(items) == 4: # int or precise_datetime
              column_id = items[1]
              raw_data_idx = -1
              name = items[2]
              if items[3] == "int":
                type = 1
              elif items[3] == "precise_datetime":
                type = 5
              else:
                ret = 1
                msg = "not support type {0} in schema {1}".format(line, JoinPath(conf_dir, "bypass_schema.ini"))
                log.error(msg)
                SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                break
              length = -1
              matched = False
              column_infos_result.append((column_id, raw_data_idx, name, type, length, matched))

            elif len(items) == 5: #varchar
              column_id = items[1]
              raw_data_idx = -1
              name = items[2]
              if items[3] == "varchar":
                type = 6
              else:
                ret = 1
                msg = "not support type {0} in schema {1}".format(line, JoinPath(conf_dir, "bypass_schema.ini"))
                log.error(msg)
                SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                break
              length = items[4]
              if int(length) <= 0:
                ret =1
                msg = "varchar's length must larger than 0, but column with column_id={1} is {0}".format(length, column_id)
                log.error(msg)
                SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                break

              matched = False
              column_infos_result.append((column_id, raw_data_idx, name, type, length, matched))
            else:
              ret = 1
              msg = "wrong format of schema {0}".format(JoinPath(conf_dir, "bypass_schema.ini"))
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    except Exception as err:
      ret = 1
      msg = "exception happended during read_column_infos for table {0}: {1}".format(table_name, str(err))
      log.exception(msg)
    return ret,column_infos_result


  def match_column_idx(self, column_infos_result, column_infos, raw_data_field_count):
    ret = 0
    try:
      if column_infos == None:
        for idx in range(0, len(column_infos_result)):
          column_id, raw_data_idx, name, type, length, matched = column_infos_result[idx]
          raw_data_idx = idx
          matched = True
          column_infos_result[idx] = (column_id, raw_data_idx, name, type, length, matched)
        if raw_data_field_count == 0:
          raw_data_field_count = len(column_infos_result)
        elif raw_data_field_count < len(column_infos_result):
          ret = 1
          msg = "raw_data_field_count={0} must not less than colum count={1}".format(raw_data_field_count, len(column_infos_result))
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

      else:
        max_raw_data_idx = 0
        column_infos = column_infos.split(',')
        if len(column_infos) != len(column_infos_result):
          ret = 1
          msg = "the count of input column_infos setting is {0}, but the count of column_infos from schema is {1}".format(
              len(column_infos), len(column_infos_result))
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

        if ret == 0:
          for column_info in column_infos:
            m = re.match('^(\S+[^-])-(-|\d+)$', column_info)
            if m == None or len(m.groups()) != 2:
              ret = 1
              msg = "wrong format of column_infos={0}".format(column_infos)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
              break
            else:
              column_name_in= m.group(1)
              raw_data_index_in = m.group(2)
              if raw_data_index_in != '-' and int(raw_data_index_in) >= max_raw_data_idx:
                max_raw_data_idx = int(raw_data_index_in)

              if raw_data_index_in != '-':
                if  int(raw_data_index_in) < 0 or "{0}".format(int(raw_data_index_in)) != raw_data_index_in:
                  ret = 1
                  msg = "wrong raw_data_index_in [{0}], please check setting column_infos={1}".format(raw_data_index_in, column_infos)
                  log.error(msg)
                  SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                  break

              found = False
              for idx in range(0, len(column_infos_result)):
                column_id, raw_data_idx, name, type, length, matched = column_infos_result[idx]
                if name == column_name_in:
                  if matched == True:
                    found = True
                    ret = 1
                    msg = "column[{0}] is matched already with raw_data_idx={1}, cant match {2} again, column_infos={3}".format(
                        column_id, raw_data_idx, raw_data_index_in, column_infos)
                    log.error(msg)
                    SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                    break
                  if raw_data_index_in == '-':
                    raw_data_idx = '-1'
                  else:
                    raw_data_idx = raw_data_index_in
                  matched = True
                  column_infos_result[idx] = (column_id, raw_data_idx, name, type, length, matched)
                  found = True
                  break
              if found != True:
                ret = 1
                msg = "no column_name[{0}] found, please check if the setting column_infos={1} is right".format(column_name_in, column_infos)
                log.error(msg)
                SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                break

        if ret == 0 and raw_data_field_count == 0:
          raw_data_field_count = max_raw_data_idx + 1
        if ret == 0 and max_raw_data_idx >= raw_data_field_count:
          ret = 1
          msg = "max_raw_data_idx={0} must less than raw_data_field_count={1}".format(
              max_raw_data_idx, raw_data_field_count)
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    except Exception as err:
      ret = 1
      msg = "exception happended during math_column_idx {0}".format(str(err))
      log.exception(msg)
    return ret, raw_data_field_count

  def parse_rowkey(self, table_name, column_infos_result):
    ret = 0
    rowkey_desc = [] #raw_data_idx, type, length
    try:
      # parse rowkey item
      schema_path = JoinPath(conf_dir, app_name, table_name, "schema.ini")
      schema_config = ConfigParser.RawConfigParser()
      schema_config.read(schema_path)
      if schema_config.has_option(table_name, "rowkey"):
        rowkey_str = schema_config.get(table_name, "rowkey")
      else:
        ret = 1
        msg = "failed to get rowkey info of {1} from {0}".format(schema_path, table_name)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

      if ret == 0:
        rowkey_fields = rowkey_str.split(',')
        for rowkey_field in rowkey_fields:
          m = re.match(r'^(\S+)\((\S+)%(\S+)\)$', rowkey_field)
          if m == None or len(m.groups()) != 3:
            ret = 1
            msg = "wrong format of rowkey {0} in schema {1}".format(rowkey_str, schema_path)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
            break
          else:
            rowkey_name = m.group(1)
            rowkey_len = m.group(2)
            rowkey_type_name = m.group(3)
            if rowkey_type_name == "int":
              rowkey_type = 1
            elif rowkey_type_name == "precise_datetime":
              rowkey_type = 5
            elif rowkey_type_name == "varchar":
              rowkey_type = 6
            else:
              ret = 1
              msg = "unsupport type {0} in rowkey of table {1} in schema {2}".format(
                  rowkey_type_name, table_name, schema_path)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
              break

            for (column_id, raw_data_idx, name, type, length, matched) in column_infos_result:
              if name == rowkey_name:
                if rowkey_type != type or (rowkey_type == 6 and rowkey_len != length): # type 6 "varchar"
                  ret = 1
                  msg = "varchar len of rowkey [{0}] doesn't equal to [{1}] in schema column_info".format(rowkey_len, length)
                  log.error(msg)
                  SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                  break
                elif int(raw_data_idx) < 0:
                  ret = 1
                  msg = "rowkey field must has input! column_id={0} has wrong raw_data_idx".format(column_id)
                  log.error(msg)
                  SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
                  break
                else:
                  rowkey_desc.append((raw_data_idx, type, length))
    except Exception as err:
      ret = 1
      msg = "exception happended during parse_rowkey for table {0}: {1}".format(table_name, str(err))
      log.exception(msg)
    return ret, rowkey_desc

  def write_data_syntax(self, table_name, raw_data_field_count, delim, null_flag, rowkey_desc, column_infos_result):
    ret = 0
    f = None
    try:
      f = open(JoinPath(conf_dir, app_name, table_name, "data_syntax.ini"), "w")

      f.write("[public]\n")
      f.write("table_name={0}\n".format(table_name))
      f.write("delim={0}\n".format(delim))
      f.write("null_flag={0}\n".format(null_flag))
      f.write("raw_data_field_cnt={0}\n".format(raw_data_field_count))
      f.write("sstable_block_size={0}\n".format(sstable_block_size))

      f.write("[{0}]\n".format(table_name))
      rowkey_desc_str = ""
      for raw_data_idx, type, length in rowkey_desc:
        if len(rowkey_desc_str) > 0:
          rowkey_desc_str += ","
        if type == 6: #varchar
          rowkey_desc_str +="{0}-{1}-{2}".format(raw_data_idx, type, length)
        else:
          rowkey_desc_str +="{0}-{1}".format(raw_data_idx, type)
      f.write("rowkey_desc={0}\n\n".format(rowkey_desc_str))

      for (column_id, raw_data_idx, name, type, length, matched) in column_infos_result:
        f.write("column_info={0},{1} #{2}\n".format(column_id, raw_data_idx, name))

      f.flush()

    except Exception as err:
      ret = 1
      msg = "exception happended during parse_rowkey for table {0}: {1}".format(table_name, str(err))
      log.exception(msg)
    finally:
      if f != None:
        f.close()
    return ret

  def generate_data_syntax(self, table_name, raw_data_field_count, delim, null_flag, column_infos):
    #already in update_conf_lock acquire and schema is already updated
    ret = 0
    try:
      column_infos_result = [] # (column_id, raw_data_idx, column_name, type, length, matched) // len only for varchar
      rowkey_desc = [] # (raw_data_idx, type, length)

      if ret == 0:
        ret, column_infos_result = self.read_cloumn_infos(table_name)
        if ret != 0:
          log.error("failed to read_cloumn_infos table_name {0}".format(table_name))

      if ret == 0:
        ret, raw_data_field_count = self.match_column_idx(column_infos_result, column_infos, raw_data_field_count)
        if ret != 0:
          log.error("failed to match_column_idx table_name {0}, column_infos {1}".format(table_name, column_infos))

      if ret == 0:
        ret, rowkey_desc = self.parse_rowkey(table_name, column_infos_result)
        if ret != 0:
          log.error("failed to parse_rowkey table_name {0}, column_infos {1}".format(table_name, column_infos))

      if ret == 0:
        ret = self.write_data_syntax(table_name, raw_data_field_count, delim, null_flag, rowkey_desc, column_infos_result)
        if ret != 0:
          log.error("failed to write_data_syntax table_name {0}, column_infos {1}".format(table_name, column_infos))

    except Exception as err:
      ret = 1
      msg = "exception happended during generate_data_syntax for table {0}: {1}".format(table_name, str(err))
      log.exception(msg)

    return ret

  def handle(self):
    need_reset_task = False
    try:
      ret = 0
      (packettype, data) = ReceivePacket(self.connection)
      log.info("handle packettype: {0} data: {1}".format(packettype, data))
      if packettype == None and data == None:
        log.debug("recieve None packettype and None data type, maybe monitor check by dba")
      elif packettype == SUBMIT_STATE_TASK_PKG:
        ret = self.get_task_stat(data)
      elif packettype == DELETE_BYPASS_CONFIG:
        ret = self.delete_bypass_config(data)
      elif packettype == PRINT_BYPASS_CONFIG:
        ret = self.print_bypass_config(data)
      elif packettype == CREATE_BYPASS_CONFIG:
        ret = self.create_bypass_config(data)
      elif packettype == SUBMIT_KILL_TASK_PKG:
        ret = self.kill_task(data)
      elif packettype == SUBMIT_MAJOR_FREEZE_PKG:
        ret = self.major_freeze()
      elif packettype == OVERWRITE_CONTINUE_TASK_PKG:
        task_id = data
        ret, task, msg = self.task_trace.continue_task(task_id, packettype)
        if ret != 0 or task == None:
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
        else:
          need_reset_task = True
          inputs = task['inputs']
          task_type = task['type']
          table_dict = task['table_dict']
          if packettype == OVERWRITE_CONTINUE_TASK_PKG and task_type == 'OVERWRITE':
            if ret == 0:
              ret = self.overwrite_op_copy_and_load(task_id, table_dict)
              if ret != 0:
                msg = 'overwrite_op_copy_and_load failed, task_id={0}, inputs={1} table_dict={2}'.format(task_id, inputs, table_dict)
                log.error(msg)
                SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          else:
            msg = 'cannot continue task with task_id={0} packettype={1} task type={2}'.format(task_id, packettype, task_type)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
      elif packettype in (SUBMIT_OVERWRITE_TASK_PKG, OVERWRITE_GENERATE_TASK_PKG):
        task_id = datetime.datetime.now().strftime('%Y%m%d%H%M%S-%f')
        table_name_list = []
        if packettype == SUBMIT_OVERWRITE_TASK_PKG:
          task_type = 'OVERWRITE'
        elif packettype == OVERWRITE_GENERATE_TASK_PKG:
          task_type = 'OVERWRITE_GENERATE'
        else:
          ret = 1

        cmd = task_type
        for i in data:
          cmd += " {0} {1}".format(i[0], i[1])
          if i[1] in table_name_list:
            ret = 1
            msg = "there are more than two {0} tables in input param, cant load same table twice".format(i[1])
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          table_name_list.append(i[1])
        log.debug("importcli cmd: " + cmd)

        if ret == 0:
          for table_name in table_name_list:
            table_dir = JoinPath(conf_dir, app_name, table_name)
            if os.path.exists(table_dir) == False:
              ret = 1
              msg = "no bypass config found for table {0}".format(table_name)
              log.error(msg)
              SendPacket(self.connection, RESPONSE_PKG, msg + '\n')

        if ret == 0:
          ret, msg = self.task_trace.add_task(task_id, task_type, table_name_list, data)
          if ret != 0:
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          else:
            need_reset_task = True
            log.info(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
            if packettype == SUBMIT_OVERWRITE_TASK_PKG:
              ret = self.overwrite_op(task_id, data)
            elif packettype ==  OVERWRITE_GENERATE_TASK_PKG:
              ret, table_dict = self.overwrite_op_generate_sstable(task_id, data)
            else:
              ret = 1
      else:
        ret = 1
        msg = 'wrong pkg code: {0}'.format(packettype)
        log.error(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    except Exception as err:
      ret = 1
      log.exception("ERROR " + str(err))
      SendPacket(self.connection, RESPONSE_PKG, 'FAILED\n')
    finally:
      if need_reset_task:
        end_ret, end_msg = self.task_trace.end_task(task_id, ret)
        if end_ret == 0 and ret == 0:
          log.info(end_msg)
          SendPacket(self.connection, RESPONSE_PKG, end_msg + '\n')
        else:
          ret = 1
          log.error(end_msg)
          SendPacket(self.connection, RESPONSE_PKG, end_msg + '\n')

    if packettype == None and data == None:
      pass
    else:
      if ret == 0:
        SendPacket(self.connection, END_PKG, 'SUCCESSFUL\n')
      else:
        SendPacket(self.connection, END_PKG, 'FAILED\n')

def GenUpsBypassDirList(pattern):
  match = re.match(r'^(.+)raid\[(\d+)-(\d+)\]/store\[(\d+)-(\d+)\]/bypass$', pattern)
  if match is None or len(match.groups()) != 5:
    raise Exception('wrong bypass dir configuration: ' + pattern)
  dir_base = match.group(1)
  raid_start = int(match.group(2))
  raid_end = int(match.group(3))
  store_start = int(match.group(4))
  store_end = int(match.group(5))
  if raid_start > raid_end or store_start > store_end:
    raise Exception('wrong bypass dir configuration: ' + pattern)

  ups_dirs = []
  raid_index = raid_start
  store_index = store_start
  while raid_index <= raid_end:
    while store_index <= store_end:
      ups_dirs.append("{0}raid{1}/store{2}/bypass".format(dir_base, raid_index, store_index))
      store_index += 1
    raid_index += 1
    store_index = store_start
  return ups_dirs


def ClearBypassDir(path, ups_slaves):
  trash_dir = R('{path}/trash', locals())
  mk_trash_dir = R('mkdir -p {trash_dir}', locals())
  clear_cmd = R("""for f in `ls -l {path} | grep -v "^total" | grep -v "^d" | awk "{{print \\\$NF}}"`; """
      """do mv {path}/$f {trash_dir}; done""", locals())
  tmp_dir = R('{path}/tmp', locals())
  rm_tmp_dir = R('rm -rf {tmp_dir}', locals())
  mkdir_tmp_dir = R('mkdir -p {tmp_dir}', locals())
  r = Shell.popen(mk_trash_dir)
  log.debug('execute `{0}`, returned {1}'.format(mk_trash_dir, r))
  r = Shell.popen(clear_cmd)
  log.debug('execute `{0}`, returned {1}'.format(clear_cmd, r))
  r = Shell.sh(rm_tmp_dir)
  msg = 'execute `{0}`, returned {1}'.format(rm_tmp_dir, r)
  log.debug(msg)
  if r != 0:
    raise ExecutionError(msg)
  r = Shell.sh(mkdir_tmp_dir)
  msg = 'execute `{0}`, returned {1}'.format(mkdir_tmp_dir, r)
  log.debug(msg)
  if r != 0:
    raise ExecutionError(msg)
  for ups in ups_slaves:
    r = Shell.popen(mk_trash_dir, host = ups)
    log.debug('execute `{0}` on {1}, returned {2}'.format(
        mk_trash_dir, ups, r))
    r = Shell.popen(clear_cmd, host = ups)
    log.debug('execute `{0}` on {1}, returned {2}'.format(
        clear_cmd, ups, r))
    r = Shell.popen(rm_tmp_dir, host = ups)
    log.debug('execute `{0}` on {1}, returned {2}'.format(
        rm_tmp_dir, ups, r))
    r = Shell.popen(mkdir_tmp_dir, host = ups)
    log.debug('execute `{0}` on {1}, returned {2}'.format(
        mkdir_tmp_dir, ups, r))


def GetUpsList(rs):
  '''
  Parsing updateserver addresses from output of 'rs_admin stat -o ups' command
  '''
  try:
    rs_ip = rs['ip']
    rs_port = rs['port']
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} '
        'stat -o ups', locals())
    ups_list = dict(master = '', slaves = [])
    try:
      output = Shell.popen(rs_admin_cmd)
      ups_str_list = output.splitlines()[-1].split('|')[-1].split(',')
      for ups_str in ups_str_list:
        m = re.match(r'(.*)\(.* (.*) .* .* .* .*\)', ups_str)
        if m is not None:
          addr = m.group(1).split(':')[0]
          if m.group(2) == 'master':
            ups_list['master'] = addr
          else:
            ups_list['slaves'].append(addr)
        elif ups_str != '':
          log.error("error rs output: `{0}'".format(ups_str))
      if ups_list['master'] == '' or ups_list['master'] == None:
        raise Exception('oceanbase {0} has no ups master'.format(rs_ip))
    except ExecutionError as err:
      log.exception('ERROR: ' + str(err))
    finally:
      return ups_list
  except Exception as err:
    output = 'ERROR: ' + str(err)
    log.exception(output)
    return None

def GetCsList(rs_ip, rs_port):
  '''
  Parsing chunkserver addresses from output of 'rs_admin stat -o cs' command
  '''
  ret = 0
  cs_list = []
  try:
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} stat -o cs', locals())
    log.info(rs_admin_cmd)
    output = Shell.popen(rs_admin_cmd)
    cs_str_list = output.splitlines()[-1].strip().split(' ')
    if len(cs_str_list) < 2 or cs_str_list[0] != 'chunkservers:':
      ret = 1
      log.error("failed to get cs list from {0}:{1}".format(rs_ip, rs_admin_cmd))
    for idx in range(1, len(cs_str_list)):
      cs_str = cs_str_list[idx]
      m = re.match(r'^(\d+.\d+.\d+.\d+):(\d+)$', cs_str)
      if m is not None:
        addr = m.group(1)
        cs_list.append(addr)
      else:
        ret = 1
        log.error("error rs output: `{0}'".format(output))
        break
  except ExecutionError as err:
    ret = 1
    log.exception('ERROR: ' + str(err))
  if ret != 0:
    cs_list = None
  return cs_list

def DeletePath(path, conn=None):
  ret = 0
  try:
    if " " in path:
      msg = "hadoop path should not contains ' ', path={0}".format(path)
      log.error(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg +"\n")
    else:
      hadoop_ls_cmd = R('{hadoop_bin_dir}hadoop fs -rmr {path}', locals())
      ret = Shell.sh_and_print(hadoop_ls_cmd, conn)
  except Exception as err:
    ret = 1
    output = 'ERROR: ' + str(err)
    log.exception(output)
  return ret

def ParseLoadedNum(output):
  try:
    loaded_num = int(output.strip().splitlines()[-1].split('=')[-1])
    return loaded_num
  except Exception as err:
    output = 'ERROR: ' + str(err)
    log.exception(output)
    return 0

class GetUpsDataFromHadoop:
  def __init__(self, connection, task, table_name, i):
    '''task should be a dict with these fields:
         dir
         files
    '''
    self.connection = connection
    self.task = task
    self.ret = 0
    self.table_name = table_name
    self.dir_seq = i

  def __call__(self):
    err = 0
    dir = self.task['dir']
    files = self.task['files']
    dest_dir = dir
    tmp_dir = R('{dest_dir}/tmp', locals())
    succ_files_count = 0
    skip_files_count = 0
    for f in files:
      m = re.match(r'.*/([^/]+)', f)
      if m is None:
        skip_files_count += 1
        msg = "skip file: {0}".format(f)
        log.info(msg)
        SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
      else:
        ts = int(time.time() * 1000000)
        filename = '{0}_{1}'.format(m.group(1), ts)
        hadoop_get_cmd = R('{hadoop_bin_dir}hadoop fs -get {f} {tmp_dir}/{filename}', locals())
        commit_mv_cmd = R('mv {tmp_dir}/{filename} {dest_dir}', locals())
        log.debug(hadoop_get_cmd)
        log.debug(commit_mv_cmd)
        err = Shell.sh_and_print(hadoop_get_cmd, self.connection)
        if err != 0:
          self.ret = 1
          msg = R('Failed to get "{filename}" from hadoop to "{dest_dir}"', locals())
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          return self.ret
        err = Shell.sh(commit_mv_cmd)
        if err != 0:
          self.ret = 1
          msg = R('Failed to mv "{filename}" to "{dest_dir}"', locals())
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
          return self.ret
        else:
          msg = R('Successfully get "{filename}" to "{dest_dir}"', locals())
          log.info(msg)
        copy_ups_list = copy.copy(ups_slaves)
        copy_ups_list.append(ups_master)

        for obi in obi_list:
          copy_ups_list.append(obi['master'])
        for h in copy_ups_list:
          rsync_cmd = R('rsync -e \"ssh -oStrictHostKeyChecking=no -c arcfour\" '
              '-av --inplace --bwlimit={rsync_band_limit} '
              '{dest_dir}/{filename} {h}:{dest_dir}', locals())
          log.debug(rsync_cmd)
          err = Shell.sh(rsync_cmd)
          if err != 0:
            self.ret = 1
            msg = R('Failed to push "{filename}" to ups slave '
                '{h}:{dest_dir}', locals())
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
            return self.ret
          else:
            msg = R('Successfully push "{filename}" to ups slave '
                '{h}:{dest_dir}', locals())
            log.info(msg)
        for obi in obi_list:
          for slave in obi['slaves']:
            rsync_cmd = R('rsync -e \"ssh -oStrictHostKeyChecking=no\" '
                '-avz --inplace --bwlimit={rsync_band_limit} '
                '{dest_dir}/{filename} {slave}:{dest_dir}', locals())
            log.debug(rsync_cmd)
            master_addr = obi['master']
            err = Shell.sh(rsync_cmd, host = master_addr)
            if err != 0:
              self.ret = 1
              l = R('Failed to push "{filename}" from {master_addr} '
                  'to ups slave {slave}:{dest_dir}', locals())
              log.error(l)
              SendPacket(self.connection, RESPONSE_PKG, l + '\n')
              return self.ret
            else:
              l = R('Successfully push "{filename}" from {master_addr} '
                  'to ups slave {slave}:{dest_dir}', locals())
              log.info(l)
        succ_files_count += 1
      msg = "table_name={3} dir_seq={4} succ_files_count={0} skip_files_count={1} total_files_count_this_dir={2}".format(
         succ_files_count, skip_files_count, len(files), self.table_name, self.dir_seq)
      log.info(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg + '\n')
    return self.ret

def RoundInc(n, ceiling):
  n += 1
  if n >= ceiling:
    n = 0
  return n

def UpsCopySSTable(connection, wp, table_name, input_dir, data_dir_list):
  dir_num = len(data_dir_list)
  tasks = []
  for i in range(dir_num):
    tasks.append(dict(disk_id=None, files=[]))
  file_list = []

  hadoop_ls_cmd = R("{hadoop_bin_dir}hadoop fs -ls {input_dir}", locals())
  log.debug(hadoop_ls_cmd)

  ls_output = Shell.popen(hadoop_ls_cmd).split('\n')
  for l in ls_output:
    log.debug("ls output: " + l)
    m = re.match(r'^.* ([^ ]+)$', l)
    if m is not None:
      fullpath = m.group(1)
      filename = os.path.basename(fullpath)
      if filename.startswith("part-"):
        file_list.append(fullpath)
  log.debug("file_list: " + str(file_list))

  disk_index = 0
  total_file_count = 0
  for f in file_list:
    if f != '':
      tasks[disk_index]['files'].append(f)
      disk_index = RoundInc(disk_index, dir_num)
      total_file_count += 1
  msg = 'table_name={0} total_file_count={1} dir_num={2}'.format(table_name, total_file_count, dir_num)
  log.info(msg)
  if connection != None:
    SendPacket(connection, RESPONSE_PKG, msg + '\n')

  for i in range(dir_num):
    tasks[i]['dir'] = data_dir_list[i]
    log.debug(str(tasks[i]))
    wp.add_task(GetUpsDataFromHadoop(connection, tasks[i], table_name, i))

  return len(file_list)

class OverwriteCheckLoadTable:
  def __init__(self, connection, rs_ip, table_name, table_id, task_trace, task_id):
    '''
    invoke the dispatch.sh script to dispatch sstables
    '''
    self.connection = connection
    self.ret = 0
    self.rs_ip = rs_ip
    self.table_name = table_name
    self.table_id = table_id
    self.task_trace = task_trace
    self.task_id = task_id

  def __call__(self):
    try:
      self.ret = self.wait_load_table_result(self.rs_ip, self.table_name, self.table_id)
    except Exception as err:
      log.exception('ERROR: failed to overwrite import table ' + str(err))
      ret = 1

    return self.ret

  def wait_load_table_result(self, rs_ip, table_name, table_id):
    finish = False
    ret = 0
    fail_count = 0
    sleep_interval = 30
    count = load_table_timeout / sleep_interval + 2
    cmd = R('mysql --connect_timeout=30 -N -h {rs_ip} -P {ob_port} -u {ob_user} -p{ob_password} -e "select * from load_data_history where table_id = {table_id} and table_name = \'{table_name}\' order by start_time desc"', locals())
    log.debug(cmd)
    for i in range(0, count):
      try:
        cur_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        output = Shell.popen(cmd)
        log.debug(output)
        line = output.split("\n")[0]
        items = line.split("\t")
        if len(items) != 6:
          ret = 1
          log.error("size of items should be 6, but current is {0}, items={1}".format(len(items), items))
          break
        elif items[2] != table_name or items[3] != str(table_id):
          ret = 1
          log.error("wrong sql result {0}".format(items))
          break
        elif items[4] == 'DOING':
          msg = "{3}: {0:>20}:{1:10} DOING     start_time={2:>20}".format(table_name, table_id, items[0], cur_time)
          log.info(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg+"\n")
          ret, msg =  self.task_trace.is_need_kill(self.task_id)
          if ret != 0:
            msg = "{0} is killed".format(self.task_id)
            log.error(msg)
            SendPacket(self.connection, RESPONSE_PKG, msg+"\n")
            break
          time.sleep(sleep_interval)
          fail_count = 0 #reset fail_count
          continue
        elif items[4] == 'DONE':
          finish = True
          msg = "{4}: {0:>20}:{1:10}  DONE     start_time={2:>20} end_time={3:>20}".format(table_name, table_id, items[0], items[1], cur_time)
          log.info(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg+"\n")
          break
        else:
          ret = 1
          msg = "load table {0} failed, status={1}".format(table_name, items[4])
          log.error(msg)
          SendPacket(self.connection, RESPONSE_PKG, msg+"\n")
          break
      except Exception as err:
        fail_count += 1
        log.exception('WARN: failed to check load table status: ' + str(err))
        if fail_count > 3:
          ret = 1
          log.error("failed to wait load table 3 times, stop wait")
          break

    if ret == 0 and finish == False:
      ret = 1
      msg = "load table {0} timeout, timeout={1}s".format(table_name, load_table_timeout)
      log.error(msg)
      SendPacket(self.connection, RESPONSE_PKG, msg+"\n")
    return ret


def UpdateSchema(rs_ip, rs_port, schema_path):
  ret = 0
  try:
    update_conf_lock.acquire()
    conf_dir = globals()['conf_dir']
    rs_admin_cmd = R('rm -f {schema_path} && {bin_dir}/rs_admin -r {rs_ip} -p {rs_port} '
        'print_schema -o location={schema_path}', locals())

    output = Shell.popen(rs_admin_cmd)
    log.debug('rs_admin_cmd: {0}, output: {1}'.format(rs_admin_cmd, output))
    expect_start_title = "write schema to file. file_name"
    if expect_start_title not in output:
      log.exception("failed to update schema.ini")
  except Exception as err:
    ret = 1
    output = 'ERROR: ' + str(err)
    log.exception(output)
  finally:
    update_conf_lock.release()
    return ret

def GetCSBypassTableIdList(rs, table_names):
  '''
  Parsing temp table_name->table_id map from output of
  'cs_admin -i "fetch_bypass_table_id"' command
  return type is dict of 'table_name->table_id'
  '''
  try:
    rs_ip = rs['ip']
    rs_port = rs['port']
    table_names_str = ','.join(table_names)
    cs_admin_cmd = R('{bin_dir}/cs_admin -t 60 -q -s {rs_ip} -p {rs_port} '
        '-i "fetch_bypass_table_id {table_names_str}"', locals())
    table_list = []
    table_dict = {}

    output = Shell.popen(cs_admin_cmd)
    log.debug('cs_admin_cmd: {0}, output: {1}'.format(cs_admin_cmd, output))
    expect_start_title = "fetch_bypass_table_id> "
    if not output.startswith(expect_start_title):
      log.exception("failed to get bypass table id list")
    else:
      table_str_list = output[len(expect_start_title):].split(' ')
      for table_str in table_str_list:
        m = re.match(r'^(.*):([0-9]+)', table_str)
        if m is not None:
          table_list.append([m.group(1), m.group(2)])
        else:
          log.error("wrong output: {0}".format(table_str_list))
          table_dict = {}
          break
      if len(table_list) > 0:
        table_dict = dict(table_list)
        log.debug('overwrite bypass table id is: {0}'.format(table_dict))
  except Exception as err:
    output = 'ERROR: ' + str(err)
    log.exception(output)
    table_dict = {}
  finally:
    if len(table_dict) == 0:
      log.exception("table_dict is empty")
    return table_dict

def GetUpsBypassTableIdList(rs, table_names):
  table_dict = dict()
  try:
    rs_ip = rs['ip']
    rs_port = rs['port']
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} print_schema | grep table=', locals())
    table_list = []
    table_dict = {}

    output = Shell.popen(rs_admin_cmd)
    log.debug('rs_admin_cmd: {0}\noutput: {1}'.format(rs_admin_cmd, output))
    for line in output.splitlines():
      m = re.match(r'table=(\S+) id=(\d+)', line)
      if m == None or len(m.groups()) != 2:
        table_dict = None
        log.error('failed to parse ups bypass table id: {0}'.format(line))
        break
      else:
        table_name = m.group(1)
        table_id = m.group(2)
        table_dict[table_name] = table_id

  except Exception as err:
    output = 'ERROR: ' + str(err)
    log.exception(output)
    table_dict = None
  finally:
    if table_dict == None:
      log.exception("table_dict is {0}, but table_names is {1}".format(table_dict, table_names))
    return table_dict

def IsMaster(vip, port, connection = None):
  '''
  Check if the rs belongs to the master cluster
  '''
  try:
    is_master = False
    rs_ip = vip
    rs_port = port
    rs_admin_cmd = R('{bin_dir}/rs_admin -r {rs_ip} -p {rs_port} stat', locals())
    output = Shell.popen(rs_admin_cmd)
    if 'obi_role: MASTER' in output:
      is_master = True
    return is_master
  except Exception as err:
    msg = 'failed to call {0}'.format(rs_admin_cmd)
    if connection != None:
      SendPacket(connection, RESPONSE_PKG, msg + '\n')
    log.exception(msg)
    return None

def ReloadRsAndGateway(connection = None):
  ret = 0
  config = ConfigParser.RawConfigParser()
  config.read(config_file)

  rs_ip = None
  rs_port = None
  obi_rs = []
  obi_count = 0

  if config.has_option('ob_instances', 'obi_count'):
    obi_count = config.getint('ob_instances', 'obi_count')

  if obi_count <= 0:
    ret =1
    msg = "obi count should not less than 1"
    log.error(msg)
    if connection != None:
      SendPacket(connection, RESPONSE_PKG, msg + '\n')
  else:
    for i in range(obi_count):
      remote_rs_ip = config.get('ob_instances', 'obi{0}_rs_vip'.format(i))
      remote_rs_port = config.getint('ob_instances', 'obi{0}_rs_port'.format(i))
      if len(remote_rs_ip) == 0:
        msg = "obi setting error, please check obi setting"
        if connection != None:
          SendPacket(connection, RESPONSE_PKG, msg + '\n')
        log.error(msg)
        ret = 1
      else:
        tmp_ret = IsMaster(remote_rs_ip, remote_rs_port, connection)
        if tmp_ret == None:
          ret = 1
          msg = "failed to check if the {0}:{1} is master rs".format(remote_rs_ip, remote_rs_port)
          log.error(msg)
          if connection != None:
            SendPacket(connection, RESPONSE_PKG, msg + '\n')
        elif tmp_ret == True:
          if rs_ip == None:
            rs_ip = remote_rs_ip
            rs_port = remote_rs_port
          else:
            msg = "more than one master cluster found, please check obi setting."\
                "old {0} cur {1}".format(rs_ip, remote_rs_ip)
            log.error(msg)
            if connection != None:
              SendPacket(connection, RESPONSE_PKG, msg + '\n')
            ret = 1
        else: #tmp_ret == False
          obi_rs.append(dict(ip=remote_rs_ip, port=remote_rs_port))

    if rs_ip == None:
      msg = "no master cluster found in obi list, please check obi setting"
      log.error(msg)
      if connection != None:
        SendPacket(connection, RESPONSE_PKG, msg + '\n')

  if ret == 0:
    if 'rs_ip' not in globals() \
        or globals()['rs_ip'] != rs_ip \
        or globals()['rs_port'] != rs_port \
        or globals()['obi_rs'] != obi_rs \
        or globals()['obi_count'] != obi_count:
      msg = "update rs: rs_ip={0}, rs_port={1}, obi_count={2}, obi_rs={3}".format(
          rs_ip, rs_port, obi_count, obi_rs)
      if connection != None:
        SendPacket(connection, RESPONSE_PKG, msg + '\n')
      globals()['rs_ip']        = rs_ip
      globals()['rs_port']      = rs_port
      globals()['obi_count']    = obi_count
      globals()['obi_rs']       = obi_rs
    else:
      msg = "curs: rs_ip={0}, rs_port={1}, obi_count={2}, obi_rs={3}".format(
          rs_ip, rs_port, obi_count, obi_rs)
    log.info(msg)

  else:
    msg = "failed to reload rs"
    log.error(msg)
    if connection != None:
      SendPacket(connection, RESPONSE_PKG, msg + '\n')
  return ret

def ReloadDispatchTimeRange():
  ret = 0
  config = ConfigParser.RawConfigParser()
  config.read(config_file)
  if config.has_option('public', 'dispatch_time_range'):
    dispatch_time_range = config.get('public', 'dispatch_time_range')
  else:
    dispatch_time_range = None
  if dispatch_time_range != None:
    part = dispatch_time_range.split(',')
    if len(part) != 2:
      log.error('invalid dispatch time range format, {0}',
          dispatch_time_range)
      ret = 1
    elif part[1] <= 0:
      log.error('invalid dispatch time range format, {0}',
          dispatch_time_range)
      ret = 1
    else:
      m = re.match('^(\d+):(\d+)$', part[0])
      if m == None or len(m.groups()) != 2:
        ret = 1
      else:
        hour = int(m.group(1))
        minute = int(m.group(2))
        if hour < 0 or hour >= 24:
          ret = 1
        elif minute < 0 or minute >= 60:
          ret = 1

  if ret == 0:
    if 'dispatch_time_range' not in globals()\
        or globals()['dispatch_time_range'] != dispatch_time_range:
      log.info('dispatch_time_range => ' + str(dispatch_time_range))
      globals()['dispatch_time_range'] = dispatch_time_range
  else:
    log.error('wrong format of dispatch_time_range: ' + str(dispatch_time_range))

  return ret

def ParseArgs():
  try:
    parser = optparse.OptionParser()
    parser.add_option('-f', '--config_file',
        help='Configuration file',
        dest='config_file', default='importserver.conf')
    (options, args) = parser.parse_args()
    config_file = options.config_file

    if os.path.isfile(config_file) == False:
      print "config file {0} is not a file, please check if it exists.".format(config_file)
      parser.print_help()
      sys.exit(1)

    config = ConfigParser.RawConfigParser()
    config.read(config_file)

    app_name = config.get('public', 'app_name')
    if len(app_name) == 0:
      msg = "invalide app name, cant start\n"
      sys.stderr.write(msg)
      exit(1)

    ob_user = config.get('public', 'ob_user')
    ob_password = config.get('public', 'ob_password')
    ob_port = config.getint('public', 'ob_port')
    load_table_timeout = config.getint('public', 'load_table_timeout')
    start_load_table_retry = config.getint('public', 'start_load_table_retry')
    if start_load_table_retry < 1:
      msg = "invalide start_load_table_retry={0}, must larger then 0".format(start_load_table_retry)
      sys.stderr.write(msg)
      exit(1)

    if config.has_option('public', 'sstable_block_size'):
      sstable_block_size = config.getint('public', 'sstable_block_size')
    else:
      sstable_block_size = 65536


    if config.has_option('public', 'dispatch_time_range'):
      dispatch_time_range = config.get('public', 'dispatch_time_range')
    else:
      dispatch_time_range = None

    # all path
    base_dir = config.get('public', 'base_dir')

    if config.has_option('public', 'conf_dir'):
      conf_dir = config.get('public', 'conf_dir')
    else:
      conf_dir = JoinPath(base_dir, 'etc/')

    if config.has_option('public', 'bin_dir'):
      bin_dir = config.get('public', 'bin_dir')
    else:
      bin_dir = JoinPath(base_dir, 'bin/')

    if config.has_option('public', 'log_dir'):
      log_dir = config.get('public', 'log_dir')
    else:
      log_dir = JoinPath(base_dir, 'log/')

    if config.has_option('public', 'run_dir'):
      run_dir = config.get('public', 'run_dir')
    else:
      run_dir = JoinPath(base_dir, 'run/')

    # hadoop env
    if config.has_option('public', 'java_home'):
      java_home = config.get('public', 'java_home')
    else:
      java_home = '/usr/java/jdk1.6.0_13/'

    if config.has_option('public', 'hadoop_home'):
      hadoop_home = config.get('public', 'hadoop_home')
    else:
      hadoop_home = '/home/hadoop/hadoop-current/'

    if config.has_option('public', 'hadoop_bin_dir'):
      hadoop_bin_dir = config.get('public', 'hadoop_bin_dir')
    else:
      hadoop_bin_dir = hadoop_home + '/bin/'

    if config.has_option('public', 'hadoop_conf_dir'):
      hadoop_conf_dir = config.get('public', 'hadoop_conf_dir')
    else:
      hadoop_conf_dir = '/home/admin/config/'

    keep_hadoop_data_days = config.getint('public', 'keep_hadoop_data_days')
    if keep_hadoop_data_days < 1:
      msg = "keep_hadoop_data_days must not less than 1\n"
      sys.stderr.write(msg)
      exit(1)

    # hdfs path
    hdfs_name   = config.get('public', 'hdfs_name')
    hdfs_data_dir = config.get('public', 'hdfs_data_dir')

    # import server
    listen_port = config.getint('import_server', 'port')
    log_level   = config.get('import_server', 'log_level')

    if config.has_option('import_server', 'pid_file'):
      pid_file = config.get('public', 'pid_file')
    else:
      pid_file = JoinPath(run_dir, 'importserver.pid')

    if config.has_option('import_server', 'log_file'):
      log_file = config.get('import_server', 'log_file')
    else:
      log_file = JoinPath(log_dir,'importserver.log')

    if config.has_option('import_server', 'keep_task_state_count'):
      keep_task_state_count = config.getint('import_server', 'keep_task_state_count')
    else:
      keep_task_state_count = 512

    CheckPid(pid_file)

    import logging
    LEVELS = {'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL}
    level = LEVELS.get(log_level.lower(), logging.NOTSET)
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('[%(asctime)s] %(levelname)s  %(funcName)s (%(filename)s:%(lineno)d) [%(thread)d] %(message)s')
    handler.setFormatter(formatter)
    log = logging.getLogger("importserver")
    log.addHandler(handler)
    log.setLevel(level)
    globals()['log'] = log

    log.info('config_file => ' + str(config_file))
    log.info('dispatch_time_range => ' + str(dispatch_time_range))
    log.info('pid_file    => ' + str(pid_file))
    log.info('java_home   => ' + str(java_home))
    log.info('hadoop_home => ' + str(hadoop_home))
    log.info('hadoop_conf_dir => ' + str(hadoop_conf_dir))
    log.info('keep_hadoop_data_days=> ' + str(keep_hadoop_data_days))
    log.info('listen_port => ' + str(listen_port))
    log.info('keep_task_state_count => ' + str(keep_task_state_count))
    log.info('app_name    => ' + app_name)
    log.info('ob_user => ' + ob_user)
    log.info('ob_password => ' + ob_password)
    log.info('ob_port => ' + str(ob_port))
    log.info('load_table_timeout=> ' + str(load_table_timeout))
    log.info('start_load_table_retry=> ' + str(start_load_table_retry))
    log.info('sstable_block_size=> ' + str(sstable_block_size))
    log.info('conf_dir    => ' + conf_dir)
    log.info('bin_dir     => ' + bin_dir)
    log.info('log_dir     => ' + log_dir)
    log.info('hadoop_bin_dir  => ' + hadoop_bin_dir)
    log.info('hdfs_name       => ' + hdfs_name)
    log.info('hdfs_data_dir   => ' + hdfs_data_dir)
    globals()['config_file']  = config_file
    globals()['dispatch_time_range'] = dispatch_time_range
    globals()['pid_file']     = pid_file
    globals()['java_home']    = java_home
    globals()['hadoop_home']  = hadoop_home
    globals()['hadoop_conf_dir']  = hadoop_conf_dir
    globals()['keep_hadoop_data_days'] = keep_hadoop_data_days
    globals()['listen_port']  = listen_port
    globals()['keep_task_state_count'] = keep_task_state_count
    globals()['app_name']     = app_name
    globals()['ob_user']      = ob_user
    globals()['ob_password']  = ob_password
    globals()['ob_port']      = ob_port
    globals()['load_table_timeout']      = load_table_timeout
    globals()['start_load_table_retry']      = start_load_table_retry 
    globals()['sstable_block_size']     = sstable_block_size
    globals()['conf_dir']     = conf_dir
    globals()['bin_dir']      = bin_dir
    globals()['log_dir']      = log_dir
    globals()['hadoop_bin_dir'] = hadoop_bin_dir
    globals()['hdfs_name']    = hdfs_name
    globals()['hdfs_data_dir'] = hdfs_data_dir

    ret = ReloadRsAndGateway(None)
    if ret != 0:
      message = "failed to load rs, maybe no master rs is alive. please check the importserver.conf[ob_instances] and rs status\n"
      sys.stderr.write(message)
      sys.exit(1)
    ret = ReloadDispatchTimeRange()
    if ret != 0:
      message = "dispatch time range is wrong, please check the importserver.conf\n"
      sys.stderr.write(message)
      sys.exit(1)

    os.putenv('JAVA_HOME', java_home)
    os.putenv('HADOOP_HOME', hadoop_home)
    os.putenv('HADOOP_CONF_DIR', hadoop_conf_dir)

  except Exception as err:
    sys.stderr.write(str(err)+"\n")
    if 'log' in globals():
      log.exception('ERROR: ' + str(err))
    sys.exit(1)

def CheckPythonVersion():
  version = platform.python_version()
  m = re.match(r'^\d+.\d+', version)
  version = m.group(0)
  if version < 2.6:
    print "python version should not less than 2.6"
    sys.exit(1)

def Main():
  CheckPythonVersion()
  ParseArgs()
  Daemonize()
  try:
    server = ImportServer(("", listen_port), RequestHandler)
    server.serve_forever()
  except Exception as err:
    log.exception("ERROR: {0}".format(err))
  finally:
    if server is not None:
      server.shutdown()

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
  Main()
