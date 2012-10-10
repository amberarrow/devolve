# Some Utility modules and classes
#
# Author: Ram (Munagala V. Ramanath)
#
# Copyright 2012 Munagala V. Ramanath.
#
require 'logger'

module Constants    # common constants
  DEF_QUEUE_SIZE = 5000                    # default job queue size
  DEF_FILE       = '/var/lib/dpkg/status'  # default package data file
  DEF_PORT       = 11_111                  # default listener port on boss

  ACK, QUIT = 'ack', 'quit'    # acknowledge, quit tokens
end  # Constants

# Wrapper for result of computation: created in worker process and unmarshalled in the
# boss (application)
#
class Result
  # success -- true iff computation was successful
  # result  -- result (object) of computation if success == true (undefined otherwise)
  # err_msg -- error message (String) for failed computation if success == false
  #            (undefined otherwise)
  #
  attr :success, :err_msg, :result

  def initialize s, m, r
    @success = s
    if s
      @result = r
    else
      @err_msg = m
    end
  end  # initialize

end  # Result

class Util

  # write message string to socket; used by both boss and workers
  def self.send_str s, msg      # s is socket, msg is string to send
    # logging omitted to minimize overhead; enable if necessary
    #log = Log.instance

    #log.debug "Thread #{Thread.current[:name]}: sending msg, length = #{msg.size}"
    bsize = msg.bytesize        # size in bytes (may differ from character size !)
    s.puts bsize                # write size in characters
    nbytes = s.write msg        # write string
    s.flush

    # we expect that Ruby does the necessary looping to complete the write, so this
    # exception should never trigger
    #
    raise "Thread #{Thread.current[:name]}: partial write, #{nbytes} != #{bsize}" if
      nbytes != bsize

    #log.debug "Thread #{Thread.current[:name]}: waiting for ack"
    ack = s.gets
    raise "Thread #{Thread.current[:name]}: Got nil instead of ACK" if ack.nil?
    ack.strip!
    raise "Thread #{Thread.current[:name]}: Bad ACK #{ack}" if Constants::ACK != ack
    #log.debug "#{pfx}: Got ack"
  end  # send_str

  # read string data from socket and return it; used by both boss and workers
  #
  # + read size of data in bytes (sz); if it is QUIT, that constant is returned
  # + read sz bytes using multiple reads if necessary
  # + send ACK
  # + return result
  #
  def self.recv_str s    # arg is socket
    # length of string
    msg = s.gets
    raise "Thread #{Thread.current[:name]}: msg(1): Got nil" if msg.nil?
    msg.strip!
    return Constants::QUIT if Constants::QUIT == msg

    bsize = msg.to_i    # size in bytes (may differ from size in chars !)

    # read message
    data = s.read bsize
    raise "Thread #{Thread.current[:name]}: msg(2):  Got nil" if data.nil?

    while !(len = bsize - data.bytesize).zero? do   # need another read
      str = s.read len
      raise "Thread #{Thread.current[:name]}: (data loop) Got nil" if str.nil?
      raise "Thread #{Thread.current[:name]}: (data loop) need #{len} bytes, got 0" if
        str.empty?
      data += str
    end

    # send ACK
    s.puts Constants::ACK

    return data
  end  # recv_str

end  # Util

# single common logger for all classes
class Log < Logger
  include Singleton

  # default name, maximum size (bytes) and number of log files
  F_NAME, F_SIZE, F_COUNT = 'main.log', 4_000_000, 2

  # logger details:
  #
  # f_name  -- name prefix for log files
  # f_cnt   -- no. of log filesfile name prefix
  # f_size  -- max size of log files
  #
  @@f_name, @@f_cnt, @@f_size = F_NAME, F_COUNT, F_SIZE

  # initialize logger details -- _must_ be called by a single thread before the first
  # invocation of the instance() method; throws exception if called after because the
  # unique instance has already been created.
  #
  def self.init( args )      # args is a hash
    if defined? @@created    # single instance already created
      warn "Logger #{@@f_name} already initialized"
      return
    end
    raise "No arguments" if !args || args.empty?

    # all parameters optional
    name = args[ :name ]
    if name
      name = name.strip
      raise "Empty file name" if name.empty?
      @@f_name = name
    end

    c = args[ :count ]    # file count
    if c
      raise "Bad file count" if (c < 1 || c > 100)
      @@f_cnt = c
    end

    sz = args[ :size ]    # file size
    if sz
      raise "File size too small #{sz}" if sz < 32_000
      raise "File size too large #{sz}" if sz > 2**30
      @@f_size = sz
    end
  end  # init

  def initialize
    super @@f_name, @@f_cnt, @@f_size
    self.datetime_format = "%H:%M:%S"
    # levels are: DEBUG, INFO, WARN, ERROR and FATAL
    self.level = Logger::DEBUG
    @@created = true
  end  # initialize
end  # Log
