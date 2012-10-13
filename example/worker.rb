#!/usr/bin/ruby

# Copyright 2012 Munagala V. Ramanath. All rights reserved.

# application _must_ provide compute.rb which has a suitable Worker#compute method
%w{ optparse ostruct singleton socket ../util.rb ./compute.rb }.each{ |f| require f }

# Example use of dynamic thread pool
# Worker process for distributed parsing of debian package list
#
class Worker

  # commandline options parsed and stored here
  # -n : unique worker name
  # -b : boss IP or hostname
  # -p : port (optional)
  #
  @@options = OpenStruct.new( :name => nil, :boss => nil, :port => nil )

  # name   : name of worker (sent to boss)
  # socket : connection to boss
  # port   : boss port
  # cnt    : count of jobs completed
  #
  attr_reader :name, :socket, :port, :cnt

  def initialize    # assume all error checking already done
    log = Log.instance
    @name   = @@options.name
    @port   = @@options.port ? @@options.port : Constants::DEF_PORT
    log.info "Worker %s: Opening connection to %s:%d" % [@name, @@options.boss, @port]
    @socket = TCPSocket.new @@options.boss, @port
    @cnt = 0
  end  # initialize

  # log connection details
  def log_connection
    log = Log.instance
    s = @socket
    log.info "Starting worker #{@name}, pid = #{$$}"

    # log connection details
    msg = sprintf( "Connected:\n  self:\n" +
                   "    port = %d\n    hostname = %s\n    hostIP = %s\n" +
                   "  generating server:\n" +
                   "    port = %d\n    hostname = %s\n    hostIP = %s\n",
                   *s.addr[1..3], *s.peeraddr[1..3] )
    log.info msg
  end  # log_connection

  # entry point
  def run
    Thread.current[ :name ] = @name
    log = Log.instance
    begin
      log_connection
      s = @socket

      # first, send our name, process id so server can uniquely identify us
      s.puts @name; s.puts $$

      # main loop for processing jobs
      loop do
        # get data string ...
        data = Util.recv_str s
        break if Constants::QUIT == data

        # ... pass it on to application function and get results ...
        pkg = compute data

        # ... and finally, send results to server
        Util.send_str s, pkg
        @cnt += 1
      end  # loop

      # output stats
      log.info "Worker %s finished, pid = %d, cnt = %d" % [@name, $$, cnt]
      # normal exit

    rescue => ex
      log.error "Worker %s failed, pid = %d; cnt = %d: %s: %s\n" %
        [@name, $$, cnt, ex.class.name, ex.message]
      log.error ex.backtrace.join( "\n" )
      # error exit
    end  # begin

    s.close    # close socket

  end  # run

  def self.parse_args    # parse commandline arguments
    # cannot use logger here since the worker name is not yet known
    opt = OptionParser.new
    opt.on( '-h', '--help', 'Show option summary' ) {
      puts opt; exit
    }

    # worker name
    opt.on( '-n', '--name NAME', 'Name of worker' ) { |name|
      name.strip!
      raise "Blank name" if name.empty?
      @@options.name = name
    }
    opt.on( '-b', '--boss HOST', 'IP/hostname of boss' ) { |boss|
      boss.strip!
      raise "Blank boss" if boss.empty?
      @@options.boss = boss
    }
    opt.on( '-p', '--port PORT', 'port number' ) { |p|
      p.strip!
      raise "Blank port" if p.empty?
      port = p.to_i
      raise "port too small: #{port}" if port < 1024
      raise "port too large: #{port}" if port > 65535
      
      @@options.port = port
    }

    opt.parse ARGV

    # worker name and boss required, port optional
    raise "Missing worker name (-n)"      if !@@options.name
    raise "Missing boss IP/hostname (-b)" if !@@options.boss
    @@options.freeze

  end  # parse_args

  def self.go
    t1 = Time.now
    parse_args    # must be done before initializing logger since we need the name
    Log.init( :name => @@options.name + '.log' )
    log = Log.instance

    s = Worker.new
    s.run

    t2 = Time.now
    log.info "Started: #{t1}"; log.info "Finished: #{t2}"
  end  # go
end  # Worker

Worker.go
