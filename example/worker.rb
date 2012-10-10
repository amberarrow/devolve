#!/usr/bin/ruby

# Copyright 2012 Munagala V. Ramanath. All rights reserved.

%w{ logger optparse ostruct singleton socket ../util.rb }.each{ |f| require f }

# Example use of dynamic thread pool
# Worker process for distributed parsing of debian package list
#
class Worker

  # commandline options parsed and stored here
  # -n : unique worker name
  # -b : boss IP or hostname
  # -p : port (optional)
  #
  @@options = OpenStruct.new( :name => nil, :boss => nil, :port => Constants::DEF_PORT )

  # name   : name of worker (sent to boss)
  # socket : connection to boss
  # port   : boss port
  # cnt    : count of jobs completed
  #
  attr_reader :name, :socket, :port, :cnt

  def initialize    # assume all error checking already done
    @name   = @@options.name
    @port   = @@options.port
    @socket = TCPSocket.new @@options.boss, @@options.port
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

  # regular expressions for various fields
  R_gen = /^([-0-9a-zA-Z]+):\s*(.*)$/o    # generic key value pair
  R_con = /^\s+/o                         # continuation lines begin with white space

  def parse stanza    # parse a package stanza and return hash of all key-value pairs
    # The stanzas look like this:
    #
    # Package: xserver-xorg-input-vmmouse
    # Status: install ok installed
    # Priority: optional
    # Section: x11
    # Installed-Size: 176
    # Maintainer: Ubuntu X-SWAT <ubuntu-x@lists.ubuntu.com>
    # Architecture: amd64
    # Version: 1:12.6.5-4ubuntu2
    # Replaces: mdetect (<< 0.5.2.2), xserver-xorg (<< 6.8.2-35)
    # Provides: xserver-xorg-input-7
    # Depends: libc6 (>= 2.7), xserver-xorg-core (>= 2:1.6.99.900), xserver-xorg-input-mouse, udev
    # Description: X.Org X server -- VMMouse input driver to use with VMWare
    #  This package provides the driver for the X11 vmmouse input device.
    #  .
    #  The VMMouse driver enables support for the special VMMouse protocol
    #  that is provided by VMware virtual machines to give absolute pointer
    #  positioning.
    #  .
    #  The vmmouse driver is capable of falling back to the standard "mouse"
    #  driver if a VMware virtual machine is not detected. This allows for
    #  dual-booting of an operating system from a virtual machine to real hardware
    #  without having to edit xorg.conf every time.
    #  .
    #  More information about X.Org can be found at:
    #  <URL:http://www.X.org>
    #  <URL:http://xorg.freedesktop.org>
    #  <URL:http://lists.freedesktop.org/mailman/listinfo/xorg>
    #  .
    #  This package is built from the X.org xf86-input-vmmouse driver module.
    # Original-Maintainer: Debian X Strike Force <debian-x@lists.debian.org>
    #

    # log     -- logger
    # h       -- hash holding parsed results
    # success -- true iff parsing was successful
    # err_msg -- error message if parsing was not successful
    # prev    -- previous value to append continuation lines
    #
    log, h, success, err_msg, prev = Log.instance, {}, true, nil, nil

    log.debug "Job %d: stanza size %d" % [@cnt, stanza.size]
    stanza.each_line{ |line|
      # Don't do this since continuation lines start with a blank
      # line.strip!
      if line.empty?    # we should never see a blank line
        err_msg = "Unexpected: blank line"
        log.debug err_msg
        success = false
        break
      end

      if line !~ R_gen      # does not match "key: value" pattern
        if line =~ R_con    # continuation line
          if prev
            prev += line    # append to previous value
            next
          end

          err_msg = "No prev value to append continuation line"      # error
          log.debug err_msg
          success = false
          break
        end  # R_con match
        # error
        err_msg = "Match failed for: %s" % line
        #log.debug err_msg
        success = false
        break
      end

      # we have a key-value pair
      k, v = $1, $2
      #log.debug "Match: key = %s, val = %s" % [k, v]
      raise "Key #{k} already defined as #{h[k]}; v = #{v}" if h[ k ]
      h[ k ] = prev = v
    }
    
    pname = h[ 'Package' ]
    log.debug "Job %d: %d pairs for package %s" % [@cnt, h.size, pname]
    return Marshal.dump Result.new( success, err_msg, h )
  end  # parse

  # entry point
  def run
    Thread.current[ :name ] = @name
    log = Log.instance
    begin
      log_connection
      s = @socket

      # first, send our name, process id so server can uniquely identify us
      s.puts @name; s.puts $$

      # main loop for processing stanzas
      loop do
        # get stanza string containing package description
        data = Util.recv_str s
        break if Constants::QUIT == data

        pkg = parse data

        # send results to server
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
