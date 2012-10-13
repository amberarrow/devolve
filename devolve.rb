# Devolve -- A Simple Tool for Distributed Computation
#
# Author: Ram (Munagala V. Ramanath)
#
# Copyright 2012 Munagala V. Ramanath.
#
%w{ socket thread singleton ../util.rb }.each{ |m| require m }

# proxy for a single worker process; the run() method here has the main loop and is
# executed by a new thread when a new worker connects.
#
class WorkerProxy
  # name     : name of worker; each worker _must_ have a unique name (IP address is not
  #            enough since multiple workers may be running on the same host). This name
  #            is set when the worker is started (e.g. on the command line, from a
  #            config file or algorithmically created at runtime) and sent to the boss
  #            when the worker connects. We don't check for uniqueness currently.
  # socket   : socket to worker
  # peer     : array [port, hostname, IP]
  # n_jobs   : total number of jobs processed so far
  # remote_pid : process id of remote end
  # queue    : shared job queue
  # status   : :busy (alive), :done (terminated normally), :error (terminated with error)
  #
  attr_reader :name, :socket, :peer, :n_jobs, :remote_pid, :queue
  attr_accessor :status

  def initialize( nm, sock, pid )
    raise "name is nil" if nm.nil?
    raise "name is empty" if nm.empty?
    @name = nm.strip
    raise "name is blank" if @name.empty?
    @remote_pid, @status = pid, nil

    @socket = sock
    @peer = sock.peeraddr[ 1..3 ]    # peeraddr: [family, port, hostname, IP]
    @n_jobs = 0
  end  # initialize

  # send next job to worker and get back results
  def send_receive data
    #log = Log.instance

    #log.debug "Thread #{@name}: sending data"
    Util.send_str @socket, data

    #log.debug "Thread #{@name}: receiving result"
    r = Util.recv_str @socket
    return r
  end  # send_receive

  # main entry point for proxy threads
  def run
    log, pool = Log.instance, Devolve.instance
    loop {
      job = pool.queue.pop              # blocks if queue is empty
      if Constants::QUIT == job         # terminate
        pool.queue << Constants::QUIT   # put it back so other threads can see it
        break
      end
      
      # get work data, send to worker and process result
      r = send_receive job.get_work
      job.put_result r
      @n_jobs += 1
    }  # main loop

    @socket.puts Constants::QUIT    # ask worker to terminate
    sleep 1
    @socket.close
    log.info "Thread #{Thread.current[:name]} terminating normally; processed " +
             "#{n_jobs} jobs"
  end  # run

end  # WorkerProxy

# Singleton thread pool
class Devolve
  include Singleton

  # class variables used as parameters to initialize
  #
  @@port = Constants::DEF_PORT              # default port to listen on
  @@queue_size = Constants::DEF_QUEUE_SIZE  # default job queue size

  # initialize pool parameters -- must be invoked before first call of instance()
  def self.init( args )      # args is a hash
    if defined? @@created    # single instance already created
      Log.instance.warn "Devolve already initialized"
      return
    end
    raise "No arguments" if !args || args.empty?

    p = args[ :port ]
    if p
      raise "port too small: #{p}" if p < 1024
      raise "port too large: #{p}" if p > 65535
      @@port = p
    end

    q = args[ :queue_size ]
    if q
      raise "queue size too small: #{p}" if q < 1
      raise "queue size too large: #{p}" if q > 1_000_000_000
      @@queue_size = q
    end
  end  # init

  # queue       -- job queue
  # port        -- port to listen on for worker connections
  # thr_boss    -- master thread that listens for connections from workers
  # thr_workers -- list of pairs [p, t] where p is a WorkerProxy object and t the
  #                associated thread.
  # closed      -- if true, pool is closed; main thread waits for all WorkerProxies to
  #                terminate and exits
  #
  attr :queue, :port, :thr_boss, :thr_workers, :closed

  def initialize
    @closed = false
    @port = @@port
    @queue = SizedQueue.new @@queue_size
    @thr_workers = []
    @thr_boss = Thread.new {
      begin
        Thread.current[ :name ] = 'pool_listener'
        run
        Log.instance.info "Pool listener thread exiting"
        # normal exit
      rescue => ex
        log = Log.instance
        log.error "Pool listener thread failed: %s: %s\n" % [ex.class.name, ex.message]
        log.error ex.backtrace.join( "\n" )
        # error exit
      end  # begin
    }
  end  # initialize
  
  def log_connection s   # log connection details
    log  = Log.instance
    name = Thread.current[ :name ]
    msg  = sprintf( "Thread #{name}: Connected:\n  self:\n" +
                    "    port = %d\n    hostname = %s\n    hostIP = %s\n" +
                    "  worker:\n" +
                    "    port = %d\n    hostname = %s\n    hostIP = %s\n",
                    *s.addr[1..3], *s.peeraddr[1..3] )
    log.info msg
  end  # log_connection

  # run by thread-pool listener thread:
  # -- open socket and listen for workers
  # -- when a connection is made, create a WorkerProxy thread for the worker and have it
  #    pull jobs from the queue and run them
  # -- terminate by invoking close() or adding Constants::QUIT to job queue
  #
  def run
    log  = Log.instance
    name = Thread.current[ :name ]

    # accept connections from workers
    log.info "Thread #{name}: opening server socket on port #{@port}"
    server = TCPServer.open @port
    sockets = [server].freeze
    loop do
      # use select to accept connections from slaves -- 30 sec timeout
      # change to use EPoll -- do later
      #
      ready = select( sockets, nil, nil, 30 )

      if ready.nil?         # timed out
        break if @closed    # terminate
        log.info "Thread #{name}: select timeout"
        next                # wait some more
      end  # ready.nil?

      # server socket is ready, so we have a connection
      readable = ready[ 0 ]
      raise "Thread #{name}: Wrong number of ready sockets: #{readable.size}" if
        1 != readable.size
      raise "Thread #{name}: Bad ready socket" if server != readable[ 0 ]
      client = server.accept

      # log connection details
      log_connection client

      # get worker name
      msg = client.gets
      raise "Thread #{name}: Worker connection closed before getting name" if msg.nil?
      wname = msg.strip
      raise "Thread #{name}: empty name" if wname.empty?
      log.info "Thread #{name}: Got name = #{wname}"

      # get pid
      msg = client.gets
      raise "Thread #{name}: Worker connection closed before getting pid" if msg.nil?
      pid = msg.strip.to_i
      raise "Thread #{name}: bad pid = #{pid}" if pid <= 0
      log.info "Thread #{name}: Worker pid = #{pid}"

      # start proxy thread to handle connection and go back to waiting
      proxy = WorkerProxy.new( wname, client, pid )
      thr = Thread.new( proxy ) { |p|
        begin
          Thread.current[ :name ] = p.name
          p.status = :busy
          p.run
          p.status = :done
          # normal termination
        rescue => ex
          p.status = :error
          log = Log.instance
          log.error "Thread #{p.name}: Thr on #{p.peer}/#{p.remote_pid} failed"
          log.error "Thread #{p.name}: #{ex.class}: #{ex.message}"
          log.error ex.backtrace.join( "\n" )
        end
      }
      @thr_workers << [proxy, thr]
      log.info "Thread #{name}: Worker thread #{proxy.name} started, " +
                "ip/pid = #{proxy.peer}/#{proxy.remote_pid}"
      Thread.pass
    end  # loop
    server.close
    wrapup
  end  # run

  # wrapup on normal termination; invoked by pool-listener thread
  def wrapup
    log, name = Log.instance, Thread.current[:name]
    log.info "Thread #{name}: Wrapping up"
    @thr_workers.each_with_index { |(p, t), i|
      prefix = "Thread #{name}: Proxy #{p.name} on #{p.peer}/#{p.remote_pid}"
      case p.status
      when :done then t.join   # normal case
      when :error then
        log.error prefix + " (i=#{i}) got error"
        t.join
      when :busy then
        log.info prefix + " (i=#{i}) still busy ..."
        t.join
        log.info prefix + " (i=#{i}) ... finished"
      else
        raise "Thread #{name}: #{i}: Bad status: #{p.status}"
      end
    }  # @thr_workers iterator
    @thr_workers.clear
    @thr_workers = nil
  end  # wrapup

  # add job to queue; adding Constants:QUIT terminates pool and all threads in it
  def add job
    @queue << job
  end  # add
  
  def close    # invoked by client/application thread
    log, name = Log.instance, Thread.current[ :name ]
    if @closed
      log.warn "Thread #{name}: Already closed"
      return
    end
    log.info "Thread #{name}: Closing thread pool"
    @closed = true
    @queue << Constants::QUIT
  end  # close

end  # Devolve

if $0 == __FILE__
  # trivial test: starts pool main thread which listens on socket but not much else
  # see files under 'example' for a more elaborate test
  #
  Thread.current[ :name ] = 'main'
  pool = Devolve.instance
  sleep 60
  pool.close
end
