#!/usr/bin/ruby -w

# sample program illustrating use of dynamic thread pool

%w{ optparse ostruct ../devolve.rb }.each{ |f| require f }

# A Job is a single work unit submitted to a thread pool
class Job

  # file_data -- entire content of /var/lib/dpkg/status
  # packages  -- hash of package name to parsed content
  # mutex     -- for synchronized access to @@packages
  #
  @@file_data, @@packages, @@mutex = nil, {}, Mutex.new

  def self.set_file_data data
    @@file_data = data
  end  # set_file_data

  def self.get_packages
    @@packages
  end  # set_file_data

  # the thread pool needs these methods:
  #   get_work   -- return string that will be sent to worker
  #   put_result -- argument is the unmodified string from worker or nil which indicates
  #                 an unexpected error such as worker crash or bug

  # pairs -- pairs of [start, end] indices into file for stanzas
  attr :pairs

  def initialize p    # array of index pairs: beginning, end of package stanza
    raise "File data is nil" if !@@file_data
    raise "Argument is nil" if !p
    raise "No pairs" if p.empty?
    @pairs = p
  end  # initialize

  def get_work    # marshal array of stanzas
    s = @@file_data
    Marshal.dump @pairs.inject( [] ){ |m, (i, j)| m << s[ i ... j ] }
  end  # get_work

  def put_result r
    log = Log.instance
    if !r
      # this is due to a protocol bug where expected strings were not sent or worker
      # crashed causing socket failure; re-enqueue job -- do later
      #
      log.error "Unexpected worker failure while parsing stanza (%d,%d): %s" %
        [i_beg, i_end, result.err_msg]
      return
    end

    # unmarshal result as a Result object
    result = Marshal.load r
    if !result.success
      log.error "Failed to parse stanza (%d,%d): %s" % [i_beg, i_end, result.err_msg]
      return
    end

    # processing was successful; iterate over array of hashes
    @@mutex.synchronize {
      result.result.each{ |h|
        n, a = h[ 'Package' ], h[ 'Architecture' ]
        raise "Package name missing" if !n           # should never happen
        raise "Architecture missing" if !a           # should never happen
        name, arch = n.strip, a.strip
        raise "Package name blank" if name.empty?    # should never happen
        raise "Architecture blank" if arch.empty?    # should never happen

        # save result in global hash
        key = [name, arch]
        raise "Duplicate package: %s" % name if @@packages[ key ]
        @@packages[ key ] = h
        log.debug "Package: %s" % name
      }  # each
    }    # mutex
  end  # put_result
end  # Job

# Extracts data about all installed packages
module DebPkg

  DEF_FILE = '/var/lib/dpkg/status'  # default package data file
  DEF_JOB_SIZE = 4                   # max stanzas per job

  # commandline options parsed and stored here
  # -f : file of debian package description stanzas
  # -p : port (optional)
  # -q : size of job queue
  # -s : size of job (i.e. no. of stanzas per job)
  #
  @@options = OpenStruct.new( :file => nil, :port => nil, :queue_size => nil,
                              :job_size => nil )

  @@file_data = nil

  # read entire file content into class variable
  def self.read_file path
    raise "File not found: #{path}"    if !File.exist? path
    raise "File not readable: #{path}" if !File.readable? path
    raise "File empty: #{path}"        if 0 == File.size( path )
    @@file_data = IO.read path
    Job.set_file_data @@file_data
  end  # read_file

  # find package stanza boundaries and enqueue jobs to parse them
  def self.parse pool    # arg is thread pool

    # pos -- beginning of next stanza
    # idx -- end of next stanza
    # len -- size of file
    # max -- max no. of stanzas per job
    # cnt -- no. of jobs
    #
    max = @@options.job_size ? @@options.job_size : DEF_JOB_SIZE
    s   = @@file_data
    len = s.size
    log = Log.instance
    pos, cnt, stanzas = 0, 0, []
    loop do
      # each stanza ends with a blank line, so look for double newlines
      idx = s.index "\n\n", pos
      raise "Blank line not found after pos = #{pos}" if !idx

      stanzas << [pos, idx]                     # add stanza to current set
      pos = idx + 2
      next if pos < len && stanzas.size < max   # set not yet full and more data remains

      # end of data or set full; enqueue job in either case
      #log.debug "Creating job with: %s" % stanzas.to_s
      job = Job.new stanzas       # create job and enqueue in pool
      pool.add job
      stanzas = []
      cnt += 1
      break if pos >= len
    end  # loop
    log.info "Queued %d jobs" % cnt
  end  # parse

  def self.parse_args    # parse commandline arguments
    opt = OptionParser.new
    opt.on( '-h', '--help', 'Show option summary' ) {
      puts opt; exit
    }

    # worker name
    opt.on( '-f', '--file PATH', 'Path to input file' ) { |path|
      path.strip!
      raise "Blank name" if path.empty?
      raise "File not found: #{path}" if ! File.exist? path
      raise "File not readable: #{path}" if ! File.readable? path
      @@options.file = path
    }
    opt.on( '-p', '--port PORT', 'port number' ) { |p|
      p.strip!
      raise "Blank port" if p.empty?
      port = p.to_i
      raise "port too small: #{port}" if port < 1024
      raise "port too large: #{port}" if port > 65535
      @@options.port = port
    }
    opt.on( '-q', '--queue SIZE', 'size of job queue' ) { |q|
      q.strip!
      raise "Blank queue size" if q.empty?
      size = q.to_i
      raise "queue size too small: #{size}" if size < 1
      raise "queue size too large: #{size}" if size > 1_000_000_000
      @@options.queue_size = size
    }
    opt.on( '-s', '--job-size SIZE', 'size of job' ) { |s|
      s.strip!
      raise "Blank job size" if s.empty?
      size = s.to_i
      raise "job size too small: #{size}" if size < 1
      raise "job size too large: #{size}" if size > 100
      @@options.job_size = size
    }

    opt.parse ARGV

    # both arguments are optional
    @@options.freeze

  end  # parse_args

  def self.go
    Log.init( :name => 'boss.log' )
    parse_args
    path = @@options.file ? @@options.file : DEF_FILE
    read_file path

    # initialize thread pool listener port if it different from the default
    h = {}
    h [ :port ] = @@options.port if
      @@options.port && Constants::DEF_PORT != @@options.port
    h [ :queue_size ] = @@options.queue_size if
      @@options.queue_size && Constants::DEF_QUEUE_SIZE != @@options.queue_size
    Devolve.init( h ) if ! h.empty?

    pool = Devolve.instance
    parse pool
    pool.close
    pool.thr_boss.join
    puts "Processed %d packages" % Job.get_packages.size
  end  # go
end  # DebPkg

DebPkg.go
