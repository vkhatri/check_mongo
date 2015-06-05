#!/usr/bin/env ruby
require 'optparse'
require 'json'
require 'mongo'
require 'time'

options = { :host => '127.0.0.1', :port => 27_017, :user => nil, :password => nil, :timeout => 5 }
valid_checks = %w(connect rs_cluster secondary_nodes node_state total_db_size connect_time queues connections sync_lag).sort

OptionParser.new do |opt|
  opt.banner = "Usage: #{$PROGRAM_NAME} command <options>"

  opt.separator ''
  opt.separator 'Plugin options:'

  opt.on('-H', '--host [HOSTADDRESS]', 'mongodb host address, default: 127.0.0.1') { |h| options[:host] = h }
  opt.on('-P', '--port [PORT]', 'mongodb port, default: 6379') { |h| options[:port] = h }
  opt.on('-u', '--user [USER]', 'mongodb user, default: nil') { |h| options[:password] = h }
  opt.on('-p', '--password [PASSWORD]', 'Password (Default: blank)') { |h| options[:password] = h }
  opt.on('-t', '--timeout [TIMEOUT]', Integer, 'mongodb connect timeout (s), default: 10') { |h| options[:timeout] = h }
  opt.on('-c', '--checks [CHECK,CHECK]', Array, 'mongodb checks, syntax: check_name:warning:critical') { |h| options[:checks] = h }

  opt.on('-v', '--version', 'show version') do
    puts 'check_mongo ruby mongo monitoring plugin, v0.1.1'
    exit
  end

  opt.on_tail('-h', '--help', 'print usage') do
    puts opt
    exit 0
  end
end.parse!

run_checks = options[:checks].map { |o| o.split(':')[0] }.sort
unknown_checks = run_checks - valid_checks

fail "unknown checks '#{unknown_checks.join(' ')}', valid checks are '#{valid_checks.join(' ')}'" if unknown_checks.length > 0

unless options[:checks]
  printf 'CRITICAL: missing --checks [CHECKS]'
  exit 2
end

# Mongo Metrics Monitoring
class CheckMongoDB
  attr_accessor :connection, :opts, :critical_checks, :warning_checks, :ok_checks, :exit_perfdata, :stats, :thresholds, :databases, :rsstats, :connect_time, :admin

  def initialize(options = {})
    @opts = options
    @connection = connect
    @exit_perfdata = ''
    @critical_checks = ''
    @warning_checks = ''
    @ok_checks = ''

    # default check thresholds
    @thresholds = {
      :check_total_connections => { :warning => 10_000, :critical => 20_000 },
      :check_connections => { :warning => 80, :critical => 90 },
      :check_queues => { :warning => 200, :critical => 300 },
      :check_total_db_size => { :warning => 25.0, :critical => 50.0 },
      :check_node_state => { :warning => 6, :critical => 8 },
      :check_connect_time => { :warning => 1.0, :critical => 2.0 },
      :check_secondary_nodes => { :warning => 1, :critical => 1 },
      :check_sync_lag => { :warning => 180, :critical => 300 }
    }
  end

  # connect to mongo instance
  def connect
    s = Time.now
    @connection = Mongo::Connection.new(opts[:host], opts[:port], :op_timeout => opts[:timeout], :slave_ok => true)
    @connect_time = Time.now - s
    server_status
  rescue => error
    printf "CRITICAL: #{error}"
    exit 2
  end

  def server_status
    @databases = connection.database_names
    @admin = connection.db('admin')
    cmd = BSON::OrderedHash.new

    # fetch server status info
    cmd['serverStatus'] = 1
    cmd['repl'] = 2
    @stats = admin.command(cmd, :check_response => false)

    if replica_set?
      # fetch replica set status
      cmd = BSON::OrderedHash.new
      cmd['replSetGetStatus'] = { 'members' => 1 }
      @rsstats = admin.command(cmd, :check_response => false)
    end
  end

  def replica_set_name
    stats['repl']['setName']
  end

  def replica_set?
    stats['repl'].key?('setName') ? true : false
  end

  def master?
    !replica_set? && stats['repl']['ismaster']
  end

  def slave?
    !replica_set? && !master?
  end

  def arbiter?
    replica_set? && stats['repl']['arbiterOnly']
  end

  def secondary?
    replica_set? && stats['repl']['secondary']
  end

  def primary?
    replica_set? && stats['repl']['ismaster']
  end

  def node_type
    if primary?
      'primary'
    elsif secondary?
      'secondary'
    elsif arbiter?
      'arbiter'
    elsif master?
      'master'
    elsif slave?
      'slave'
    else
      'unknown'
    end
  end

  def state_type(state)
    case state
    when 1
      'primary'
    when 2
      'secondary'
    when 7
      'arbiter'
    end
  end

  # check connectivity and report basic info
  def check_connect(_w = nil, _c = nil, i)
    message = "MongoDB #{node_type.capitalize} node, "
    message << "ReplicaSet: #{replica_set_name}, " if replica_set_name
    message << "Version: #{stats['version']}, "
    message << "Storage: #{stats['storageEngine']['name']}, "
    days, hrs, mins, secs = [24, 60, 60].reverse.each_with_object([stats['uptime'].to_i]) do |unitsize, result|
      result[0, 0] = result.shift.divmod(unitsize)
    end

    message << "DB Count: #{databases.count} "
    message << "Uptime: #{days}d #{hrs}h#{mins}m#{secs}s "
    assemble_message(message, nil, nil, nil, i)
  end

  # check replica set state
  def check_rs_cluster(w = nil, c = nil, i)
    if replica_set?
      primary = 0
      arbiter = 0
      secondary = 0
      rsstats['members'].each do |m|
        case m['state']
        when 1
          primary += 1
        when 7
          arbiter += 1
        when 2
          secondary += 1
        end
      end
      message = ''
      severity = ''
      if primary > 0 && secondary > 0 && arbiter > 0
        message = "Replicate Set OK: primary: #{primary}, secondary: #{secondary}, arbiter: #{arbiter}"
      else
        message = "Replicate Set Not OK: primary: #{primary}, secondary: #{secondary}, arbiter: #{arbiter}"
        severity = 'CRITICAL'
      end
      assemble_message(message, nil, nil, nil, nil, severity)

    else
      message = 'Node is not configured with Replica Set'
      assemble_message(message, value, w, c, i, severity)
    end
  end

  # check numbers of secondary nodes in a replica set
  def check_secondary_nodes(w = nil, c = nil, i)
    w ||= thresholds[:check_secondary_nodes][:warning]
    c ||= thresholds[:check_secondary_nodes][:critical]
    if replica_set?
      value = 0
      rsstats['members'].each { |m| value += 1 if m['state'] == 2 }
      message = "Replicate Set #{rsstats['set']} Secondary Node Count: #{value}"
      @exit_perfdata << "secondary_node=#{value};#{w};#{c} "
      assemble_message(message, value, w, c, true)
    else
      message = 'Node is not configured with Replica Set'
      assemble_message(message, nil, nil, nil, i)
    end
  end

  # check state of replica set members
  def check_node_state(w = nil, c = nil, i)
    # http://docs.mongodb.org/manual/reference/replica-states/
    # w ||= thresholds[:check_node_state][:warning]
    # c ||= thresholds[:check_node_state][:critical]
    if replica_set?
      severity = ''
      value = rsstats['myState']
      message = "Node State: #{value}"
      @exit_perfdata << "node_state=#{value};; "
      severity = 'CRITICAL' unless [1, 2, 7].include?(value)
      assemble_message(message, value, w, c, i, severity)
    else
      message = 'Node is not configured with Replica Set'
      assemble_message(message, value, w, c, i)
    end
  end

  # check total db size and also print
  # size of each db
  def check_total_db_size(w = nil, c = nil, i)
    w ||= thresholds[:check_total_db_size][:warning]
    c ||= thresholds[:check_total_db_size][:critical]
    cmd = BSON::OrderedHash.new
    cmd['listDatabases'] = 1
    result = admin.command(cmd, :check_response => false)
    total_size = (result['totalSize'].to_f / 1024.0 / 1024.0 / 1024.0).round(3)
    message = "Database Size: #{total_size}GB "
    result['databases'].each do |d|
      next if d['empty']
      value = (d['sizeOnDisk'].to_f / 1024.0 / 1024.0 / 1024.0).round(3)
      message << "#{d['name']}:#{value}G "
      @exit_perfdata << "#{d['name']}_size=#{value};; "
    end
    @exit_perfdata << "totalsize=#{total_size};#{w};#{c} "
    assemble_message(message, total_size, w, c, i)
  end

  # check connect time
  def check_connect_time(w = nil, c = nil, i)
    w ||= thresholds[:check_connect_time][:warning]
    c ||= thresholds[:check_connect_time][:critical]
    value = connect_time
    message = "Connect Time: #{value}"
    @exit_perfdata << "connect_time=#{value};#{w};#{c} "
    assemble_message(message, value, w, c, i)
  end

  # check queues size
  def check_queues(w = nil, c = nil, i)
    w ||= thresholds[:check_queues][:warning]
    c ||= thresholds[:check_queues][:critical]
    value = stats['globalLock']['currentQueue']['total']
    readers = stats['globalLock']['currentQueue']['readers']
    writers = stats['globalLock']['currentQueue']['writers']
    message = "Queue: total=#{value}, readers=#{readers}, writers=#{writers} "
    @exit_perfdata << "total_queues=#{value};#{w};#{c} "
    assemble_message(message, value, w, c, i)
  end

  # check total connections limit
  def check_total_connections(w = nil, c = nil, i = true)
    w ||= thresholds[:check_connections][:warning]
    c ||= thresholds[:check_connections][:critical]
    conns = stats['connections']
    value = conns['current'] + conns['available']
    message = "Total Connections: #{value}"
    assemble_message(message, value, w, c, i)
  end

  # check % connections used
  def check_connections(w = nil, c = nil, i)
    w ||= thresholds[:check_connections][:warning]
    c ||= thresholds[:check_connections][:critical]
    conns = stats['connections']
    current = conns['current']
    available = conns['available']
    total = current + available
    value = (current * 100 / total)
    message = "#{value} percent (#{current} of #{total}) connections used"

    # TODO: add warn and crit thresholds to perf data
    @exit_perfdata << "connections=#{current};; "
    assemble_message(message, value, w, c, i)
  end

  # check replication lag
  def check_sync_lag(w = nil, c = nil, i)
    w ||= thresholds[:check_sync_lag][:warning]
    c ||= thresholds[:check_sync_lag][:critical]
    if replica_set?
      self_state = nil
      primary_node = nil
      self_optimedate = nil
      primary_optimedate = nil
      replica_set = rsstats['set']

      rsstats['members'].each do |m|
        if m['self']
          self_optimedate = Time.parse m['optimeDate'].to_s if m.key?('optimeDate')
          self_state = m['state']
        end

        if m['state'] == 1
          primary_node = m['name']
          primary_optimedate = Time.parse m['optimeDate'].to_s
        end
      end

      case self_state
      when 2
        if !primary_node
          message = "Replica Set #{replica_set} missing Primary node"
          assemble_message(message, nil, nil, nil, i, 'CRITICAL')
        else
          value = primary_optimedate - self_optimedate
          message = "Replica Set #{replica_set} Secondary Replication is #{value}s behind from Primary #{primary_node}"
          @exit_perfdata << "sync_lag=#{value};#{w};#{c} "
          assemble_message(message, value, w, c, i)
        end
      when 1, 7
        message = "Replica Set #{replica_set} node #{state_type(self_state).capitalize} is not a Secondary node"
        assemble_message(message, nil, nil, nil, i)
      else
        message = "Replica Set #{replica_set} node state #{self_state} is unknown"
        assemble_message(message, nil, nil, nil, i, 'CRITICAL')
      end
    else
      if master?
        message = 'Master/Slave Replication Node is master'
        assemble_message(message, nil, nil, nil, i)
      elsif slave?
        syncto = stats['repl']['sources'][0]['host']
        value = stats['repl']['sources'][0]['lagSeconds'].to_i
        message = "Slave Replication is #{value}s behind from Master #{syncto}"
        @exit_perfdata << "sync_lag=#{value};#{w};#{c} "
        assemble_message(message, value, w, c, i)
      else
        message = 'Node is not configured with --replSet or --slave or --master'
        assemble_message(message, value, w, c, i)
      end
    end
  end

  # add check result message to the output
  def assemble_message(message, value, warning, critical, invert, severity = '')
    if critical && value && ((invert && value.to_f < critical.to_f) || (!invert && value.to_f >= critical.to_f))
      @critical_checks << "CRIT: #{message};"
    elsif warning && value && ((invert && value.to_f < warning.to_f) || (!invert && value.to_f >= warning.to_f))
      @warning_checks << "WARN: #{message};"
    elsif severity == 'CRITICAL'
      @critical_checks << "CRIT: #{message};"
    elsif severity == 'WARNING'
      @warning_checks << "WARN: #{message};"
    elsif message
      @ok_checks << "OK: #{message};"
    end
  end

  # collect different checks result
  def run
    opts[:checks].each do |check|
      name, w, c, i = check.split(':')
      i = i.to_i == 1 ? true : false
      send("check_#{name}", w, c, i)
    end

    exit_message = ''
    exit_message << critical_checks
    exit_message << warning_checks
    exit_message << ok_checks

    if !critical_checks.empty?
      exit_code = 2
    elsif critical_checks.empty? && !warning_checks.empty?
      exit_code = 1
    else
      exit_code = 0
    end

    exit_message << " | #{exit_perfdata}" unless exit_perfdata.empty?
    printf "#{exit_message}"
    exit exit_code
  end
end

CheckMongoDB.new(options).run
