#!/usr/bin/env ruby
require "rubygems"
require "active_support/inflector"
require "snmp"
require 'graphite-api'
require 'graphite-api/core_ext/numeric'
require 'rufus-scheduler'
require 'logger'
require 'optparse'
require 'pp'

@log = Logger.new(STDOUT)

NODE_STATE = [ :undefined, :alive, :dead, :unknown, :draining ]
POOL_STATE = [ :undefined, :active, :disabled, :draining, :unused ]

def query snmp_manager, system_name, identifiers, metric_name_prefix, metric_names, metric_name_function
  graphite_metrics = {}
      
  snmp_manager.bulk_walk(identifiers+metric_names) do |row|
    identifiers = row.take(identifiers.count).map {|x| x.value.to_s  }
    metric_prefix = metric_name_prefix.call identifiers
    metrics = Hash[row.drop(identifiers.count).map.with_index {|x,i| [metric_name_function.call(metric_names[i]), x.value.to_i] }]
    graphite_metrics.merge! Hash[metrics.map {|k,v| ["#{metric_prefix}.#{k}",v]}]
  end
  graphite_metrics
end

def query_pools snmp_manager, system_name
  node_identifiers = [ "perPoolNodePoolName", "perPoolNodeNodeHostName", "perPoolNodeNodePort" ]    
    
  metric_name_prefix = lambda do |identifiers| 
    [ "zxtm.#{system_name}.pools.",
      "#{identifiers[0].tr(' ','_').underscore}.",
      "nodes.",
      "#{identifiers[1].underscore.tr('.','_')}_#{identifiers[2]}" ].join 
  end
    
  metric_name_lambda = lambda do |metric_name|
    metric_name.sub('perPoolNode','').tr(' ','_').underscore
  end              

  node_metrics = [
    "perPoolNodeState",
    "perPoolNodeCurrentRequests",
    "perPoolNodeTotalConn",
    "perPoolNodePooledConn",
    "perPoolNodeFailures",
    "perPoolNodeNewConn",
    "perPoolNodeErrors",
    "perPoolNodeResponseMean",
    "perPoolNodeResponseMax",
    "perPoolNodeResponseMin",
    "perPoolNodeIdleConns",
    "perPoolNodeCurrentConn",
    "perPoolNodeBytesFromNode",
    "perPoolNodeBytesToNode"]

  query snmp_manager, system_name, node_identifiers, metric_name_prefix, node_metrics, metric_name_lambda   
end

def query_virtualservers snmp_manager, system_name
  node_identifiers = [ "virtualserverName" ]    
    
  metric_name_prefix = lambda do |identifiers| 
    "zxtm.#{system_name}.virtualservers.#{identifiers[0]}"
  end
    
  metric_name_lambda = lambda do |metric_name|
    metric_name.sub('virtualserver','').tr(' ','_').underscore
  end              

  node_metrics = [
    "virtualserverCurrentConn",
    "virtualserverMaxConn",
    "virtualserverTotalConn",
    "virtualserverDiscard",
    "virtualserverDirectReplies",
    "virtualserverConnectTimedOut",
    "virtualserverDataTimedOut",
    "virtualserverKeepaliveTimedOut",
    "virtualserverUdpTimedOut",
    "virtualserverTotalDgram",
    "virtualserverGzip",
    "virtualserverHttpRewriteLocation",
    "virtualserverHttpRewriteCookie",
    "virtualserverConnectionErrors",
    "virtualserverConnectionFailures",
    "virtualserverBytesIn",
    "virtualserverBytesOut",
    "virtualserverGzipBytesSaved",
    "virtualserverCertStatusRequests"
  ]

  query snmp_manager, system_name, node_identifiers, metric_name_prefix, node_metrics, metric_name_lambda   
end

options = {}
options[:verbose] = false
options[:port] = 2003
options[:interval] = 10 
options[:graphite_interval] = 0

optparse=OptionParser.new do |opts|
  opts.banner = "Usage: zxtm-graphite -G <graphite-server> [options] <zxtm> [<zxtm>...]"
  
  opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
    options[:verbose] = v
  end
  
  opts.on("-d", "--[no-]debug", "Enable debug") do |d|
    options[:debug] = d
  end
  
  opts.on("-G", "--graphite-server SERVER") do |server|
    options[:graphite_host] = server
  end
  
  opts.on("-P", "--graphite-port PORT") do |port|
    options[:graphite_port] = port.to_i
  end
  
  opts.on("-t", "--graphite-interval SECONDS") do |interval|
    options[:graphite_interval] = interval.to_i
  end
  
  opts.on("-i", "--interval INTERVAL") do |interval|
    options[:interval] = interval.to_i
  end
end

optparse.parse!
raise OptionParser::MissingArgument if options[:graphite_host].nil?

if options[:debug]
  @log.level = Logger::DEBUG
elsif options[:verbose]
  @log.level = Logger::INFO
else
  @log.level = Logger::WARN
end

snmp_managers = ARGV.map do |host| 
  @log.debug "Creating SNMP::Manager for #{host}"
  SNMP::Manager.new(host: host,  mib_dir: 'mib', mib_modules: ['ZXTM-MIB-SMIv2'])
end  

client = GraphiteAPI.new(graphite: "#{options[:graphite_host]}:#{options[:graphite_port]}", interval: options[:graphite_interval])

scheduler=Rufus::Scheduler.new overlap: false

snmp_managers.each do |snmp_manager|
  host=snmp_manager.config[:host]
  
  begin
    reported_host_name = snmp_manager.get_value('1.3.6.1.2.1.1.5.0').underscore
  rescue SNMP::RequestTimeout => e
    @log.fatal "#{e} while requesting system name for #{host}"
    abort
  end
    
  @log.info "Scheduling collection job every #{options[:interval]} seconds for #{host} as #{reported_host_name}"
  scheduler.every "#{options[:interval]}s" do
    @log.info "Scheduled collection job running for #{reported_host_name}"
    @log.info "Collecting metrics for #{reported_host_name}"
    begin 
      @log.info "Collecting Pool metrics for #{reported_host_name}"
      pool_metrics = query_pools snmp_manager, reported_host_name
      @log.debug "Pool metrics collected: #{pool_metrics}"
      @log.info "Collecting Virtualserver metrics for #{reported_host_name}"
      virtualserver_metrics = query_virtualservers snmp_manager, reported_host_name
      @log.debug "Virtualserver metrics collected: #{virtualserver_metrics}"
      @log.debug "Uploading metrics to #{options[:graphite_host]} for #{reported_host_name}"
      client.metrics pool_metrics.merge virtualserver_metrics
      @log.info "Uploaded metrics to #{options[:graphite_host]} for #{reported_host_name}"
    rescue SNMP::RequestTimeout => e
      @log.error "#{e} while requesting metrics for #{host}"
    end
  end
end  

@log.debug "Joining scheduler to main task"
scheduler.join