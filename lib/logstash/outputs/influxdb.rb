# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/json"
require "stud/buffer"

# This output lets you output Metrics to InfluxDB
#
# The configuration here attempts to be as friendly as possible
# and minimize the need for multiple definitions to write to
# multiple series and still be efficient
#
# the InfluxDB API let's you do some semblance of bulk operation
# per http call but each call is database-specific
#
# You can learn more at http://influxdb.com[InfluxDB homepage]
class LogStash::Outputs::InfluxDB < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "influxdb"

  # The database to write
  config :db, :validate => :string, :default => "stats"

  # URL of InfluxDB instance, eg. http://localhost:8086
  config :url, :validate => :string, :required => true

  # The user who has access to the named database
  config :user, :validate => :string, :default => nil, :required => true

  # The password for the user who access to the named database
  config :password, :validate => :password, :default => nil, :required => true

  # Set the level of precision of `time`
  #
  # only useful when overriding the time value
  config :time_precision, :validate => ["n", "u", "ms", "s", "m", "h"], :default => "s"

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same series
  config :flush_size, :validate => :number, :default => 100

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 10

  public
  def register
    require 'cgi'
    require 'net/http'
    @queue = []

    @query_params = "db=#{@db}&u=#{@user}&p=#{@password.value}&time_precision=#{@time_precision}"
    @base_url = "#{@url}/write"
    @uri = URI.parse("#{@base_url}?#{@query_params}")

    @http = Net::HTTP.new(@uri.hostname, @uri.port)

    buffer_initialize(
      :max_items => @flush_size,
      :max_interval => @idle_flush_time,
      :logger => @logger
    )
  end # def register

  public
  def receive(event)
    buffer_receive(event["message"])
  end # def receive

  def flush(events, teardown = false)
    post_data(events)
  end # def receive_bulk

  def post_data(body)
    @logger.debug("Post body: #{body}")
    request = Net::HTTP::Post.new(@uri.request_uri)
    request.body = body.join("\n")
    request["Content-Type"] = "text/plain"
    response = @http.request(request)

    # Consume the body for error checking
    # This will also free up the connection for reuse.
    response_body = response.body

    case response
    when Net::HTTPSuccess then
    else
      @logger.error("Error writing to InfluxDB",
                    :response => response,
                    :response_body => response_body,
                    :request_body => body.join("\n"))
    end
  end # def post

  def close
    buffer_flush(:final => true)
  end # def teardown
end # class LogStash::Outputs::InfluxDB
