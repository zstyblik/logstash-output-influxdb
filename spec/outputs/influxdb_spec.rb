require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/influxdb"

describe LogStash::Outputs::InfluxDB do

  let(:pipeline) { LogStash::Pipeline.new(config) }

  context "complete pipeline run with 2 events" do

    let(:config) do
      {
        "host" => "localhost",
        "user" => "someuser",
        "password" => "somepwd",
      }
    end

    subject { LogStash::Outputs::InfluxDB.new(config) }

    before do
      subject.register
      allow(subject).to receive(:post).with(result)

      2.times do
        subject.receive(LogStash::Event.new("message" => "foo bar", "time" => "3", "type" => "generator"))
      end

      # Close / flush the buffer
      subject.close
    end

    let(:result) { "foo bar" }

    it "should receive 2 events, flush and call post with 2 items json array" do
      expect(subject).to have_received(:post).with(result)
    end

  end
end
