# require 'spec_helper'
$LOAD_PATH.unshift(File.dirname(__FILE__))
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', '..', '..', 'lib'))

require 'rubygems' # Use the gems path only for the spec suite
require 'riak'
require 'rspec'
require 'rexml/document'

RSpec.configure do |c|
  # declare an exclusion filter
  @http_port ||= 10018 # test_server.http_port
  @client = Riak::Client.new(:http_port => @http_port)
  yz_active = @client.ping rescue false
  c.filter_run_excluding :yokozuna => !yz_active
end

describe "Yokozuna", :yokozuna => true do
  before(:all) do
    @pbc_port ||= 10017  # test_server.pb_port
    @http_port ||= 10018 # test_server.http_port
    @pb_client = Riak::Client.new(:http_port => @http_port, :pb_port => @pbc_port, :protocol => "pbc")
    @http_client = Riak::Client.new(:http_port => @http_port)
    @index = "test"
    @pb_client.http do |h|
      h.put([204, 409, 415], h.path("/yz/index/#{@index}"), "", {})
    end
    @bucket = @pb_client.bucket(@index)
    
    @object = @bucket.get_or_new("cat")
    @object.raw_data = '{"cat_s":"Lela"}'
    @object.content_type = 'application/json'
    @object.store

    @object2 = @bucket.get_or_new("dogs")
    @object2.raw_data = '{"dog_ss":["Einstein", "Olive"]}'
    @object2.content_type = 'application/json'
    @object2.store

    sleep 1.1
  end

  context "using the PB driver" do
    it "should search one row" do
      resp = @pb_client.search("test", "*:*", {:rows => 1})
      resp.should include('docs')
      resp['docs'].size.should == 1
    end

    it "should search with df" do
      resp = @pb_client.search("test", "Olive", {:rows => 1, :df => 'dog_ss'})
      resp.should include('docs')
      resp['docs'].size.should == 1
      resp['docs'].first['dog_ss']
    end
  end

  context "using the HTTP driver" do
    it "should search as XML" do
      resp = @http_client.search("test", "*:*", {:rows => 1, :wt => 'xml'})
      root = REXML::Document.new(resp).root
      root.name.should == 'response'
      doc = root.elements['result'].elements['doc']
      doc.should_not be_nil
      cats = doc.elements["str[@name='cat_s']"]
      cats.should have(1).item
      cats.text.should == 'Lela'
    end

    it "should search as JSON with df" do
      resp = @http_client.search("test", "Olive", {:rows => 1, :wt => 'json', :df => 'dog_ss'})
    end
  end
end