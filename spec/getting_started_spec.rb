require 'spec_helper'
require 'pry'
require 'lock_jar'

describe "Seattle Sample", :jruby => true do
  before(:all) do
    LockJar.load
    ['Attribute',
     'Connection',
     'Database',
     'Datom',
     'Entity',
     'ListenableFuture',
     'Log',
     'Peer',
     'Util'].each do |name|
      java_import "datomic.#{name}"
    end
  end

  after(:all) do
    Peer.shutdown(true)
  end

  def read_string(str)
    read_string_fn = Java::ClojureLang::RT.var("clojure.core", "read-string")
    read_string_fn.invoke(str)
  end

  it "demonstrates datomic usages" do
    begin
      puts "Creating and connecting to database..."

      uri = "datomic:mem://seattle";
      Peer.createDatabase(uri);
      conn = Peer.connect(uri);

      binding.pry

      puts "Parsing schema edn file and running transaction..."

      schema = File.read(File.join(File.dirname(__FILE__), "edn", "seattle-schema.edn"))
      schema_tx = read_string(schema)
      tx_result = conn.transact(schema_tx).get
      puts tx_result

      binding.pry

      puts "Parsing seed data edn file and running transaction..."

      data_rdr = File.read(File.join(File.dirname(__FILE__), "edn", "seattle-data0.edn"))
      data_tx = read_string(data_rdr)
      tx_result = conn.transact(data_tx).get

      binding.pry

      puts "Finding all communities, counting results..."

      results = Peer.q("[:find ?c :where [?c :community/name]]", conn.db)
      puts results.size

      binding.pry

      puts "Getting first entity id in results, making entity map, displaying keys..."

      id = results.first[0]
      entity = conn.db.entity(id)
      puts entity.key_set

      binding.pry

      puts "Displaying the value of the entity's community name..."

      puts entity.get(":community/name")

      binding.pry

      puts "Getting name of each community (some may appear more than " +
        "because multiple online communities share the same name)..."

      db = conn.db();
      results.each do |result|
        entity = db.entity(result[0])
        puts entity.get(":community/name")
      end

      binding.pry
    rescue java.lang.Exception => e
      puts e.message
      puts e.backtrace.inspect
    end
  end
end

