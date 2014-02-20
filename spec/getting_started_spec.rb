require 'spec_helper'
require 'pry'
require 'lock_jar'

describe "Seattle Sample", :jruby => true do
  before(:all) do
    LockJar.load
    ['Peer',
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

      db = conn.db
      results.each do |result|
        entity = db.entity(result[0])
        puts entity.get(":community/name")
      end
      binding.pry

      puts "Getting communities' neighborhood names (there are duplicates because " +
        "multiple communities are in the same neighborhood..."

      db = conn.db
      results.each do |result|
        entity = db.entity(result[0])
        neighborhood = entity.get(":community/neighborhood");
        puts neighborhood.get(":neighborhood/name")
      end
      binding.pry

      puts "Getting names of all communities in first community's " +
        "neighborhood..."

      community = conn.db.entity(results.first[0])
      neighborhood = community.get(":community/neighborhood")
      communities = neighborhood.get(":community/_neighborhood")
      communities.each do |comm|
        comm.get(":community/name")
      end
      binding.pry

      puts "Find all communities and their names..."

      results = Peer.q("[:find ?c ?n :where [?c :community/name ?n]]", conn.db)
      results.each do |result|
        puts result[1]
      end
      binding.pry

      puts "Find all community names and urls..."

      results = Peer.q("[:find ?n ?u :where [?c :community/name ?n][?c :community/url ?u]]",
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts 'Find all categories for community named "belltown"...'

      results = Peer.q('[:find ?e ?c :where [?e :community/name "belltown"][?e :community/category ?c]]',
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find names of all communities that are twitter feeds..."

      results = Peer.q("[:find ?n :where [?c :community/name ?n][?c :community/type :community.type/twitter]]",
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find names of all communities that are in a neighborhood " +
        "in a district in the NE region..."

      results = Peer.q("[:find ?c_name :where " +
                       "[?c :community/name ?c_name]" +
                       "[?c :community/neighborhood ?n]" +
                       "[?n :neighborhood/district ?d]" +
                       "[?d :district/region :region/ne]]",
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find community names and region names for of all communities..."

      results = Peer.q("[:find ?c_name ?r_name :where " +
                       "[?c :community/name ?c_name]" +
                       "[?c :community/neighborhood ?n]" +
                       "[?n :neighborhood/district ?d]" +
                       "[?d :district/region ?r]" +
                       "[?r :db/ident ?r_name]]",
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find all communities that are twitter feeds and facebook pages using " +
        "the same query and passing in type as a parameter..."

      query_by_type =
        "[:find ?n :in $ ?t :where " +
        "[?c :community/name ?n]" +
        "[?c :community/type ?t]]"
      results = Peer.q(query_by_type,
                       conn.db,
                       ":community.type/twitter")
      results.each do |result|
        puts result
      end
      results = Peer.q(query_by_type,
                       conn.db,
                       ":community.type/facebook-page");
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find all communities that are twitter feeds or facebook pages using " +
        "one query and a list of individual parameters..."

      results = Peer.q("[:find ?n ?t :in $ [?t ...] :where " +
                       "[?c :community/name ?n]" +
                       "[?c :community/type ?t]]",
                       conn.db,
                       Util.list(":community.type/facebook-page",
                                 ":community.type/twitter"))
      results.each do |result|
        puts result
      end
      binding.pry
      
      puts "Find all communities that are non-commercial email-lists or commercial " +
        "web-sites using a list of tuple parameters..."

      results = Peer.q("[:find ?n ?t ?ot :in $ [[?t ?ot]] :where " +
                       "[?c :community/name ?n]" +
                       "[?c :community/type ?t]" +
                       "[?c :community/orgtype ?ot]]",
                       conn.db,
                       Util.list(Util.list(":community.type/email-list",
                                           ":community.orgtype/community"),
                                 Util.list(":community.type/website",
                                           ":community.orgtype/commercial")))
      results.each do |result|
        puts result
      end
      binding.pry
      
      puts 'Find all community names coming before "C" in alphabetical order...'

      results = Peer.q('[:find ?n :where ' +
                       '[?c :community/name ?n]' +
                       '[(.compareTo ?n "C") ?res]' +
                       '[(< ?res 0)]]',
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts 'Find all communities whose names include the string "Wallingford"...'

      results = Peer.q('[:find ?n :where ' +
                       '[(fulltext $ :community/name "Wallingford") [[?e ?n]]]]',
                       conn.db)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find all communities that are websites and that are about " +
        "food, passing in type and search string as parameters..."

      results = Peer.q("[:find ?name ?cat :in $ ?type ?search :where " +
                       "[?c :community/name ?name]" +
                       "[?c :community/type ?type]" +
                       "[(fulltext $ :community/category ?search) [[?c ?cat]]]]",
                       conn.db,
                       ":community.type/website",
                       "food")
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find all names of all communities that are twitter feeds, using rules..."

      rules = "[[[twitter ?c] [?c :community/type :community.type/twitter]]]"
      results = Peer.q("[:find ?n :in $ % :where " +
                       "[?c :community/name ?n]" +
                       "(twitter ?c)]",
                       conn.db,
                       rules);
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find names of all communities in NE and SW regions, using rules " +
        "to avoid repeating logic..."
      rules =
        "[[[region ?c ?r]" +
        "  [?c :community/neighborhood ?n]" +
        "  [?n :neighborhood/district ?d]" +
        "  [?d :district/region ?re]" +
        "  [?re :db/ident ?r]]]"
      results = Peer.q("[:find ?n :in $ % :where " +
                       "[?c :community/name ?n]" +
                       "(region ?c :region/ne)]",
                       conn.db,
                       rules)
      results.each do |result|
        puts result
      end
      results = Peer.q("[:find ?n :in $ % :where " +
                       "[?c :community/name ?n]" +
                       "(region ?c :region/sw)]",
                       conn.db,
                       rules)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find names of all communities that are in any of the southern " +
        "regions and are social-media, using rules for OR logic..."
      rules =
        "[[[region ?c ?r]" +
        "  [?c :community/neighborhood ?n]" +
        "  [?n :neighborhood/district ?d]" +
        "  [?d :district/region ?re]" +
        "  [?re :db/ident ?r]]" +
        " [[social-media ?c]" +
        "  [?c :community/type :community.type/twitter]]" +
        " [[social-media ?c]" +
        "  [?c :community/type :community.type/facebook-page]]" +
        " [[northern ?c] (region ?c :region/ne)]" +
        " [[northern ?c] (region ?c :region/n)]" +
        " [[northern ?c] (region ?c :region/nw)]" +
        " [[southern ?c] (region ?c :region/sw)]" +
        " [[southern ?c] (region ?c :region/s)]" +
        " [[southern ?c] (region ?c :region/se)]]"
      results = Peer.q("[:find ?n :in $ % :where " +
                             "[?c :community/name ?n]" +
                             "(southern ?c)" +
                             "(social-media ?c)]",
                             conn.db,
                             rules)
      results.each do |result|
        puts result
      end
      binding.pry

      puts "Find all database transactions..."

      results = Peer.q("[:find ?when :where [?tx :db/txInstant ?when]]",
                       conn.db)
      binding.pry

      puts "Sort transactions by time they occurred, then " +
        "pull out date when seed data load transaction and " +
        "schema load transactions were executed..."

      tx_dates = results.inject([]) do |memo, result|
        memo << result[0]
        memo
      end
      tx_dates.sort! {|x, y| y <=> x }
      data_tx_date = tx_dates[0]
      schema_tx_date = tx_dates[1]
      binding.pry

    rescue java.lang.Exception => e
      puts e.message
      puts e.backtrace.inspect
    end
  end
end

