require 'spec_helper'
require 'pry'
require 'lock_jar'

describe "Seattle Sample", :jruby => true do
  def read_string(str)
    read_string_fn = Java::ClojureLang::RT.var("clojure.core", "read-string")
    read_string_fn.invoke(str)
  end

  def with_rescue
    begin
      yield if block_given?
    rescue java.lang.Exception => e
      puts e.message
    end
  end

  before(:all) do
    LockJar.load
    ['Peer',
     'Connection',
     'Util'].each do |name|
      java_import "datomic.#{name}"
    end

    with_rescue do
      puts "\nCreating and connecting to database..."
      uri = "datomic:mem://seattle";
      Peer.createDatabase(uri);
      @conn = Peer.connect(uri);
      binding.pry
    end
  end

  after(:all) do
    Peer.shutdown(true)
  end

  context "with connection" do
    before :all do
      with_rescue do
        puts "\nParsing schema edn file and running transaction..."
        schema = File.read(File.join(File.dirname(__FILE__), "edn", "seattle-schema.edn"))
        schema_tx = read_string(schema)
        tx_result = @conn.transact(schema_tx).get
        puts tx_result
        binding.pry

        puts "\nParsing seed data edn file and running transaction..."
        data_rdr = File.read(File.join(File.dirname(__FILE__), "edn", "seattle-data0.edn"))
        data_tx = read_string(data_rdr)
        tx_result = @conn.transact(data_tx).get
        binding.pry
      end
    end

    describe "with common results" do
      before(:all) do
        with_rescue do
          puts "\nFinding all communities, counting results..."
          @results = Peer.q("[:find ?c :where [?c :community/name]]", @conn.db)
          puts @results.size
          binding.pry
        end
      end

      it "demonstrates sample 1" do
        with_rescue do
          puts "\nGetting first entity id in results, making entity map, displaying keys..."
          id = @results.first[0]
          entity = @conn.db.entity(id)
          puts entity.key_set
          binding.pry

          puts "\nDisplaying the value of the entity's community name..."
          puts entity.get(":community/name")
          binding.pry
        end
      end

      it "demonstrates sample 2" do
        with_rescue do
          puts "\nGetting name of each community (some may appear more than " +
            "because multiple online communities share the same name)..."
          db = @conn.db
          @results.each do |result|
            entity = db.entity(result[0])
            puts entity.get(":community/name")
          end
          binding.pry
        end
      end

      it "demonstrates sample 3" do
        with_rescue do
          puts "\nGetting communities' neighborhood names (there are duplicates because " +
            "multiple communities are in the same neighborhood..."
          db = @conn.db
          @results.each do |result|
            entity = db.entity(result[0])
            neighborhood = entity.get(":community/neighborhood");
            puts neighborhood.get(":neighborhood/name")
          end
          binding.pry
        end
      end

      it "demonstrates sample 4" do
        with_rescue do
          puts "\nGetting names of all communities in first community's " +
            "neighborhood..."
          community = @conn.db.entity(@results.first[0])
          neighborhood = community.get(":community/neighborhood")
          communities = neighborhood.get(":community/_neighborhood")
          communities.each do |comm|
            comm.get(":community/name")
          end
          binding.pry
        end
      end
    end

    describe "with each results" do
      it "demonstrates sample 5" do
        with_rescue do
          puts "\nFind all communities and their names..."
          results = Peer.q("[:find ?c ?n :where [?c :community/name ?n]]", @conn.db)
          results.each do |result|
            puts result[1]
          end
          binding.pry
        end
      end

      it "demonstrates sample 6" do
        with_rescue do
          puts "\nFind all community names and urls..."
          results = Peer.q("[:find ?n ?u :where "+
                           "[?c :community/name ?n]"+
                           "[?c :community/url ?u]]",
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 7" do
        with_rescue do
          puts '\nFind all categories for community named "belltown"...'

          results = Peer.q('[:find ?e ?c :where '+
                           '[?e :community/name "belltown"]'+
                           '[?e :community/category ?c]]',
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 8" do
        with_rescue do
          puts "\nFind names of all communities that are twitter feeds..."
          results = Peer.q("[:find ?n :where "+
                           "[?c :community/name ?n]"+
                           "[?c :community/type :community.type/twitter]]",
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 9" do
        with_rescue do
          puts "\nFind names of all communities that are in a neighborhood " +
            "in a district in the NE region..."
          results = Peer.q("[:find ?c_name :where " +
                           "[?c :community/name ?c_name]" +
                           "[?c :community/neighborhood ?n]" +
                           "[?n :neighborhood/district ?d]" +
                           "[?d :district/region :region/ne]]",
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 10" do
        with_rescue do
          puts "\nFind community names and region names for of all communities..."
          results = Peer.q("[:find ?c_name ?r_name :where " +
                           "[?c :community/name ?c_name]" +
                           "[?c :community/neighborhood ?n]" +
                           "[?n :neighborhood/district ?d]" +
                           "[?d :district/region ?r]" +
                           "[?r :db/ident ?r_name]]",
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 11" do
        with_rescue do
          puts "\nFind all communities that are twitter feeds and facebook pages using " +
            "the same query and passing in type as a parameter..."
          query_by_type =
            "[:find ?n :in $ ?t :where " +
            "[?c :community/name ?n]" +
            "[?c :community/type ?t]]"
          results = Peer.q(query_by_type,
                           @conn.db,
                           ":community.type/twitter")
          results.each do |result|
            puts result
          end
          binding.pry

          results = Peer.q(query_by_type,
                           @conn.db,
                           ":community.type/facebook-page");
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 12" do
        with_rescue do
          puts "\nFind all communities that are twitter feeds or facebook pages using " +
            "one query and a list of individual parameters..."
          results = Peer.q("[:find ?n ?t :in $ [?t ...] :where " +
                           "[?c :community/name ?n]" +
                           "[?c :community/type ?t]]",
                           @conn.db,
                           Util.list(":community.type/facebook-page",
                                     ":community.type/twitter"))
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 13" do
        with_rescue do
          puts "\nFind all communities that are non-commercial email-lists or commercial " +
            "web-sites using a list of tuple parameters..."
          results = Peer.q("[:find ?n ?t ?ot :in $ [[?t ?ot]] :where " +
                           "[?c :community/name ?n]" +
                           "[?c :community/type ?t]" +
                           "[?c :community/orgtype ?ot]]",
                           @conn.db,
                           Util.list(Util.list(":community.type/email-list",
                                               ":community.orgtype/community"),
                                     Util.list(":community.type/website",
                                               ":community.orgtype/commercial")))
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 14" do
        with_rescue do
          puts '\nFind all community names coming before "C" in alphabetical order...'
          results = Peer.q('[:find ?n :where ' +
                           '[?c :community/name ?n]' +
                           '[(.compareTo ?n "C") ?res]' +
                           '[(< ?res 0)]]',
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 15" do
        with_rescue do
          puts '\nFind all communities whose names include the string "Wallingford"...'
          results = Peer.q('[:find ?n :where ' +
                           '[(fulltext $ :community/name "Wallingford") [[?e ?n]]]]',
                           @conn.db)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 16" do
        with_rescue do
          puts "\nFind all communities that are websites and that are about " +
            "food, passing in type and search string as parameters..."
          results = Peer.q("[:find ?name ?cat :in $ ?type ?search :where " +
                           "[?c :community/name ?name]" +
                           "[?c :community/type ?type]" +
                           "[(fulltext $ :community/category ?search) [[?c ?cat]]]]",
                           @conn.db,
                           ":community.type/website",
                           "food")
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 17" do
        with_rescue do
          puts "\nFind all names of all communities that are twitter feeds, using rules..."
          rules = "[[[twitter ?c] [?c :community/type :community.type/twitter]]]"
          results = Peer.q("[:find ?n :in $ % :where " +
                           "[?c :community/name ?n]" +
                           "(twitter ?c)]",
                           @conn.db,
                           rules);
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 18" do
        with_rescue do
          puts "\nFind names of all communities in NE and SW regions, using rules " +
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
                           @conn.db,
                           rules)
          results.each do |result|
            puts result
          end
          binding.pry

          results = Peer.q("[:find ?n :in $ % :where " +
                           "[?c :community/name ?n]" +
                           "(region ?c :region/sw)]",
                           @conn.db,
                           rules)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end

      it "demonstrates sample 19" do
        with_rescue do
          puts "\nFind names of all communities that are in any of the southern " +
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
                           @conn.db,
                           rules)
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end
    end

    describe "with transaction data" do
      before :all do
        with_rescue do
          puts "\nFind all database transactions..."
          results = Peer.q("[:find ?when :where [?tx :db/txInstant ?when]]",
                           @conn.db)
          binding.pry

          puts "\nSort transactions by time they occurred, then " +
            "pull out date when seed data load transaction and " +
            "schema load transactions were executed..."

          tx_dates = results.inject([]) do |memo, result|
            memo << result[0]
            memo
          end
          tx_dates.sort! {|x, y| y <=> x }
          @data_tx_date = tx_dates[0]
          @schema_tx_date = tx_dates[1]
          binding.pry
        end
      end

      it "demonstrates sample 20" do
        with_rescue do
          puts "\nMake query to find all communities, use with database " +
            "values as of and since different points in time..."

          puts "\nFind all communities as of schema transaction..."
          db_asOf_schema = @conn.db.asOf(@schema_tx_date)
          results = Peer.q("[:find ?c :where [?c :community/name]]", db_asOf_schema)
          puts results.size
          binding.pry
        end
      end

      it "demonstrates sample 21" do
        with_rescue do
          puts "\nFind all communities as of seed data transaction..."
          db_asOf_data = @conn.db.asOf(@data_tx_date)
          results = Peer.q("[:find ?c :where [?c :community/name]]", db_asOf_data)
          puts results.size
          binding.pry
        end
      end

      it "demonstrates sample 22" do
        with_rescue do
          puts "\nFind all communities since schema transaction..."
          db_since_schema = @conn.db.since(@schema_tx_date)
          results = Peer.q("[:find ?c :where [?c :community/name]]", db_since_schema)
          puts results.size
          binding.pry
        end
      end

      it "demonstrates sample 23" do
        with_rescue do
          puts "\nFind all communities since seed data transaction..."
          db_since_data = @conn.db.since(@data_tx_date);
          results = Peer.q("[:find ?c :where [?c :community/name]]", db_since_data)
          puts results.size
          binding.pry
        end
      end
    end

    describe "with new data" do
      it "demonstrates sample 24" do
        with_rescue do
          puts "\nMake a new partition..."
          partition_tx = Util.list(Util.map("db/id", Peer.tempid(":db.part/db"),
                                            "db/ident", ":communities",
                                            "db.install/_partition", "db.part/db"))
          txResult = @conn.transact(partition_tx).get
          puts txResult
          binding.pry
        end
      end

      it "demonstrates sample 25" do
        with_rescue do
          puts "\nMake a new community..."
          add_community_tx = Util.list(Util.map(":db/id", Peer.tempid(":communities"),
                                                ":community/name", "Easton"))
          txResult = @conn.transact(add_community_tx).get
          puts txResult
          binding.pry
        end
      end

      it "demonstrates sample 26" do
        with_rescue do
          @conn.transact(Util.list(Util.map(":db/id", Peer.tempid(":communities"),
                                            ":community/name", "Easton"))).get

          puts "\nUpdate data for a community..."
          results = Peer.q("[:find ?id :where [?id :community/name \"belltown\"]]",
                           @conn.db)
          belltown_id = results.first[0]
          update_category_tx = Util.list(Util.map(":db/id", belltown_id,
                                                  ":community/category", "free stuff"))
          txResult = @conn.transact(update_category_tx).get
          puts txResult
          binding.pry

          puts "\nRetract data for a community..."
          retract_category_tx = Util.list(Util.list(":db/retract", belltown_id,
                                                    ":community/category", "free stuff"))
          txResult = @conn.transact(retract_category_tx).get
          puts txResult
          binding.pry

          puts "\nRetract a community entity..."
          results = Peer.q("[:find ?id :where [?id :community/name \"Easton\"]]",
                           @conn.db)
          easton_id = results.first[0]
          retract_entity_tx = Util.list(Util.list(":db.fn/retractEntity", easton_id))
          txResult = @conn.transact(retract_category_tx).get
          puts txResult
          binding.pry
        end
      end

      it "demonstrates sample 27" do
        with_rescue do
          puts "\nGet transaction report queue, add new community again..."
          queue = @conn.txReportQueue
          add_community_tx = Util.list(Util.map(":db/id", Peer.tempid(":communities"),
                                                ":community/name", "Easton"))
          txResult = @conn.transact(add_community_tx).get
          puts txResult
          binding.pry

          puts "\nPoll queue for transaction notification, print data that was added..."
          report = queue.poll
          results = Peer.q("[:find ?e ?aname ?v ?added " +
                           ":in $ [[?e ?a ?v _ ?added]] " +
                           ":where " +
                           "[?e ?a ?v _ ?added]" +
                           "[?a :db/ident ?aname]]",
                           report.get(Connection.DB_AFTER),
                           report.get(Connection.TX_DATA))
          results.each do |result|
            puts result
          end
          binding.pry
        end
      end
    end
  end
end
