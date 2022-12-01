require 'test_helper'

module SqlTracker
  class HandlerTest < Minitest::Test
    def setup
      reset_sql_tracker_options
    end
    
    def test_should_track_sql_command_in_the_list
      config = sample_config
      config.tracked_sql_command = %w(SELECT)
      config.tracked_paths = nil
      handler = SqlTracker::Handler.new(config)

      queries = [
        'SELECT * FROM users',
        'select id from products',
        'SELECT t.oid, t.typname FROM pg_type as t WHERE t.typname IN (test1, test2, test3)', 
      ]

      queries.each_with_index do |query, i|
        payload = { sql: query }
        handler.call('???', Time.now.to_f, Time.now.to_f, 1, payload)
        assert_equal(i + 1, handler.data.keys.count)
      end
    end

    def test_should_not_track_sql_command_not_in_the_list
      config = sample_config
      config.tracked_sql_command = %w(INSERT)
      config.tracked_paths = nil
      handler = SqlTracker::Handler.new(config)

      queries = [
        'SELECT * FROM users',
        'select id from products',
      ]

      queries.each do |query|
        payload = { sql: query }
        handler.call('???', Time.now.to_f, Time.now.to_f, 1, payload)
        assert_equal(0, handler.data.keys.count)
      end
    end

    def test_should_concatenate_in_parameters
      query = %{
        SELECT * FROM a
        WHERE a.id IN (
          SELECT b.id FROM b WHERE b.id IN (1,2,3,4)
          AND b.uid IN ('aaaa', 'bbbb')
        ) AND a.xid IN (11, 22, 33)
      }
      expected = %{
        SELECT * FROM a
        WHERE a.id IN (
          SELECT b.id FROM b WHERE b.id IN (???)
          AND b.uid IN (???)
        ) AND a.xid IN (???)
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_query_count_should_be_case_insensitive
      config = sample_config
      config.tracked_sql_command = %w(INSERT SELECT)

      handler = SqlTracker::Handler.new(config)

      queries = [
        'SELECT * FROM users',
        'select * from Users'
      ]

      queries.each do |query|
        payload = { sql: query }
        handler.call('???', Time.now.to_f, Time.now.to_f, 1, payload)
      end

      assert_equal(1, handler.data.keys.count)
      assert_equal(2, handler.data[handler.data.keys.first][:count])
    end

    def test_clean_values_from_where_in_clause
      query = %{
        SELECT * FROM a
        WHERE a.id IN (
          SELECT b.id FROM b WHERE b.id IN (1,2,3,4)
          AND b.uid IN ('aaaa', 'bbbb')
        ) AND a.xid IN (11, 22, 33)
      }
      expected = %{
        SELECT * FROM a
        WHERE a.id IN (
          SELECT b.id FROM b WHERE b.id IN (???)
          AND b.uid IN (???)
        ) AND a.xid IN (???)
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_clean_values_from_comparison_operators
      query = %{
        SELECT * FROM a
        WHERE a.id = 1 AND a.uid != 'bbb'
        (a.num > 1 AND a.num < 3) AND
        (start_date >= '2010-01-01' AND end_date <= '2010-10-01') AND
        a.total BETWEEN 0 AND 100 LIMIT 25 OFFSET 0
      }
      expected = %{
        SELECT * FROM a
        WHERE a.id = ??? AND a.uid != ???
        (a.num > ??? AND a.num < ???) AND
        (start_date >= ??? AND end_date <= ???) AND
        a.total BETWEEN ??? AND ??? LIMIT ??? OFFSET ???
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_clean_floating_numbers
      query = %{
        SELECT * FROM a
        WHERE (a.lat BETWEEN 12.4567 AND 38.0678) AND
        (a.lng BETWEEN -70.487 AND -87.790)
      }
      expected = %{
        SELECT * FROM a
        WHERE (a.lat BETWEEN ??? AND ???) AND
        (a.lng BETWEEN ??? AND ???)
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_clean_sql_query_is_case_insensitive
      query = %{
        SELECT * FROM a
        where a.id = 1 AND a.uid != 'bbb'
        (a.num > 1 AND a.num < 3) AND
        (start_date >= '2010-01-01' AND end_date <= '2010-10-01') AND
        a.total between 0 and 100
      }
      expected = %{
        SELECT * FROM a
        where a.id = ??? AND a.uid != ???
        (a.num > ??? AND a.num < ???) AND
        (start_date >= ??? AND end_date <= ???) AND
        a.total between ??? and ???
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_clean_values
      query = %{
        INSERT INTO users VALUES
        (nextval('id_seq'), 'a', 105, DEFAULT),
        (nextval('id_seq'), 'b', 9100, DEFAULT);
      }
      expected = %{
        INSERT INTO users VALUES (nextval(???), ???, ???, DEFAULT), (nextval(???), ???, ???, DEFAULT);
      }.squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    def test_clean_pattern_matching
      query = %(
        SELECT users.* FROM users
        WHERE users.name LIKE '%test%' AND NOT SIMILAR TO '%test ppp'
      )
      expected = %(
        SELECT users.* FROM users
        WHERE users.name LIKE ??? AND NOT SIMILAR TO ???
      ).squish

      handler = SqlTracker::Handler.new(nil)
      cleaned_query = handler.clean_sql_query(query)
      assert_equal(expected, cleaned_query)
    end

    private

    def sample_config
      config = SqlTracker::Config.apply_defaults.new
      config.enabled = true
      config
    end
  end
end
