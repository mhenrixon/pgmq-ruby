# frozen_string_literal: true

# Example: Shared Connection Thread Safety
#
# Demonstrates that PGMQ now detects when a callable returns the same
# PG::Connection object to multiple pool slots and raises ConfigurationError
# immediately, preventing the thread-safety corruption that previously caused
# NoMethodError on ntuples.
#
# Previously, sharing a single PG::Connection across pool slots caused:
#   NoMethodError: undefined method 'ntuples' for nil
#   pgmq-ruby/lib/pgmq/client/message_lifecycle.rb:165:in 'archive'
#
# Now, the duplicate connection is detected at pool creation time and a
# descriptive ConfigurationError is raised.
#
# Run: bundle exec ruby spec/integration/shared_connection_thread_safety_spec.rb

require_relative "support/example_helper"

THREAD_COUNT = 5
ITERATIONS = 30
# Timeout per thread to avoid hanging on fully corrupted connections
THREAD_TIMEOUT = 10

ExampleHelper.run_example("Shared Connection Thread Safety") do |_client, queues, interrupted|
  queue = ExampleHelper.unique_queue_name("threadsafe")
  queues << queue

  # --- Test 1: shared connection (the unsafe pattern) should raise ConfigurationError ---
  raw_conn = PG.connect(ExampleHelper::DB_PARAMS)

  puts "Testing unsafe pattern: callable returning same PG::Connection to #{THREAD_COUNT} pool slots"
  puts "Expected: ConfigurationError raised when duplicate connection detected"
  puts

  config_errors = 0
  other_errors = 0
  ok_results = 0
  mutex = Mutex.new

  unsafe_client = PGMQ::Client.new(-> { raw_conn }, pool_size: THREAD_COUNT)

  # The first pool slot is created lazily. Force multiple slots to be created
  # by using the pool concurrently from multiple threads.
  threads = THREAD_COUNT.times.map do |i|
    break [] if interrupted.call

    Thread.new do
      ITERATIONS.times do |j|
        break if interrupted.call

        begin
          unsafe_client.list_queues
          mutex.synchronize { ok_results += 1 }
        rescue PGMQ::Errors::ConfigurationError => e
          mutex.synchronize do
            config_errors += 1
            puts "  [thread=#{i} iter=#{j}] DETECTED: #{e.class}: #{e.message[0..80]}..."
          end
        rescue => e
          mutex.synchronize { other_errors += 1 }
        end
      end
    end
  end

  threads.each do |t|
    unless t.join(THREAD_TIMEOUT)
      t.kill
      mutex.synchronize { other_errors += 1 }
    end
  end

  total_unsafe = ok_results + config_errors + other_errors
  puts
  puts "  Results: ok=#{ok_results}, config_errors=#{config_errors}, other_errors=#{other_errors}"

  if config_errors.positive?
    puts "  CONFIRMED: Shared connection detected and ConfigurationError raised"
  elsif other_errors.positive?
    puts "  PARTIAL: Some errors occurred but not ConfigurationError"
  else
    puts "  Note: All operations completed on single pool slot (race condition not triggered)"
  end

  raw_conn.close rescue nil

  break if interrupted.call

  # --- Test 2: safe pattern — each pool slot gets its own connection ---
  puts
  puts "Testing safe pattern: connection string (each pool slot gets its own PG::Connection)"
  puts "Running #{THREAD_COUNT} threads x #{ITERATIONS} iterations..."

  safe_client = ExampleHelper.create_client(pool_size: THREAD_COUNT)
  safe_client.create(queue)

  safe_config_errors = 0
  safe_other_errors = 0
  safe_ok = 0

  threads = THREAD_COUNT.times.map do |i|
    break [] if interrupted.call

    Thread.new do
      ITERATIONS.times do
        break if interrupted.call

        begin
          safe_client.archive(queue, 99_999)
          mutex.synchronize { safe_ok += 1 }
        rescue PGMQ::Errors::ConfigurationError => e
          mutex.synchronize { safe_config_errors += 1 }
        rescue => e
          mutex.synchronize { safe_other_errors += 1 }
        end
      end
    end
  end
  threads.each { |t| t.join(THREAD_TIMEOUT) || t.kill }

  total_safe = safe_ok + safe_config_errors + safe_other_errors
  puts "  Results: ok=#{safe_ok}, config_errors=#{safe_config_errors}, other_errors=#{safe_other_errors}"
  puts "  Failure rate: #{total_safe.zero? ? 0 : ((safe_config_errors + safe_other_errors) * 100.0 / total_safe).round(1)}%"

  if safe_config_errors.zero? && safe_other_errors.zero?
    puts "  CONFIRMED: Separate connections per pool slot is thread-safe"
  else
    puts "  UNEXPECTED: errors even with separate connections"
  end

  safe_client.close
end
