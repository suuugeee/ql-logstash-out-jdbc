# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/plugin_mixins/ecs_compatibility_support"
require "java"

java_import "java.sql.DriverManager"
java_import "java.sql.Connection"
java_import "java.sql.PreparedStatement"
java_import "java.sql.SQLException"

class LogStash::Outputs::QlJdbc < LogStash::Outputs::Base
  include LogStash::PluginMixins::ECSCompatibilitySupport

  config_name "ql_jdbc"

  config :driver_jar_path, :validate => :string, :required => true
  config :driver_class, :validate => :string, :required => true
  config :connection_string, :validate => :string, :required => true
  config :username, :validate => :string, :required => true
  config :password, :validate => :string, :required => true
  config :statement, :validate => :array, :required => true
  config :max_pool_size, :validate => :number, :default => 5
  config :connection_timeout, :validate => :number, :default => 10000
  config :max_retries, :validate => :number, :default => 3
  config :retry_delay, :validate => :number, :default => 1000
  config :batch_size, :validate => :number, :default => 100
  config :flush_interval, :validate => :number, :default => 5

  public
  def register
    @logger.info("=== QL JDBC 插件开始注册 ===")
    @logger.info("插件版本: 1.0.0")
    @logger.info("配置参数:")
    @logger.info("  - driver_jar_path: #{@driver_jar_path}")
    @logger.info("  - driver_class: #{@driver_class}")
    @logger.info("  - connection_string: #{@connection_string}")
    @logger.info("  - username: #{@username}")
    @logger.info("  - batch_size: #{@batch_size}")
    @logger.info("  - max_pool_size: #{@max_pool_size}")
    
    # 检查JDBC驱动文件是否存在
    begin
      driver_file = java.io.File.new(@driver_jar_path)
      if driver_file.exists()
        @logger.info("JDBC驱动文件存在: #{@driver_jar_path}")
        @logger.info("文件大小: #{driver_file.length()} bytes")
      else
        @logger.error("JDBC驱动文件不存在: #{@driver_jar_path}")
        raise "JDBC驱动文件不存在"
      end
    rescue => e
      @logger.error("检查JDBC驱动文件时出错: #{e.message}")
      raise e
    end
    
    # 尝试加载JDBC驱动
    begin
      @logger.info("尝试加载JDBC驱动: #{@driver_class}")
      java.lang.Class.forName(@driver_class)
      @logger.info("JDBC驱动加载成功: #{@driver_class}")
    rescue => e
      @logger.error("JDBC驱动加载失败: #{e.message}")
      @logger.error("请确保JDBC驱动JAR文件已正确放置在classpath中")
      raise e
    end
    
    # 测试数据库连接
    begin
      @logger.info("测试数据库连接...")
      test_conn = DriverManager.getConnection(@connection_string, @username, @password)
      if test_conn.isValid(5)
        @logger.info("数据库连接测试成功")
        test_conn.close()
      else
        @logger.error("数据库连接测试失败")
        raise "数据库连接无效"
      end
    rescue => e
      @logger.error("数据库连接测试失败: #{e.message}")
      @logger.error("请检查数据库连接配置")
      raise e
    end
    
    @connection_pool = []
    @pool_mutex = Mutex.new
    @batch_buffer = []
    @batch_mutex = Mutex.new
    @last_flush_time = Time.now
    @prepared_statements = {}
    
    @logger.info("初始化连接池，大小: #{@max_pool_size}")
    @max_pool_size.times do |i|
      begin
        @logger.info("创建连接 #{i+1}/#{@max_pool_size}")
        conn = get_connection
        if conn
          @connection_pool << conn
          @logger.info("连接 #{i+1} 创建成功")
        else
          @logger.warn("连接 #{i+1} 创建失败")
        end
      rescue => e
        @logger.warn("创建连接 #{i+1} 时出错: #{e.message}")
      end
    end
    
    @logger.info("连接池初始化完成，实际连接数: #{@connection_pool.size}")
    
    # 启动定时刷新线程
    @logger.info("启动定时刷新线程，间隔: #{@flush_interval}秒")
    @flush_thread = Thread.new do
      loop do
        sleep @flush_interval
        @batch_mutex.synchronize do
          if @batch_buffer.size > 0 && (Time.now - @last_flush_time) >= @flush_interval
            @logger.info("定时刷新触发，缓冲区大小: #{@batch_buffer.size}")
            flush_batch_with_retry
            @last_flush_time = Time.now
          end
        end
      end
    end
    
    @logger.info("=== QL JDBC 插件注册完成 ===")
  end

  public
  def receive(event)
    start_time = Time.now
    @batch_mutex.synchronize do
      @batch_buffer << event
      current_batch_size = @batch_buffer.size
      @logger.info("Added event to batch buffer. Current batch size: #{current_batch_size}, Batch threshold: #{@batch_size}")
      
      if current_batch_size >= @batch_size
        @logger.info("Batch threshold reached. Starting flush process...")
        flush_batch_with_retry
      end
    end
    processing_time = ((Time.now - start_time) * 1000).round(2)
    @logger.info("Event processing completed in #{processing_time}ms")
  end

  public
  def close
    @logger.info("Closing QL JDBC output plugin")
    @batch_mutex.synchronize { flush_batch_with_retry if @batch_buffer.size > 0 }
    
    # 关闭预处理语句
    @prepared_statements.each { |key, stmt| stmt.close() if stmt rescue nil }
    @prepared_statements.clear
    
    @pool_mutex.synchronize do
      @connection_pool.each { |conn| conn.close() if conn && !conn.isClosed() rescue nil }
      @connection_pool.clear
    end
    # 停止定时刷新线程
    @flush_thread.exit if @flush_thread
  end

  private
  def get_connection
    @pool_mutex.synchronize do
      @connection_pool.each do |conn|
        return conn if conn && !conn.isClosed() && conn.isValid(5) rescue nil
      end
      
      retry_count = 0
      begin
        if @connection_pool.size < @max_pool_size
          conn = DriverManager.getConnection(@connection_string, @username, @password)
          conn.setAutoCommit(false)
          @connection_pool << conn
          @logger.info("Created new database connection")
          return conn
        else
          raise "Connection pool is full"
        end
      rescue => e
        retry_count += 1
        if retry_count <= @max_retries
          @logger.warn("Failed to create database connection (attempt #{retry_count}/#{@max_retries}): #{e.message}")
          sleep(@retry_delay / 1000.0)
          retry
        else
          @logger.error("Failed to create database connection after #{@max_retries} attempts: #{e.message}")
          raise e
        end
      end
    end
  end

  private
  def flush_batch_with_retry
    return if @batch_buffer.empty?
    
    retry_count = 0
    begin
      flush_batch
    rescue SQLException => e
      retry_count += 1
      if e.getMessage().include?("Deadlock") && retry_count <= @max_retries
        @logger.warn("Database deadlock detected (attempt #{retry_count}/#{@max_retries}), retrying...")
        sleep(@retry_delay / 1000.0)
        retry
      else
        @logger.error("Error executing batch: #{e.getMessage()}")
        raise e
      end
    rescue => e
      @logger.error("Unexpected error executing batch: #{e.message}")
      raise e
    end
  end

  private
  def flush_batch
    start_time = Time.now
    conn = get_connection
    return unless conn
    
    batch_size = @batch_buffer.size
    @logger.info("Starting batch flush. Batch size: #{batch_size}, Connection pool size: #{@connection_pool.size}")
    
    begin
      # 使用缓存的预处理语句
      stmt_key = conn.hash
      stmt = @prepared_statements[stmt_key]
      if stmt.nil?
        stmt_prepare_start = Time.now
        stmt = conn.prepareStatement(@statement.first)
        @prepared_statements[stmt_key] = stmt
        stmt_prepare_time = ((Time.now - stmt_prepare_start) * 1000).round(2)
        @logger.info("Prepared statement created in #{stmt_prepare_time}ms")
      else
        @logger.info("Using cached prepared statement")
      end
      
      # 记录参数设置开始时间
      param_setup_start = Time.now
      @batch_buffer.each_with_index do |event, event_index|
        @statement[1..-1].each_with_index do |param, index|
          value = event.get(param)
          if value.nil?
            stmt.setNull(index + 1, java.sql.Types.VARCHAR)
          else
            # 添加调试信息
            @logger.info("Processing field '#{param}' with value: '#{value}' (type: #{value.class})")
            
            # 处理日期字段转换 - 通过值格式判断，而不是字段名
            if value.is_a?(String) && value.match?(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$/)
              @logger.info("ISO8601 pattern matched for field '#{param}' with value '#{value}'")
              # 转换ISO8601格式为MySQL datetime格式
              begin
                # 解析ISO8601格式
                parsed_time = Time.parse(value)
                # 格式化为MySQL datetime格式 (yyyy-MM-dd HH:mm:ss)
                formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
                @logger.info("Converted ISO8601 date from '#{value}' to '#{formatted_time}' for field '#{param}'")
                stmt.setString(index + 1, formatted_time)
              rescue => e
                @logger.warn("Failed to parse ISO8601 date for field '#{param}': '#{value}', using original value: #{e.message}")
                stmt.setString(index + 1, value)
              end
            elsif value.is_a?(LogStash::Timestamp)
              @logger.info("LogStash::Timestamp detected for field '#{param}' with value '#{value}'")
              # 直接格式化LogStash::Timestamp为MySQL datetime格式
              begin
                formatted_time = value.strftime("%Y-%m-%d %H:%M:%S")
                @logger.info("Converted LogStash::Timestamp from '#{value}' to '#{formatted_time}' for field '#{param}'")
                stmt.setString(index + 1, formatted_time)
              rescue => e
                @logger.warn("Failed to format LogStash::Timestamp for field '#{param}': '#{value}', using original value: #{e.message}")
                stmt.setString(index + 1, value.to_s)
              end
            else
              @logger.info("No date conversion needed for field '#{param}' with value '#{value}'")
              # 避免不必要的字符串转换
              case value
              when String
                stmt.setString(index + 1, value)
              when Integer
                stmt.setLong(index + 1, value)
              when Float
                stmt.setDouble(index + 1, value)
              when java.sql.Timestamp
                stmt.setTimestamp(index + 1, value)
              else
                stmt.setString(index + 1, value.to_s)
              end
            end
          end
        end
        stmt.addBatch()
      end
      param_setup_time = ((Time.now - param_setup_start) * 1000).round(2)
      @logger.info("Parameter setup completed in #{param_setup_time}ms for #{batch_size} records")
      
      # 记录执行开始时间
      execute_start = Time.now
      stmt.executeBatch()
      execute_time = ((Time.now - execute_start) * 1000).round(2)
      @logger.info("Batch execution completed in #{execute_time}ms")
      
      # 记录提交开始时间
      commit_start = Time.now
      conn.commit()
      commit_time = ((Time.now - commit_start) * 1000).round(2)
      @logger.info("Transaction commit completed in #{commit_time}ms")
      
      total_time = ((Time.now - start_time) * 1000).round(2)
      records_per_second = (batch_size / (total_time / 1000.0)).round(2)
      @logger.info("Successfully inserted #{batch_size} records in #{total_time}ms (#{records_per_second} records/sec)")
      @batch_buffer.clear
    rescue => e
      conn.rollback()
      @logger.error("Batch flush failed after #{((Time.now - start_time) * 1000).round(2)}ms: #{e.message}")
      raise e
    end
  end
end 