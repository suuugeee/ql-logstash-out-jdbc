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
  config :max_pool_size, :validate => :number, :default => 10
  config :connection_timeout, :validate => :number, :default => 10000
  config :max_retries, :validate => :number, :default => 3
  config :retry_delay, :validate => :number, :default => 1000
  config :batch_size, :validate => :number, :default => 100
  config :flush_interval, :validate => :number, :default => 5
  config :output_delay, :validate => :number, :default => 0, :description => "每次输出后的延迟时间（毫秒），用于控制输出速度"
  config :verbose_logging, :validate => :boolean, :default => false, :description => "是否启用详细日志输出"
  config :pool_monitor_interval, :validate => :number, :default => 60, :description => "连接池监控间隔（秒）"

  public
  def register
    @logger.info("=== QL JDBC 插件开始注册 ===")
    @logger.info("插件版本: 1.0.0")
    if @verbose_logging
      @logger.info("配置参数:")
      @logger.info("  - driver_jar_path: #{@driver_jar_path}")
      @logger.info("  - driver_class: #{@driver_class}")
      @logger.info("  - connection_string: #{@connection_string}")
      @logger.info("  - username: #{@username}")
      @logger.info("  - batch_size: #{@batch_size}")
      @logger.info("  - max_pool_size: #{@max_pool_size}")
      @logger.info("  - output_delay: #{@output_delay}ms")
    end
    
    # 检查JDBC驱动文件是否存在
    begin
      driver_file = java.io.File.new(@driver_jar_path)
      if driver_file.exists()
        @logger.info("JDBC驱动文件存在: #{@driver_jar_path}") if @verbose_logging
        @logger.info("文件大小: #{driver_file.length()} bytes") if @verbose_logging
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
      @logger.info("尝试加载JDBC驱动: #{@driver_class}") if @verbose_logging
      java.lang.Class.forName(@driver_class)
      @logger.info("JDBC驱动加载成功: #{@driver_class}") if @verbose_logging
    rescue => e
      @logger.error("JDBC驱动加载失败: #{e.message}")
      @logger.error("请确保JDBC驱动JAR文件已正确放置在classpath中")
      raise e
    end
    
    # 测试数据库连接
    begin
      @logger.info("测试数据库连接...") if @verbose_logging
      test_conn = DriverManager.getConnection(@connection_string, @username, @password)
      if test_conn.isValid(5)
        @logger.info("数据库连接测试成功") if @verbose_logging
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
    
    @logger.info("初始化连接池，最大大小: #{@max_pool_size}") if @verbose_logging
    # 不在启动时预创建所有连接，而是在需要时动态创建
    # 这样可以避免启动时的连接问题，并且更有效地管理连接资源
    
    @logger.info("连接池初始化完成，将按需创建连接")
    
    # 获取目标表字段信息
    @field_info = get_table_field_info
    @logger.info("目标表字段信息获取完成，字段数: #{@field_info.size}")
    
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
    
    # 启动连接池监控线程
    @logger.info("启动连接池监控线程，间隔: #{@pool_monitor_interval}秒")
    @pool_monitor_thread = Thread.new do
      loop do
        sleep @pool_monitor_interval
        monitor_connection_pool
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
      @logger.debug("Added event to batch buffer. Current batch size: #{current_batch_size}, Batch threshold: #{@batch_size}") if @verbose_logging
      
      if current_batch_size >= @batch_size
        @logger.info("Batch threshold reached. Starting flush process...") if @verbose_logging
        flush_batch_with_retry
      end
    end
    processing_time = ((Time.now - start_time) * 1000).round(2)
    @logger.debug("Event processing completed in #{processing_time}ms") if @verbose_logging
  end

  public
  def close
    @logger.info("Closing QL JDBC output plugin")
    
    # 停止监控线程
    @pool_monitor_thread.exit if @pool_monitor_thread
    @flush_thread.exit if @flush_thread
    
    # 刷新剩余数据
    @batch_mutex.synchronize { flush_batch_with_retry if @batch_buffer.size > 0 }
    
    # 关闭预处理语句
    @prepared_statements.each { |key, stmt| stmt.close() if stmt rescue nil }
    @prepared_statements.clear
    
    # 关闭所有连接
    @pool_mutex.synchronize do
      @connection_pool.each { |conn| conn.close() if conn && !conn.isClosed() rescue nil }
      @connection_pool.clear
    end
    
    @logger.info("QL JDBC output plugin closed successfully")
  end

  private
  def get_connection
    @pool_mutex.synchronize do
      # 首先尝试从连接池中获取可用连接
      @connection_pool.each do |conn|
        if conn && !conn.isClosed() && conn.isValid(5)
          @logger.debug("Reusing existing connection from pool") if @verbose_logging
          return conn
        end
      end
      
      # 清理无效连接
      @connection_pool.reject! do |conn|
        if conn.nil? || conn.isClosed() || !conn.isValid(5)
          @logger.debug("Removing invalid connection from pool") if @verbose_logging
          conn.close() rescue nil
          true
        else
          false
        end
      end
      
      # 如果连接池未满，创建新连接
      if @connection_pool.size < @max_pool_size
        retry_count = 0
        begin
          conn = DriverManager.getConnection(@connection_string, @username, @password)
          conn.setAutoCommit(false)
          @connection_pool << conn
          @logger.info("Created new database connection. Pool size: #{@connection_pool.size}/#{@max_pool_size}")
          return conn
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
      else
        @logger.error("Connection pool is full (#{@connection_pool.size}/#{@max_pool_size}). Available connections: #{@connection_pool.count { |conn| conn && !conn.isClosed() && conn.isValid(5) rescue false }}")
        raise "Connection pool is full"
      end
    end
  end

  private
  def return_connection_to_pool(conn)
    return unless conn
    
    @pool_mutex.synchronize do
      # 检查连接是否仍然有效
      if conn && !conn.isClosed() && conn.isValid(5)
        @logger.debug("Returning valid connection to pool") if @verbose_logging
        # 连接已经在池中，不需要额外操作
      else
        # 连接无效，从池中移除
        @connection_pool.reject! { |c| c == conn }
        @logger.warn("Removed invalid connection from pool") if @verbose_logging
        conn.close() rescue nil
      end
    end
  end

  private
  def monitor_connection_pool
    @pool_mutex.synchronize do
      total_connections = @connection_pool.size
      valid_connections = @connection_pool.count { |conn| conn && !conn.isClosed() && conn.isValid(5) rescue false }
      
      @logger.info("连接池状态监控 - 总连接数: #{total_connections}/#{@max_pool_size}, 有效连接数: #{valid_connections}")
      
      # 清理无效连接
      if total_connections > valid_connections
        @logger.warn("检测到 #{total_connections - valid_connections} 个无效连接，正在清理...")
        @connection_pool.reject! do |conn|
          if conn.nil? || conn.isClosed() || !conn.isValid(5)
            conn.close() rescue nil
            true
          else
            false
          end
        end
        @logger.info("连接池清理完成，当前有效连接数: #{@connection_pool.size}")
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
    @logger.info("Starting batch flush. Batch size: #{batch_size}, Connection pool size: #{@connection_pool.size}") if @verbose_logging
    
    begin
      # 使用缓存的预处理语句
      stmt_key = conn.hash
      stmt = @prepared_statements[stmt_key]
      if stmt.nil?
        stmt_prepare_start = Time.now
        stmt = conn.prepareStatement(@statement.first)
        @prepared_statements[stmt_key] = stmt
        stmt_prepare_time = ((Time.now - stmt_prepare_start) * 1000).round(2)
        @logger.info("Prepared statement created in #{stmt_prepare_time}ms") if @verbose_logging
      else
        @logger.debug("Using cached prepared statement") if @verbose_logging
      end
      
      # 记录参数设置开始时间
      param_setup_start = Time.now
      @batch_buffer.each_with_index do |event, event_index|
        @statement[1..-1].each_with_index do |param, index|
          value = event.get(param)
          if value.nil?
            stmt.setNull(index + 1, java.sql.Types::VARCHAR)
          else
            # 智能时间字段检测和处理
            processed_value = process_time_field(param, value)
            
            # 根据处理后的值类型设置参数
            case processed_value
            when String
              stmt.setString(index + 1, processed_value)
            when Integer
              stmt.setLong(index + 1, processed_value)
            when Float
              stmt.setDouble(index + 1, processed_value)
            when java.sql.Timestamp
              stmt.setTimestamp(index + 1, processed_value)
            else
              stmt.setString(index + 1, processed_value.to_s)
            end
          end
        end
        stmt.addBatch()
      end
      param_setup_time = ((Time.now - param_setup_start) * 1000).round(2)
      @logger.debug("Parameter setup completed in #{param_setup_time}ms for #{batch_size} records") if @verbose_logging
      
      # 记录执行开始时间
      execute_start = Time.now
      stmt.executeBatch()
      execute_time = ((Time.now - execute_start) * 1000).round(2)
      @logger.debug("Batch execution completed in #{execute_time}ms") if @verbose_logging
      
      # 记录提交开始时间
      commit_start = Time.now
      conn.commit()
      commit_time = ((Time.now - commit_start) * 1000).round(2)
      @logger.debug("Transaction commit completed in #{commit_time}ms") if @verbose_logging
      
      total_time = ((Time.now - start_time) * 1000).round(2)
      records_per_second = (batch_size / (total_time / 1000.0)).round(2)
      @logger.info("Successfully inserted #{batch_size} records in #{total_time}ms (#{records_per_second} records/sec)")
      @batch_buffer.clear
      
      # 添加数据库插入后的延迟控制
      if @output_delay > 0
        @logger.debug("Applying output delay after database insertion: #{@output_delay}ms") if @verbose_logging
        sleep(@output_delay / 1000.0)
      end
    rescue => e
      conn.rollback()
      @logger.error("Batch flush failed after #{((Time.now - start_time) * 1000).round(2)}ms: #{e.message}")
      raise e
    ensure
      # 重要：将连接返回到连接池
      return_connection_to_pool(conn)
    end
  end
  
  private
  
  # 获取目标表字段信息
  def get_table_field_info
    field_info = {}
    conn = nil
    
    begin
      conn = DriverManager.getConnection(@connection_string, @username, @password)
      
      # 从INSERT语句中提取表名
      table_name = extract_table_name_from_statement
      @logger.info("检测到目标表名: #{table_name}")
      
      # 获取表结构信息
      sql = "DESCRIBE #{table_name}"
      @logger.info("执行SQL获取表结构: #{sql}")
      
      stmt = conn.createStatement
      rs = stmt.executeQuery(sql)
      
      while rs.next
        field_name = rs.getString("Field")
        field_type = rs.getString("Type")
        is_null = rs.getString("Null")
        key_type = rs.getString("Key")
        default_value = rs.getString("Default")
        extra = rs.getString("Extra")
        
        field_info[field_name] = {
          'type' => field_type,
          'is_null' => is_null == 'YES',
          'key_type' => key_type,
          'default_value' => default_value,
          'extra' => extra
        }
        
        # 特别记录bit字段类型
        if field_type.downcase.include?('bit')
          @logger.info("检测到bit字段: #{field_name} => #{field_type}")
        end
        
        @logger.debug("字段信息: #{field_name} => #{field_type} (NULL: #{is_null}, KEY: #{key_type})")
      end
      
      rs.close
      stmt.close
      
      @logger.info("成功获取表结构信息，共 #{field_info.size} 个字段")
      
    rescue => e
      @logger.error("获取表字段信息失败: #{e.message}")
      @logger.error("将使用启发式字段处理逻辑")
      # 返回空哈希，使用启发式处理逻辑
      return {}
    ensure
      conn.close if conn
    end
    
    field_info
  end
  
  # 从INSERT语句中提取表名
  def extract_table_name_from_statement
    # 匹配 INSERT INTO table_name 模式
    if @statement.first.match(/INSERT\s+INTO\s+(\w+)/i)
      return $1
    end
    
    # 如果无法提取，返回默认表名
    @logger.warn("无法从INSERT语句中提取表名，使用默认表名: rental_user")
    return "rental_user"
  end
  
  # 智能时间字段检测和处理
  def process_time_field(field_name, value)
    # 1. 优先使用MySQL字段信息进行精确处理
    if @field_info && @field_info[field_name]
      field_type = @field_info[field_name]['type']
      @logger.debug("MySQL字段信息: '#{field_name}' => #{field_type}")
      
      # 根据MySQL字段类型进行精确处理
      return process_field_by_mysql_type(field_name, value, field_type)
    end
    
    # 2. 如果没有MySQL字段信息，使用启发式检测
    return process_field_by_heuristic(field_name, value)
  end
  
  # 根据MySQL字段类型处理字段
  def process_field_by_mysql_type(field_name, value, mysql_type)
    mysql_type_lower = mysql_type.downcase
    
    # 时间相关字段类型
    if mysql_type_lower.include?('datetime') || mysql_type_lower.include?('timestamp')
      return process_datetime_field(field_name, value)
    elsif mysql_type_lower.include?('date')
      return process_date_field(field_name, value)
    elsif mysql_type_lower.include?('time')
      return process_time_only_field(field_name, value)
    elsif mysql_type_lower.include?('int') || mysql_type_lower.include?('bigint')
      return process_integer_field(field_name, value)
    elsif mysql_type_lower.include?('decimal') || mysql_type_lower.include?('float') || mysql_type_lower.include?('double')
      return process_decimal_field(field_name, value)
    elsif mysql_type_lower.include?('bit')
      return process_bit_field(field_name, value)
    elsif mysql_type_lower.include?('varchar') || mysql_type_lower.include?('text') || mysql_type_lower.include?('char')
      return process_string_field(field_name, value)
    else
      @logger.debug("未知MySQL字段类型: #{mysql_type} for field '#{field_name}', 使用启发式处理")
      return process_field_by_heuristic(field_name, value)
    end
  end
  
  # 处理datetime字段
  def process_datetime_field(field_name, value)
    if value.is_a?(LogStash::Timestamp)
      @logger.debug("LogStash::Timestamp detected for datetime field '#{field_name}' with value '#{value}'")
      begin
        formatted_time = value.time.strftime("%Y-%m-%d %H:%M:%S")
        @logger.debug("Converted LogStash::Timestamp to datetime: '#{value}' => '#{formatted_time}'")
        return formatted_time
      rescue => e
        @logger.warn("Failed to format LogStash::Timestamp for datetime field '#{field_name}': #{e.message}")
        return value.to_s
      end
    elsif value.is_a?(String)
      # 时间戳字符串检测（13位或10位数字）
      if value.match?(/^\d{10,13}$/)
        @logger.debug("Timestamp string detected for datetime field '#{field_name}' with value '#{value}'")
        begin
          # 如果是13位时间戳，转换为10位
          timestamp = value.length == 13 ? value.to_i / 1000 : value.to_i
          parsed_time = Time.at(timestamp)
          formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
          @logger.debug("Converted timestamp to datetime: '#{value}' => '#{formatted_time}'")
          return formatted_time
        rescue => e
          @logger.warn("Failed to parse timestamp for datetime field '#{field_name}': #{e.message}")
          return value
        end
      # ISO8601格式检测
      elsif value.match?(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$/)
        @logger.debug("ISO8601 format detected for datetime field '#{field_name}'")
        begin
          parsed_time = Time.parse(value)
          formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
          @logger.debug("Converted ISO8601 to datetime: '#{value}' => '#{formatted_time}'")
          return formatted_time
        rescue => e
          @logger.warn("Failed to parse ISO8601 for datetime field '#{field_name}': #{e.message}")
          return value
        end
      elsif value.match?(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)
        @logger.debug("MySQL datetime format already correct for field '#{field_name}'")
        return value
      end
    end
    
    # 如果无法处理，返回原值
    return value
  end
  
  # 处理date字段
  def process_date_field(field_name, value)
    if value.is_a?(LogStash::Timestamp)
      @logger.debug("LogStash::Timestamp detected for date field '#{field_name}'")
      begin
        formatted_date = value.time.strftime("%Y-%m-%d")
        @logger.debug("Converted LogStash::Timestamp to date: '#{value}' => '#{formatted_date}'")
        return formatted_date
      rescue => e
        @logger.warn("Failed to format LogStash::Timestamp for date field '#{field_name}': #{e.message}")
        return value.to_s
      end
    elsif value.is_a?(String)
      if value.match?(/^\d{4}-\d{2}-\d{2}$/)
        @logger.debug("Date format already correct for field '#{field_name}'")
        return value
      elsif value.match?(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}/)
        @logger.debug("Extracting date from datetime for field '#{field_name}'")
        begin
          parsed_time = Time.parse(value)
          formatted_date = parsed_time.strftime("%Y-%m-%d")
          @logger.debug("Extracted date: '#{value}' => '#{formatted_date}'")
          return formatted_date
        rescue => e
          @logger.warn("Failed to extract date from datetime for field '#{field_name}': #{e.message}")
          return value
        end
      end
    end
    
    return value
  end
  
  # 处理time字段
  def process_time_only_field(field_name, value)
    if value.is_a?(LogStash::Timestamp)
      @logger.debug("LogStash::Timestamp detected for time field '#{field_name}'")
      begin
        formatted_time = value.time.strftime("%H:%M:%S")
        @logger.debug("Converted LogStash::Timestamp to time: '#{value}' => '#{formatted_time}'")
        return formatted_time
      rescue => e
        @logger.warn("Failed to format LogStash::Timestamp for time field '#{field_name}': #{e.message}")
        return value.to_s
      end
    elsif value.is_a?(String)
      if value.match?(/^\d{2}:\d{2}:\d{2}$/)
        @logger.debug("Time format already correct for field '#{field_name}'")
        return value
      end
    end
    
    return value
  end
  
  # 智能检测布尔值
  def is_boolean_value?(value)
    # 检查是否为布尔类型
    return true if value.is_a?(TrueClass) || value.is_a?(FalseClass)
    
    # 检查是否为布尔字符串
    if value.is_a?(String)
      return true if ['true', 'false', '1', '0', 'yes', 'no', 'on', 'off'].include?(value.downcase)
    end
    
    # 检查是否为布尔整数
    if value.is_a?(Integer)
      return true if value == 0 || value == 1
    end
    
    false
  end
  
  # 将布尔值转换为tinyint
  def convert_boolean_to_tinyint(value)
    if value == true || value == "true" || value == "1" || value == "yes" || value == "on"
      return 1
    elsif value == false || value == "false" || value == "0" || value == "no" || value == "off"
      return 0
    elsif value.nil? || value == "" || value == "null"
      return 0
    elsif value.is_a?(Integer)
      return (value == 0 || value == 1) ? value : 0
    else
      return 0
    end
  end

  # 处理bit字段
  def process_bit_field(field_name, value)
    @logger.debug("处理bit字段 '#{field_name}' with value: #{value}")
    if value.nil?
      @logger.debug("bit字段 '#{field_name}' 值为nil，返回0")
      return 0
    end
    
    # 确保只返回0或1，适合bit(1)类型
    if value == true || value == "true" || value == "1" || value == "yes" || value == "on" || value == 1
      @logger.debug("bit字段 '#{field_name}' 转换结果: #{value} => 1")
      return 1
    else
      @logger.debug("bit字段 '#{field_name}' 转换结果: #{value} => 0")
      return 0
    end
  end
  
  # 处理整数字段
  def process_integer_field(field_name, value)
    if value.is_a?(String)
      begin
        integer_value = value.to_i
        @logger.debug("Converted string to integer for field '#{field_name}': '#{value}' => #{integer_value}")
        return integer_value
      rescue => e
        @logger.warn("Failed to convert string to integer for field '#{field_name}': '#{value}' => #{e.message}")
        return value
      end
    elsif value.is_a?(Float)
      integer_value = value.to_i
      @logger.debug("Converted float to integer for field '#{field_name}': #{value} => #{integer_value}")
      return integer_value
    end
    
    return value
  end
  
  # 处理小数字段
  def process_decimal_field(field_name, value)
    if value.is_a?(String)
      begin
        float_value = value.to_f
        @logger.debug("Converted string to float for field '#{field_name}': '#{value}' => #{float_value}")
        return float_value
      rescue => e
        @logger.warn("Failed to convert string to float for field '#{field_name}': '#{value}' => #{e.message}")
        return value
      end
    end
    
    return value
  end
  
  # 处理字符串字段
  def process_string_field(field_name, value)
    if value.is_a?(LogStash::Timestamp)
      @logger.debug("Converting LogStash::Timestamp to string for field '#{field_name}'")
      return value.time.strftime("%Y-%m-%d %H:%M:%S")
    end
    
    return value.to_s
  end
  
  # 启发式字段处理（当没有MySQL字段信息时使用）
  def process_field_by_heuristic(field_name, value)
    # 1. 智能检测布尔值，转换为tinyint
    if is_boolean_value?(value)
      @logger.debug("Boolean value detected for field '#{field_name}' with value '#{value}'")
      return convert_boolean_to_tinyint(value)
    end
    
    # 2. 检查是否为LogStash::Timestamp对象
    if value.is_a?(LogStash::Timestamp)
      @logger.debug("LogStash::Timestamp detected for field '#{field_name}' with value '#{value}'")
      begin
        formatted_time = value.time.strftime("%Y-%m-%d %H:%M:%S")
        @logger.debug("Converted LogStash::Timestamp from '#{value}' to '#{formatted_time}' for field '#{field_name}'")
        return formatted_time
      rescue => e
        @logger.warn("Failed to format LogStash::Timestamp for field '#{field_name}': '#{value}', using original value: #{e.message}")
        return value.to_s
      end
    end
    
    # 2. 检查字符串值是否为时间格式
    if value.is_a?(String)
      # ISO8601格式检测 (2024-05-25T17:11:12Z 或 2024-05-25T17:11:12.123Z)
      if value.match?(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$/)
        @logger.debug("ISO8601 pattern matched for field '#{field_name}' with value '#{value}'")
        begin
          parsed_time = Time.parse(value)
          formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
          @logger.debug("Converted ISO8601 date from '#{value}' to '#{formatted_time}' for field '#{field_name}'")
          return formatted_time
        rescue => e
          @logger.warn("Failed to parse ISO8601 date for field '#{field_name}': '#{value}', using original value: #{e.message}")
          return value
        end
      end
      
      # MySQL datetime格式检测 (2024-05-25 17:11:12)
      if value.match?(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)
        @logger.debug("MySQL datetime format detected for field '#{field_name}' with value '#{value}'")
        return value
      end
      
      # 日期格式检测 (2024-05-25)
      if value.match?(/^\d{4}-\d{2}-\d{2}$/)
        @logger.debug("Date format detected for field '#{field_name}' with value '#{value}'")
        return value
      end
      
      # 时间格式检测 (17:11:12)
      if value.match?(/^\d{2}:\d{2}:\d{2}$/)
        @logger.debug("Time format detected for field '#{field_name}' with value '#{value}'")
        return value
      end
    end
    
    # 3. 检查字段名是否包含时间相关关键词
    time_keywords = ['time', 'date', 'created', 'updated', 'modified', 'timestamp', 'start', 'end', 'begin', 'finish']
    if time_keywords.any? { |keyword| field_name.downcase.include?(keyword) }
      @logger.debug("Time-related field name detected: '#{field_name}' with value '#{value}'")
      # 如果是字符串，尝试解析为时间格式
      if value.is_a?(String)
        begin
          # 尝试多种时间格式解析
          parsed_time = Time.parse(value)
          formatted_time = parsed_time.strftime("%Y-%m-%d %H:%M:%S")
          @logger.debug("Converted time field '#{field_name}' from '#{value}' to '#{formatted_time}'")
          return formatted_time
        rescue => e
          @logger.debug("Failed to parse time field '#{field_name}': '#{value}', using original value: #{e.message}")
          return value
        end
      end
    end
    
    # 4. 如果不是时间字段，返回原值
    return value
  end
end 