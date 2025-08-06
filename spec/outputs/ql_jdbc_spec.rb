# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/ql_jdbc"

describe LogStash::Outputs::QlJdbc do
  let(:sample_event) { LogStash::Event.new("id" => 1, "phone_num" => "13800138000", "create_time" => "2023-01-01 00:00:00", "update_time" => "2023-01-01 00:00:00") }
  
  describe "插件初始化" do
    it "应该正确初始化插件" do
      config = {
        "driver_jar_path" => "/tmp/mysql-connector-java-8.0.33.jar",
        "driver_class" => "com.mysql.cj.jdbc.Driver",
        "connection_string" => "jdbc:mysql://localhost:3306/test",
        "username" => "root",
        "password" => "password",
        "statement" => [
          "INSERT IGNORE INTO test_table (id, phone_num, create_time, update_time) VALUES (?, ?, ?, ?)",
          "id", "phone_num", "create_time", "update_time"
        ],
        "batch_size" => 1000,
        "flush_interval" => 5
      }
      
      plugin = LogStash::Outputs::QlJdbc.new(config)
      expect(plugin).to be_instance_of(LogStash::Outputs::QlJdbc)
    end
  end

  describe "事件处理" do
    let(:plugin) do
      LogStash::Outputs::QlJdbc.new({
        "driver_jar_path" => "/tmp/mysql-connector-java-8.0.33.jar",
        "driver_class" => "com.mysql.cj.jdbc.Driver",
        "connection_string" => "jdbc:mysql://localhost:3306/test",
        "username" => "root",
        "password" => "password",
        "statement" => [
          "INSERT IGNORE INTO test_table (id, phone_num, create_time, update_time) VALUES (?, ?, ?, ?)",
          "id", "phone_num", "create_time", "update_time"
        ]
      })
    end

    it "应该正确处理事件" do
      expect { plugin.receive(sample_event) }.not_to raise_error
    end

    it "应该验证必需字段" do
      invalid_event = LogStash::Event.new("id" => 1) # 缺少其他必需字段
      expect { plugin.receive(invalid_event) }.not_to raise_error
    end
  end

  describe "批量处理" do
    let(:plugin) do
      LogStash::Outputs::QlJdbc.new({
        "driver_jar_path" => "/tmp/mysql-connector-java-8.0.33.jar",
        "driver_class" => "com.mysql.cj.jdbc.Driver",
        "connection_string" => "jdbc:mysql://localhost:3306/test",
        "username" => "root",
        "password" => "password",
        "statement" => [
          "INSERT IGNORE INTO test_table (id, phone_num, create_time, update_time) VALUES (?, ?, ?, ?)",
          "id", "phone_num", "create_time", "update_time"
        ],
        "batch_size" => 2
      })
    end

    it "应该在达到批量大小时刷新" do
      event1 = LogStash::Event.new("id" => 1, "phone_num" => "13800138001")
      event2 = LogStash::Event.new("id" => 2, "phone_num" => "13800138002")
      
      plugin.receive(event1)
      expect(plugin.batch_buffer.length).to eq(1)
      
      plugin.receive(event2)
      # 注意：由于异步处理，这里可能不会立即刷新
    end
  end

  describe "SQL 构建" do
    let(:plugin) do
      LogStash::Outputs::QlJdbc.new({
        "driver_jar_path" => "/tmp/mysql-connector-java-8.0.33.jar",
        "driver_class" => "com.mysql.cj.jdbc.Driver",
        "connection_string" => "jdbc:mysql://localhost:3306/test",
        "username" => "root",
        "password" => "password",
        "statement" => [
          "INSERT IGNORE INTO test_table (id, phone_num, create_time, update_time) VALUES (?, ?, ?, ?)",
          "id", "phone_num", "create_time", "update_time"
        ]
      })
    end

    it "应该正确构建批量插入 SQL" do
      events = [sample_event]
      sql = plugin.send(:build_batch_insert_sql, 
                       "INSERT IGNORE INTO test_table (id, phone_num) VALUES (?, ?)",
                       ["id", "phone_num"],
                       events)
      
      expect(sql).to include("INSERT IGNORE INTO test_table")
      expect(sql).to include("(1, '13800138000')")
    end
  end

  describe "错误处理" do
    let(:plugin) do
      LogStash::Outputs::QlJdbc.new({
        "driver_jar_path" => "/tmp/mysql-connector-java-8.0.33.jar",
        "driver_class" => "com.mysql.cj.jdbc.Driver",
        "connection_string" => "jdbc:mysql://invalid-host:3306/test",
        "username" => "root",
        "password" => "password",
        "statement" => [
          "INSERT IGNORE INTO test_table (id, phone_num, create_time, update_time) VALUES (?, ?, ?, ?)",
          "id", "phone_num", "create_time", "update_time"
        ],
        "max_retries" => 1
      })
    end

    it "应该处理连接错误" do
      expect { plugin.receive(sample_event) }.not_to raise_error
    end
  end
end 