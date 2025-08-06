# QL Logstash JDBC Output Plugin

一个高性能的Logstash JDBC输出插件，支持批量插入、连接池管理和自动重试机制。

**适用于 Logstash 8.x 版本**

## 系统要求

- **Logstash**: 8.0.0 或更高版本
- **Java**: JDK 11 或更高版本
- **Ruby**: 2.7 或更高版本（Logstash 8.x 内置）

## 功能特性

- ✅ **批量插入**: 支持批量数据插入，大幅提升同步性能
- ✅ **连接池管理**: 内置数据库连接池，优化资源使用
- ✅ **自动重试**: 智能重试机制，处理网络波动和死锁
- ✅ **性能监控**: 内置性能监控和详细日志记录
- ✅ **事务支持**: 支持数据库事务，确保数据一致性
- ✅ **配置灵活**: 丰富的配置选项，适应不同场景需求

## 安装方法

### 方法1: 使用gem文件安装

```bash
# 下载插件gem文件
wget https://github.com/suuugeee/ql-logstash-out-jdbc/releases/download/v8.0.0/ql_logstash_out_jdbc-1.0.0.gem

# 安装插件
logstash-plugin install ql_logstash_out_jdbc-1.0.0.gem
```

### 方法2: 从源码构建安装

```bash
# 克隆仓库
git clone https://github.com/suuugeee/ql-logstash-out-jdbc.git
cd ql-logstash-out-jdbc

# 构建gem文件
gem build ql_logstash_out_jdbc.gemspec

# 安装插件
logstash-plugin install ql_logstash_out_jdbc-1.0.0.gem
```

### 方法3: Docker环境安装

在Dockerfile中添加：

```dockerfile
# 复制插件文件
COPY custom_plugins/ql_logstash_out_jdbc/ql_logstash_out_jdbc-1.0.0.gem /usr/share/logstash/custom_plugins/

# 安装插件
RUN /usr/share/logstash/bin/logstash-plugin install --no-verify /usr/share/logstash/custom_plugins/ql_logstash_out_jdbc-1.0.0.gem
```

## 配置示例

### 基础配置

```ruby
output {
  ql_jdbc {
    driver_jar_path => "/path/to/mysql-connector-java.jar"
    driver_class => "com.mysql.cj.jdbc.Driver"
    connection_string => "jdbc:mysql://localhost:3306/database?useSSL=false"
    username => "your_username"
    password => "your_password"
    statement => [
      "INSERT INTO table (field1, field2) VALUES (?, ?)",
      "field1", "field2"
    ]
  }
}
```

### 高级配置

```ruby
output {
  ql_jdbc {
    # 数据库连接配置
    driver_jar_path => "/path/to/mysql-connector-java.jar"
    driver_class => "com.mysql.cj.jdbc.Driver"
    connection_string => "jdbc:mysql://localhost:3306/database?useSSL=false&serverTimezone=UTC"
    username => "your_username"
    password => "your_password"
    
    # SQL语句配置
    statement => [
      "INSERT INTO users (id, name, email, created_at) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)",
      "id", "name", "email", "created_at"
    ]
    
    # 连接池配置
    max_pool_size => 20
    connection_timeout => 60000
    
    # 重试配置
    max_retries => 3
    retry_delay => 1000
    
    # 批量处理配置
    batch_size => 1000
    flush_interval => 5
  }
}
```

## 配置参数说明

### 必需参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `driver_jar_path` | string | JDBC驱动jar文件路径 |
| `driver_class` | string | JDBC驱动类名 |
| `connection_string` | string | 数据库连接字符串 |
| `username` | string | 数据库用户名 |
| `password` | string | 数据库密码 |
| `statement` | array | SQL语句和参数映射 |

### 可选参数

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `max_pool_size` | number | 10 | 连接池最大连接数 |
| `connection_timeout` | number | 30000 | 连接超时时间(毫秒) |
| `max_retries` | number | 3 | 最大重试次数 |
| `retry_delay` | number | 1000 | 重试延迟时间(毫秒) |
| `batch_size` | number | 1000 | 批量插入大小 |
| `flush_interval` | number | 5 | 刷新间隔(秒) |

## 使用场景

### 1. 数据同步
```ruby
input {
  jdbc {
    jdbc_connection_string => "jdbc:mysql://source_host:3306/source_db"
    statement => "SELECT * FROM source_table"
    schedule => "0 2 * * *"
  }
}

output {
  ql_jdbc {
    connection_string => "jdbc:mysql://target_host:3306/target_db"
    statement => [
      "INSERT INTO target_table (id, data) VALUES (?, ?) ON DUPLICATE KEY UPDATE data = VALUES(data)",
      "id", "data"
    ]
    batch_size => 5000
  }
}
```

### 2. 实时数据处理
```ruby
input {
  kafka {
    bootstrap_servers => "localhost:9092"
    topics => ["data-topic"]
  }
}

output {
  ql_jdbc {
    connection_string => "jdbc:mysql://localhost:3306/analytics"
    statement => [
      "INSERT INTO events (timestamp, user_id, event_type) VALUES (?, ?, ?)",
      "timestamp", "user_id", "event_type"
    ]
    batch_size => 100
    flush_interval => 1
  }
}
```

## 性能优化建议

1. **批量大小**: 根据数据量和网络情况调整`batch_size`
2. **连接池**: 合理设置`max_pool_size`，避免连接数过多
3. **重试策略**: 根据业务需求调整`max_retries`和`retry_delay`
4. **SQL优化**: 使用合适的索引和SQL语句

## 故障排除

### 常见问题

1. **连接失败**
   - 检查数据库连接字符串和凭据
   - 确认JDBC驱动版本兼容性

2. **性能问题**
   - 调整批量大小和刷新间隔
   - 检查数据库索引和SQL优化

3. **内存问题**
   - 减少批量大小
   - 增加JVM堆内存

### 日志查看

插件会输出详细的日志信息，包括：
- 连接池状态
- 批量处理统计
- 错误和重试信息

## 版本历史

### v8.0.0
- 适配 Logstash 8.x 版本
- 支持基础JDBC操作
- 实现连接池和重试机制
- 添加批量处理功能
- 优化性能和稳定性

## 贡献指南

欢迎提交Issue和Pull Request来改进这个插件。

## 许可证

MIT License

## 联系方式

- GitHub: [https://github.com/suuugeee/ql-logstash-out-jdbc](https://github.com/suuugeee/ql-logstash-out-jdbc)
- Issues: [https://github.com/suuugeee/ql-logstash-out-jdbc/issues](https://github.com/suuugeee/ql-logstash-out-jdbc/issues) 