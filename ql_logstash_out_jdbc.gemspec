Gem::Specification.new do |s|
  s.name = 'ql_logstash_out_jdbc'
  s.version = '1.0.0'
  s.licenses = ['Apache-2.0']
  s.summary = 'QL Logstash JDBC 批量输出插件'
  s.description = '支持批量插入的 Logstash JDBC 输出插件，提高数据同步性能'
  s.authors = ['QL Team']
  s.email = 'support@ql.com'
  s.homepage = 'https://gitee.com/sugelalala/ql-logstash-output-jdbc'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
  
  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-mixin-ecs_compatibility_support", "~> 1.3"
  s.add_development_dependency "logstash-devutils", "~> 2.0"
end 