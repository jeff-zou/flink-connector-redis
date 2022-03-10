### 插件名称：flink-connector-redis
### 插件地址：https://github.com/jeff-zou/flink-connector-redis.git

### 项目介绍
基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，使它支持SQL直接定义写入redis,用户通过DDL指定自己需要保存的字段。

### 使用方法: 
命令行执行 mvn package -DskipTests=true打包后，将生成的包flink-connector-redis_2.12-1.11.1.jar引入flink lib中即可，无需其它设置。

### 重构介绍：
相对上一个版本简化了参数设置，思路更清晰，上一版本字段的值会根据主键等条件来自动生成，这要求使用者需要了解潜在相关规则，有一定的学习成本并且容易埋坑，重构后字段的值由用户在DDL中显示地指定，如下：
```
  'key-column'='username','value-column'='passport',' //直接指定字段名
```
取消了必须有主键的限制，使用更简单，如果有多个字段组合成key或者value,需要用户在DML中使用concat_ws等方式组装，不再是插件在后台用不可见字符拼装。

### 使用示例:
- 1.SQL方式 <br>
**示例代码路径:**  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLTest.java<br>
set示例，相当于redis命令： *set test test11*
```
       StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

        String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
                "'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','command'='set')" ;

        tEnv.executeSql(ddl);
        String sql = " insert into sink_redis select * from (values ('test', 'test11'))";
        TableResult tableResult = tEnv.executeSql(sql);
        tableResult.getJobClient().get()
                .getJobExecutionResult()
                .get();
```
- 2.DataStream方式<br>
**示例代码路径:** 
 src/test/java/org.apache.flink.streaming.connectors.redis.datastream.DataStreamTest.java<br>
hset示例，相当于redis命令：*hset tom math 150*
```
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisMapper redisMapper = RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, configuration.toMap())
                .createRedisMapper(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(0, "tom");
        genericRowData.setField(1, "math");
        genericRowData.setField(2, "150");
        DataStream<GenericRowData> dataStream = env.fromElements(genericRowData);

        TableSchema tableSchema =  new TableSchema.Builder() .field("name", DataTypes.STRING().notNull()).field("subject", DataTypes.STRING()).field("score", DataTypes.INT()).build();

        FlinkJedisConfigBase conf = getLocalRedisClusterConfig();
        RedisSinkFunction redisSinkFunction = new RedisSinkFunction<>(conf, redisMapper, tableSchema);

        dataStream.addSink(redisSinkFunction);
        env.execute("RedisSinkTest");
```