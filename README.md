### 插件名称：flink-connector-redis
###### 插件地址：https://github.com/jeff-zou/flink-connector-redis.git

###### 无法翻墙：https://gitee.com/jeff-zou/flink-connector-redis.git

###### 

### 项目介绍

基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，相对bahir增加的内容有：Table API, 维表查询。参考了腾讯云与阿里云两家主流云产商的流计算产品，取两家之长，并增加了更丰富的功能。

支持功能对应redis操作命令有：

| 插入                           | 维表查询 |
| ------------------------------ | -------- |
| set                            | get      |
| hset                           | hget     |
| rpush lpush                    |          |
| incrBy decrBy hincrBy  zincrby |          |
| sadd zadd pfadd(hyperloglog)   |          |

### 使用方法: 
命令行执行 mvn package -DskipTests打包后，将生成的包flink-connector-redis_2.12-1.13.2.jar引入flink lib中即可，无需其它设置。



### 使用说明：

无需通过primary key来映射redis中的Key，直接由ddl中的字段顺序来决定Key,如：

```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, score为value.
```



with参数说明：

| 字段                  | 默认值 | 类型    | 说明                                                         |
| --------------------- | ------ | ------- | ------------------------------------------------------------ |
| connector             | (none) | String  | `redis`                                                      |
| host                  | (none) | String  | Redis IP                                                     |
| port                  | 6379   | Integer | Redis 端口                                                   |
| password              | null   | String  | 如果没有设置，则为 null                                      |
| database              | 0      | Integer | 默认使用 db0                                                 |
| maxTotal              | 2      | Integer | 最大连接数                                                   |
| maxIdle               | 2      | Integer | 最大保持连接数                                               |
| minIdle               | 1      | Integer | 最小保持连接数                                               |
| timeout               | 2000   | Integer | 连接超时时间，单位 ms，默认 1s                               |
| cluster-nodes         | (none) | String  | 集群类型 ，当redis-mode为cluster时不为空，支持类型有：sentinels cluster |
| command               | (none) | String  | 对应上文中的redis命令                                        |
| redis-mode            | (none) | Integer | redis类型： single cluster                                   |
| lookup.cache.max-rows | -1     | Integer | lookup 缓存大小                                              |
| lookup.cache.ttl      | -1     | Integer | 缓存过期时间                                                 |
| lookup.max-retries    | 3      | Integer | lookup 失败重试次数                                          |

其它参数请参考bahir



### 使用示例:
- ##### 维表查询：
```
create table sink_redis(name varchar, level varchar, age varchar) with ( 'connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','command'='hset');

-- 先在redis中插入数据,相当于redis命令： hset 3 3 100 --
insert into sink_redis select * from (values ('3', '3', '100'));
                
create table dim_table (name varchar, level varchar, age varchar) with ('connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single', 'password'='*****','command'='hget', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3');
    
-- 随机生成10以内的数据作为数据源 --
-- 其中有一条数据会是： username = 3  level = 3, 会跟上面插入的数据关联 -- 
create table source_table (username varchar, level varchar, proctime as procTime()) with ('connector'='datagen',  'rows-per-second'='1',  'fields.username.kind'='sequence',  'fields.username.start'='1',  'fields.username.end'='10', 'fields.level.kind'='sequence',  'fields.level.start'='1',  'fields.level.end'='10');

create table sink_table(username varchar, level varchar,age varchar) with ('connector'='print');

insert into
	sink_table
select
	s.username,
	s.level,
	d.age
from
	source_table s
left join dim_table for system_time as of s.proctime as d on
	d.name = s.username
	and d.level = s.level;
-- username为3那一行会关联到redisw值，输出为： 3,3,100	
```



- ##### DataStream查询方式<br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.datastream.DataStreamTest.java<br>
  hset示例，相当于redis命令：*hset tom math 150*

```
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_CLUSTER);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisSinkMapper redisMapper = (RedisSinkMapper)RedisHandlerServices
                .findRedisHandler(RedisMapperHandler.class, configuration.toMap())
                .createRedisMapper(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        GenericRowData genericRowData = new GenericRowData(3);
        genericRowData.setField(0, "tom");
        genericRowData.setField(1, "math");
        genericRowData.setField(2, "151");
        DataStream<GenericRowData> dataStream = env.fromElements(genericRowData);

       ResolvedSchema resolvedSchema = ResolvedSchema.physical(new String[]{"name", "subject", "score"}, new DataType[]{DataTypes.STRING().notNull(), DataTypes.STRING().notNull(), DataTypes.INT().notNull()});
        FlinkJedisConfigBase conf = getLocalRedisClusterConfig();
        RedisSinkFunction redisSinkFunction = new RedisSinkFunction<>(conf, redisMapper, resolvedSchema);

        dataStream.addSink(redisSinkFunction);
        env.execute("RedisSinkTest");
```



- ##### 其它写入示例 <br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLTest.java<br>
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

