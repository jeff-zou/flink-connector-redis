### 插件名称：flink-connector-redis
###### 插件地址：https://github.com/jeff-zou/flink-connector-redis.git

###### 无法翻墙：https://gitee.com/jeff-zou/flink-connector-redis.git



### 项目介绍

基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，相对bahir调整的内容有：删除过期Flink API、增加Table/SQL API、 增加维表查询支持、增加写入与查询缓存、统一使用过期策略、写入并发数等。

因bahir使用的flink接口版本较老，所以改动较大，开发过程中参考了腾讯云与阿里云两家产商的流计算产品，取两家之长，并增加了更丰富的功能，包括更多的redis操作命令和更多的redis服务类型，如：simple sentinel cluster。

支持功能对应redis的操作命令有：

| 插入                           | 维表查询 |
| ------------------------------ | -------- |
| set                            | get      |
| hset                           | hget     |
| rpush lpush                    |          |
| incrBy decrBy hincrBy  zincrby |          |
| sadd zadd pfadd(hyperloglog)   |          |
| publish                        |          |
| zrem decrby                    |          |



### 使用方法: 

命令行执行 mvn package -DskipTests打包后，将生成的包flink-connector-redis-1.0.2.jar引入flink lib中即可，无需其它设置。

开发环境工程直接引用：

```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.0.2</version>
</dependency>
```



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
| cluster-nodes         | (none) | String  | 集群ip与端口，当redis-mode为cluster时不为空，如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000 |
| command               | (none) | String  | 对应上文中的redis命令                                        |
| redis-mode            | (none) | Integer | mode类型： single cluster                                    |
| lookup.cache.max-rows | -1     | Integer | 查询缓存大小,减少对redis重复key的查询                        |
| lookup.cache.ttl      | -1     | Integer | 查询缓存过期时间，单位为秒， 开启查询缓存条件是max-rows与ttl都不能为-1 |
| lookup.max-retries    | 1      | Integer | 查询失败重试次数                                             |
| sink.cache.max-rows   | -1     | Integer | 写入缓存大小，减少对redis重复写入相同的key与value            |
| sink.cache.ttl        | -1     | Integer | 写入缓存过期时间，单位为秒， 开启缓存条件是max-rows与ttl都不能为-1 |
| sink.max-retries      | 1      | Integer | 写入失败重试次数                                             |
| sink.parallelism      | (none) | Integer | 写入并发数                                                   |



集群类型为sentinel时额外连接参数:

| 字段               | 默认值 | 类型   | 说明 |
| ------------------ | ------ | ------ | ---- |
| master.name        | (none) | String | 主名 |
| sentinels.info     | (none) | String |      |
| sentinels.password | none)  | String |      |



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
-- username为3那一行会关联到redis内的值，输出为： 3,3,100	
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
genericRowData.setField(2, "152");
DataStream<GenericRowData> dataStream = env.fromElements(genericRowData, genericRowData);

RedisCacheOptions redisCacheOptions = new RedisCacheOptions.Builder().setCacheMaxSize(100).setCacheTTL(10L).build();
FlinkJedisConfigBase conf = getLocalRedisClusterConfig();
RedisSinkFunction redisSinkFunction = new RedisSinkFunction<>(conf, redisMapper, redisCacheOptions);

dataStream.addSink(redisSinkFunction).setParallelism(1);
env.execute("RedisSinkTest");
```



- ##### redis-cluster写入示例 <br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLTest.java<br>
  set示例，相当于redis命令： *set test test11*

```
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, environmentSettings);

String ddl = "create table sink_redis(username VARCHAR, passport VARCHAR) with ( 'connector'='redis', " +
              "'cluster-nodes'='10.11.80.147:7000,10.11.80.147:7001','redis- mode'='cluster','password'='******','command'='set')" ;

tEnv.executeSql(ddl);
String sql = " insert into sink_redis select * from (values ('test', 'test11'))";
TableResult tableResult = tEnv.executeSql(sql);
tableResult.getJobClient().get()
.getJobExecutionResult()
.get();
```



### 开发与测试环境

ide: IntelliJ IDEA 

code format: google-java-format + Save Actions

code check: CheckStyle

