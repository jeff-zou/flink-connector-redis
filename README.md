[中文](cn.md)

### Introduction

Based on the  [bahir-flink](https://github.com/apache/bahir-flink.git)，the contents adjusted relative to bahir include: deleting expired Flink API, adding Table/SQL API, adding dimension table query support, adding write and query cache, unified use of expiration policy, number of concurrent writes, etc.

Due to the older version of the flink interface used by bahir, the changes were relatively large. During the development process, the stream computing products of Tencent Cloud and Alibaba Cloud were referenced, taking the advantages of the two, and adding richer functions, including more Redis operation commands and more redis service types, such as: simple、 sentinel、 cluster.

The operation commands corresponding to the supported functions of redis are:

| insert                         | Dimension table query |
| ------------------------------ | --------------------- |
| set                            | get                   |
| hset                           | hget                  |
| rpush lpush                    |                       |
| incrBy decrBy hincrBy  zincrby |                       |
| sadd zadd pfadd(hyperloglog)   |                       |
| publish                        |                       |
| zrem decrby                    |                       |



### Instructions: 

After executing mvn package -DskipTests on the command line, import the generated package flink-connector-redis-1.0.2.jar into flink lib, no other settings are required.

Development environment engineering direct reference:

```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.0.3</version>
</dependency>
```



### Instructions for use：

There is no need to map the key in redis through the primary key, the key is directly determined by the order of the fields in the ddl, such as:

```
#Where username is the key and passport is the value.
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 

#The name is the key of the map structure, the subject is the field, and the score is the value.
create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
```



##### with parameter description:

| Field                 | Default | Type    | Description                                                  |
| --------------------- | ------- | ------- | ------------------------------------------------------------ |
| connector             | (none)  | String  | `redis`                                                      |
| host                  | (none)  | String  | Redis IP                                                     |
| port                  | 6379    | Integer | Redis port                                                   |
| password              | null    | String  | null if not set                                              |
| database              | 0       | Integer | db0 is used by default                                       |
| maxTotal              | 2       | Integer | Maximum number of connections                                |
| maxIdle               | 2       | Integer | Max keepalive connections                                    |
| minIdle               | 1       | Integer | Minimum keepalive connections                                |
| timeout               | 2000    | Integer | Connection timeout, in ms, default 1s                        |
| cluster-nodes         | (none)  | String  | Cluster ip and port, not empty when redis-mode is cluster, such as:10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000 |
| command               | (none)  | String  | Corresponds to the redis command above                       |
| redis-mode            | (none)  | Integer | mode type： single cluster                                   |
| lookup.cache.max-rows | -1      | Integer | Query cache size, reduce the query for redis duplicate keys  |
| lookup.cache.ttl      | -1      | Integer | Query cache expiration time, in seconds. The condition for enabling query cache is that neither max-rows nor ttl can be -1 |
| lookup.max-retries    | 1       | Integer | Number of retries on failed query                            |
| sink.cache.max-rows   | -1      | Integer | Write cache size, reduce repeated writing of the same key and value to redis |
| sink.cache.ttl        | -1      | Integer | Write cache expiration time, in seconds, the condition for enabling the cache is that neither max-rows nor ttl can be -1 |
| sink.max-retries      | 1       | Integer | Number of retries for write failures                         |
| sink.parallelism      | (none)  | Integer | Number of concurrent writes                                  |



##### Additional connection parameters when the cluster type is sentinel:

| Field              | Default | Type   | Description |
| ------------------ | ------- | ------ | ----------- |
| master.name        | (none)  | String | master name |
| sentinels.info     | (none)  | String |             |
| sentinels.password | none)   | String |             |



### Usage example:
- ##### Dimension table query：
```
create table sink_redis(name varchar, level varchar, age varchar) with ( 'connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','command'='hset');

-- Insert data into redis first, which is equivalent to the redis command: hset 3 3 100 --
insert into sink_redis select * from (values ('3', '3', '100'));
                
create table dim_table (name varchar, level varchar, age varchar) with ('connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single', 'password'='*****','command'='hget', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'lookup.max-retries'='3');
    
-- Randomly generate data within 10 as data source --
-- One of the data will be: username = 3 level = 3, which will be associated with the data inserted above -- 
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
-- The line where username is 3 will be associated with the value in redis, and the output will be: 3,3,100
```



- ##### DataStream query method<br>

  Sample code path:  src/test/java/org.apache.flink.streaming.connectors.redis.datastream.DataStreamTest.java<br>
  hset example, equivalent to redis command: *hset tom math 150*

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



- ##### redis-cluster write example <br>

  Sample code path:  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLTest.java<br>
  set example, equivalent to redis command： *set test test11*

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



### Development and testing environment

ide: IntelliJ IDEA 

code format: google-java-format + Save Actions

code check: CheckStyle

flink 1.13

jdk1.8

Switch to branch flink1.12 if you need flink11.12 support

