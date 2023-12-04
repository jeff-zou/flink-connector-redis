[中文](README.md)

### Introduction

Based on the  [bahir-flink](https://github.com/apache/bahir-flink.git)，the contents adjusted relative to bahir include: 
1.  Use Lettuce to replace Jedis, change sync IO to async IO<br/>
2. Add Table/SQL API, Add dimension table query support<br/>
3. Increase query cache (support incremental and full)<br/>
4. Added support for saving the entire row, which is used for multi-field dimension table associated query<br/>
5. Add current limiting function for Flink SQL online debugging function<br/>
6. Added support for Flink higher versions (including 1.12, 1.13, 1.14+)<br/>
7. Unify or increase other functions such as expiration policy, number of concurrent writes, etc. <br/>

Because the version of the flink interface used by bahir is relatively old, the changes are relatively large. During the development process, the stream computing products of Tencent Cloud and Alibaba Cloud were referenced, taking the advantages of the two, and adding richer functions.

### Redis Command
The operation commands corresponding to the supported functions of redis are:

| insert                         | Dimension table query |
| ------------------------------ |-----------------------|
| set                            | get                   |
| hset                           | hget                  |
| rpush lpush                    |                       |
| incrBy decrBy hincrBy  zincrby |                       |
| sadd zadd pfadd(hyperloglog)   |                       |
| publish                        |                       |
| zrem decrby srem               |                       |
| del hdel                       |                       |


### Instructions: 

After executing mvn package -DskipTests on the command line, import the generated package flink-connector-redis-1.3.1.jar into flink lib, no other settings are required.

Development environment engineering direct reference:

```
<dependency>
  <groupId>io.github.jeff-zou</groupId>
  <artifactId>flink-connector-redis</artifactId>
  <version>1.3.1</version>
  <classifier>jar-with-dependencies</classifier>
</dependency>
```



### Instructions for use：
#### value.data.structure = column
There is no need to map the key in redis through the primary key, the key is directly determined by the order of the fields in the ddl, such as:

```
#Where username is the key and passport is the value.
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 

#The name is the key of the map structure, the subject is the field, and the score is the value.
create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
```

#### value.data.structure = row
value is taken from the entire row, separated by '\01'
```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
#key: username , value: username\01passport.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
key: name, field:subject, value: name\01subject\01score.
```

##### with parameter description:

| Field                | Default | Type    | Description                                                                                                                                                                                        |
|----------------------|---------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| connector            | (none)  | String  | `redis`                                                                                                                                                                                            |
| host                 | (none)  | String  | Redis IP                                                                                                                                                                                           |
| port                 | 6379    | Integer | Redis port                                                                                                                                                                                         |
| password             | null    | String  | null if not set                                                                                                                                                                                    |
| database             | 0       | Integer | db0 is used by default                                                                                                                                                                             |
| timeout              | 2000    | Integer | Connection timeout, in ms, default 1s                                                                                                                                                              |
| cluster-nodes        | (none)  | String  | Cluster ip and port, not empty when redis-mode is cluster, such as:10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000                                                                           |
| command              | (none)  | String  | Corresponds to the redis command above                                                                                                                                                             |
| redis-mode           | (none)  | Integer | mode type： single cluster                                                                                                                                                                          |
| lookup.cache.max-rows | -1      | Integer | Query cache size, reduce the query for redis duplicate keys                                                                                                                                        |
| lookup.cache.ttl     | -1      | Integer | Query cache expiration time, in seconds. The condition for enabling query cache is that neither max-rows nor ttl can be -1                                                                         |
| lookup.max-retries   | 1       | Integer | Number of retries on failed query                                                                                                                                                                  |
| lookup.cache.load-all | false   | Boolean | when command is hget, query all elements from redis map to cache,help to resolve cache penetration issues                                                                                          |
| sink.max-retries     | 1       | Integer | Number of retries for write failures                                                                                                                                                               |
| value.data.structure      | column  | String  | column: The value will come from a field (for example, set: key is the first field defined by DDL, and value is the second field)<br/> row: value is taken from the entire row, separated by '\01' |
| set.if.absent         | false  | Boolean | set/hset only when the key absent                                                                                                                                                                  |


##### sink with ttl parameters

| Field              | Default | Type    | Description                                                                                          |
|--------------------|---------|---------|------------------------------------------------------------------------------------------------------|
| ttl                | (none)  | Integer | key expiration time (seconds), each time sink will set the ttl                                       |
| ttl.on.time        | (none)  | String  | The expiration time of the key in LocalTime.toString(), eg: 10:00 12:12:01, if ttl is not configured |
| ttl.key.not.absent | false   | boolean | Used with ttl, which is set when the key doesn't exist                                                                              |


##### Additional sink parameters when u debugging sql online which need to limit the resource usage:

| Field                 | Default | Type    | Description                      |
|-----------------------|---------|---------|----------------------------------|
| sink.limit            | false   | Boolean | if open the limit for sink       |
| sink.limit.max-num    | 10000   | Integer | the max num of writes per thread |
| sink.limit.interval   | 100  | String  |  the millisecond interval between each write  per thread                              |
| sink.limit.max-online | 30 * 60 * 1000L   | Long    | the max online milliseconds   per thread                                  |

##### Additional connection parameters when the cluster type is sentinel:

| Field              | Default | Type   | Description |
| ------------------ | ------- | ------ | ----------- |
| master.name        | (none)  | String | master name |
| sentinels.info     | (none)  | String |             |
| sentinels.password | none)   | String |             |

### Data Type Converter

| flink type | redis row converter                                          |
| ---------- | ------------------------------------------------------------ |
| CHAR       | String                                                       |
| VARCHAR    | String                                                       |
| String     | String                                                       |
| BOOLEAN    | String String.valueOf(boolean val) <br/>boolean Boolean.valueOf(String str) |
| BINARY     | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str) |
| VARBINARY  | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str) |
| DECIMAL    | String  BigDecimal.toString <br/>DecimalData DecimalData.fromBigDecimal(new BigDecimal(String str),int precision, int scale) |
| TINYINT    | String String.valueOf(byte val)  <br/>byte Byte.valueOf(String str) |
| SMALLINT   | String String.valueOf(short val) <br/>short Short.valueOf(String str) |
| INTEGER    | String String.valueOf(int val)  <br/>int Integer.valueOf(String str) |
| DATE       | String the day from epoch as int <br/>date show as 2022-01-01 |
| TIME       | String the millisecond from 0'clock as int <br/>time show as 04:04:01.023 |
| BIGINT     | String String.valueOf(long val) <br/>long Long.valueOf(String str) |
| FLOAT      | String String.valueOf(float val) <br/>float Float.valueOf(String str) |
| DOUBLE     | String String.valueOf(double val) <br/>double Double.valueOf(String str) |
| TIMESTAMP  | String the millisecond from epoch as long <br/>timestamp TimeStampData.fromEpochMillis(Long.valueOf(String str)) |


### 

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

- #### Muti-field dimension table query
In many cases, dimension tables have multiple fields. This example shows how to use 'value.data.structure'='row' to write multiple fields and associative queries.
```
-- init data in redis --
create table sink_redis(uid VARCHAR, score double, score2 double ) with ( 'connector'='redis', 'host'='10.11.69.176','port'='6379', 'redis-mode'='single','password'='***','command'='SET', 'value.data.structure'='row')
insert into sink_redis select * from (values ('1', 10.3, 10.1))
-- 'value.data.structure'='row':value is taken from the entire row and separated by '\01' 
-- the value in redis will be: "1\x0110.3\x0110.1" --
-- init data in redis end --

-- create join table --
create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis

-- create result table --
create table result_table(uid VARCHAR, username VARCHAR, score double, score2 double) with ('connector'='print')

-- create source table --
create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')

-- query --
insert
	into
	result_table
select
	s.uid,
	s.username,
	j.score,
	j.score2
from
	source_table as s
join join_table for system_time as of s.proc_time as j on
	j.uid = s.uid
	
result:
2> +I[2, 1e0fe885a2990edd7f13dd0b81f923713182d5c559b21eff6bda3960cba8df27c69a3c0f26466efaface8976a2e16d9f68b3, null, null]
1> +I[1, 30182e00eca2bff6e00a2d5331e8857a087792918c4379155b635a3cf42a53a1b8f3be7feb00b0c63c556641423be5537476, 10.3, 10.1]
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

RedisSinkOptions redisSinkOptions =
        new RedisSinkOptions.Builder().setMaxRetryTimes(3).build();
FlinkConfigBase conf =
        new FlinkSingleConfig.Builder()
                .setHost(REDIS_HOST)
                .setPort(REDIS_PORT)
                .setPassword(REDIS_PASSWORD)
                .build();

RedisSinkFunction redisSinkFunction =
        new RedisSinkFunction<>(conf, redisMapper, redisSinkOptions, resolvedSchema);

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

flink 1.12/1.13/1.14+

jdk1.8 Lettuce 6.2.1

### Switch to branch flink-1.12 if you need flink1.12 support（note: use jedis）
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.1.1-1.12</version>
</dependency>
```
