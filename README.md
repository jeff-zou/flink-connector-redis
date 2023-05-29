[EN](README-en.md)
### 项目介绍

基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，相对bahir调整的内容有：
```
1.使用Lettuce替换Jedis,同步读写改为异步读写，大幅度提升了性能 
2.增加了Table/SQL API，增加维表查询支持
3.增加查询缓存(支持增量与全量)
4.增加支持整行保存功能，用于多字段的维表关联查询
5.增加限流功能，用于Flink SQL在线调试功能
6.增加支持Flink高版本（包括1.12,1.13,1.14+）
7.统一过期策略等。
```

因bahir使用的flink接口版本较老，所以改动较大，开发过程中参考了腾讯云与阿里云两家产商的流计算产品，取两家之长，并增加了更丰富的功能。
### 支持功能对应redis的操作命令有：

| 插入                                                      | 维表查询 |
|---------------------------------------------------------|------|
| set                                                     | get  |
| hset                                                    | hget |
| rpush lpush                                             |      |
| incrBy incrByFloat decrBy hincrBy hincryByFloat zincrby |      |
| sadd zadd pfadd(hyperloglog)                            |      |
| publish                                                 |      |
| zrem srem                                               |      |
| del hdel                                                |      |



### 使用方法: 
1.打包命令： mvn package -DskipTests</br>
2.将生成的包放入flink lib中即可，无需其它设置。

<br/>
  项目依赖Lettuce(6.2.1)及netty-transport-native-epoll(4.1.82.Final),如flink环境有这两个包,则使用flink-connector-redis-1.2.6.jar，
否则使用flink-connector-redis-1.2.6-jar-with-dependencies.jar。
<br/>

开发环境工程直接引用：

```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.2.6</version>
</dependency>
```



### 使用说明：

#### value.data.structure = column(默认)
无需通过primary key来映射redis中的Key，直接由ddl中的字段顺序来决定Key,如：

```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, score为value.
```
#### value.data.structure = row
整行内容保存至value并以'\01'分割
```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, username\01passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, name\01subject\01score为value.
```


#### with参数说明：

| 字段                    | 默认值    | 类型    | 说明                                                                                               |
|-----------------------|--------| ------- |--------------------------------------------------------------------------------------------------|
| connector             | (none) | String  | `redis`                                                                                          |
| host                  | (none) | String  | Redis IP                                                                                         |
| port                  | 6379   | Integer | Redis 端口                                                                                         |
| password              | null   | String  | 如果没有设置，则为 null                                                                                   |
| database              | 0      | Integer | 默认使用 db0                                                                                         |
| ttl                   | (none) | Integer | sink时key过期时间(秒)                                                                                  |
| timeout               | 2000   | Integer | 连接超时时间，单位 ms，默认 1s                                                                               |
| cluster-nodes         | (none) | String  | 集群ip与端口，当redis-mode为cluster时不为空，如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000          |
| command               | (none) | String  | 对应上文中的redis命令                                                                                    |
| redis-mode            | (none) | Integer | mode类型： single cluster sentinel                                                                  |
| lookup.cache.max-rows | -1     | Integer | 查询缓存大小,减少对redis重复key的查询                                                                          |
| lookup.cache.ttl      | -1     | Integer | 查询缓存过期时间，单位为秒， 开启查询缓存条件是max-rows与ttl都不能为-1                                                       |
| lookup.max-retries    | 1      | Integer | 查询失败重试次数                                                                                         |
| lookup.cache.load-all | false  | Boolean | 开启全量缓存,当命令为hget时,将从redis map查询出所有元素并保存到cache中,用于解决缓存穿透问题                                         |
| sink.max-retries      | 1      | Integer | 写入失败重试次数                                                                                         |
| sink.parallelism      | (none) | Integer | 写入并发数                                                                                            |
| value.data.structure  | column | String  | column: value值来自某一字段 (如, set: key值取自DDL定义的第一个字段, value值取自第二个字段)<br/> row: 将整行内容保存至value并以'\01'分割 |
| expire.on.time        | (none) | String  | 指定key的过期时间点,格式为LocalTime, eg: 10:00 12:12:01,ttl字段将无效                                            |

##### 在线调试SQL时，用于限制sink资源使用的参数:

| Field                 | Default | Type    | Description                            |
|-----------------------|---------|---------|----------------------------------------|
| sink.limit            | false   | Boolean | 限制开头                                   |
| sink.limit.max-num    | 10000   | Integer | taskmanager内每个slot可以写的最大数据量            |
| sink.limit.interval   | 100     | String  | taskmanager内每个slot写入数据间隔   milliseconds            |
| sink.limit.max-online | 30 * 60 * 1000L   | Long    | taskmanager内每个slot最大在线时间, milliseconds |


#### 集群类型为sentinel时额外连接参数:

| 字段                 | 默认值 | 类型   | 说明                                                      |
|--------------------| ------ | ------ |---------------------------------------------------------|
| master.name        | (none) | String | 主名                                                      |
| sentinels.info     | (none) | String | 如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000 |
| sentinels.password | (none) | String | sentinel进程密码                                            |

### 数据类型转换

| flink type   | redis row converter                                          |
| ------------ | ------------------------------------------------------------ |
| CHAR         | String                                                       |
| VARCHAR      | String                                                       |
| String       | String                                                       |
| BOOLEAN      | String String.valueOf(boolean val) <br/>boolean Boolean.valueOf(String str) |
| BINARY       | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)             |
| VARBINARY    | String Base64.getEncoder().encodeToString  <br/>byte[]   Base64.getDecoder().decode(String str)                 |
| DECIMAL      | String  BigDecimal.toString <br/>DecimalData DecimalData.fromBigDecimal(new BigDecimal(String str),int precision, int scale)                               |
| TINYINT      | String String.valueOf(byte val)  <br/>byte Byte.valueOf(String str)                         |
| SMALLINT     | String String.valueOf(short val) <br/>short Short.valueOf(String str)                          |
| INTEGER      | String String.valueOf(int val)  <br/>int Integer.valueOf(String str)                          |
| DATE         | String the day from epoch as int <br/>date show as 2022-01-01                             |
| TIME         | String the millisecond from 0'clock as int <br/>time show as 04:04:01.023                         |
| BIGINT       | String String.valueOf(long val) <br/>long Long.valueOf(String str)                            |
| FLOAT        | String String.valueOf(float val) <br/>float Float.valueOf(String str)                            |
| DOUBLE       | String String.valueOf(double val) <br/>double Double.valueOf(String str)                           |
| TIMESTAMP | String the millisecond from epoch as long <br/>timestamp TimeStampData.fromEpochMillis(Long.valueOf(String str))                    |


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

- #### 多字段的维表关联查询
很多情况维表有多个字段,本实例展示如何利用'value.data.structure'='row'写多字段并关联查询。
```
-- 创建表
create table sink_redis(uid VARCHAR,score double,score2 double )
with ( 'connector' = 'redis',
			'host' = '10.11.69.176',
			'port' = '6379',
			'redis-mode' = 'single',
			'password' = '****',
			'command' = 'SET',
			'value.data.structure' = 'row');  -- 'value.data.structure'='row':整行内容保存至value并以'\01'分割
-- 写入测试数据，score、score2为需要被关联查询出的两个维度
insert into sink_redis select * from (values ('1', 10.3, 10.1));

-- 在redis中，value的值为: "1\x0110.3\x0110.1" --
-- 写入结束 --

-- create join table --
create table join_table with ('command'='get', 'value.data.structure'='row') like sink_redis

-- create result table --
create table result_table(uid VARCHAR, username VARCHAR, score double, score2 double) with ('connector'='print')

-- create source table --
create table source_table(uid VARCHAR, username VARCHAR, proc_time as procTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')

-- 关联查询维表，获得维表的多个字段值 --
insert
	into
	result_table
select
	s.uid,
	s.username,
	j.score, -- 来自维表
	j.score2 -- 来自维表
from
	source_table as s
join join_table for system_time as of s.proc_time as j on
	j.uid = s.uid
	
result:
2> +I[2, 1e0fe885a2990edd7f13dd0b81f923713182d5c559b21eff6bda3960cba8df27c69a3c0f26466efaface8976a2e16d9f68b3, null, null]
1> +I[1, 30182e00eca2bff6e00a2d5331e8857a087792918c4379155b635a3cf42a53a1b8f3be7feb00b0c63c556641423be5537476, 10.3, 10.1]
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

flink 1.12/1.13/1.14+

jdk1.8 Lettuce 6.2.1

### 如果需要flink 1.12版本支持，请切换到分支flink-1.12(注：1.12使用jedis)
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.1.1-1.12</version>
</dependency>
```