[EN](README-en.md)
# 1 项目介绍

基于[bahir-flink](https://github.com/apache/bahir-flink.git)二次开发，相对bahir调整的内容有：
```
1.使用Lettuce替换Jedis,同步读写改为异步读写，大幅度提升了性能 
2.增加了Table/SQL API，增加select/维表join查询支持
3.增加关联查询缓存(支持增量与全量)
4.增加支持整行保存功能，用于多字段的维表关联查询
5.增加限流功能，用于Flink SQL在线调试功能
6.增加支持Flink高版本（包括1.12,1.13,1.14+）
7.统一过期策略等
8.支持flink cdc删除及其它RowKind.DELETE
9.支持select查询
```

因bahir使用的flink接口版本较老，所以改动较大，开发过程中参考了腾讯云与阿里云两家产商的流计算产品，取两家之长，并增加了更丰富的功能。

注：redis不支持两段提交无法实现刚好一次语义。



# 2 使用方法: 
## 2.1 工程直接引用
项目依赖Lettuce(6.2.1)及netty-transport-native-epoll(4.1.82.Final),如flink环境有这两个包,则使用flink-connector-redis-1.4.3.jar，
否则使用flink-connector-redis-1.4.3-jar-with-dependencies.jar。
<br/>
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <!-- 没有单独引入项目依赖Lettuce netty-transport-native-epoll依赖时 -->
    <!--            <classifier>jar-with-dependencies</classifier>-->
    <version>1.4.3</version>
</dependency>
```
## 2.2 自行打包
打包命令： mvn package,将生成的包放入flink lib中即可，无需其它设置。

## 2.3 使用示例 
```
-- 创建redis表示例
create table redis_table (name varchar, age int) 
  with ('connector'='redis', 'host'='10.11.69.176', 'port'='6379','password'='test123', 
  'redis-mode'='single','command'='set');
-- 写入  
  insert into redis_table select * from (values('test', 1));

-- 查询  
  insert into redis_table select name,age + 1 from redis_table /*+ options('scan.key'='test') */
  
create table gen_table (age int , level int, proctime as procTime()) with ('connector'='datagen','fields.age.kind' = 'sequence',
 'fields.age.start' = '2','fields.age.end' = '2','fields.level.kind' = 'sequence','fields.level.start' = '10','fields.level.end' = '10'); 

-- 关联查询 
insert into redis_table select 'test', j.age + 10 from gen_table s left join redis_table  for system_time as of proctime as j
on j.name = 'test'

```


# 3 参数说明：
## 3.1 主要参数：

| 字段                    | 默认值    | 类型      | 说明                                                                                               |
|-----------------------|--------|---------|--------------------------------------------------------------------------------------------------|
| connector             | (none) | String  | `redis`                                                                                          |
| host                  | (none) | String  | Redis IP                                                                                         |
| port                  | 6379   | Integer | Redis 端口                                                                                         |
| password              | null   | String  | 如果没有设置，则为 null                                                                                   |
| database              | 0      | Integer | 默认使用 db0                                                                                         |
| timeout               | 2000   | Integer | 连接超时时间，单位 ms，默认 1s                                                                               |
| cluster-nodes         | (none) | String  | 集群ip与端口，当redis-mode为cluster时不为空，如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000          |
| command               | (none) | String  | 对应上文中的redis命令                                                                                    |
| redis-mode            | (none) | Integer | mode类型： single cluster sentinel                                                                  |
| lookup.cache.max-rows | -1     | Integer | 查询缓存大小,减少对redis重复key的查询                                                                          |
| lookup.cache.ttl      | -1     | Integer | 查询缓存过期时间，单位为秒， 开启查询缓存条件是max-rows与ttl都不能为-1                                                       |
| lookup.cache.load-all | false  | Boolean | 开启全量缓存,当命令为hget时,将从redis map查询出所有元素并保存到cache中,用于解决缓存穿透问题                                         |
| max.retries           | 1      | Integer | 写入/查询失败重试次数                                                                                      |
| value.data.structure  | column | String  | column: value值来自某一字段 (如, set: key值取自DDL定义的第一个字段, value值取自第二个字段)<br/> row: 将整行内容保存至value并以'\01'分割 |
| set.if.absent         | false  | Boolean | 在key不存在时才写入,只对set hset有效                                                                         |
| io.pool.size          | (none) | Integer | Lettuce内netty的io线程池大小,默认情况下该值为当前JVM可用线程数，并且大于2                                                   |
| event.pool.size       | (none) | Integer | Lettuce内netty的event线程池大小 ,默认情况下该值为当前JVM可用线程数，并且大于2                                               |
| scan.key              | (none) | String  | 查询时redis key                                                                                     |
| scan.addition.key     | (none) | String  | 查询时限定redis key,如map结构时的hashfield                                                                 |
| scan.range.start      | (none) | Integer | 查询list结构时指定lrange start                                                                          |
| scan.range.stop       | (none) | Integer | 查询list结构时指定lrange start                                                                          |
| scan.count            | (none) | Integer | 查询set结构时指定srandmember count                                                                      |
| zset.zremrangeby      | (none) | String  | 执行zadd之后，是否执行zremrangeby，取值：SCORE、LEX、RANK                                                       |
| audit.log             | false  | Boolean | 打开sink日志                                                                                         |

### 3.1.1 command值与redis命令对应关系：

| command值              | 写入                    | 查询             | 维表关联    | 删除(Flink CDC等产生的RowKind.delete)  |
|-----------------------|-----------------------|----------------|---------|----------------------------------|
| set                   | set                   | get            | get     | del                              |
| hset                  | hset                  | hget           | hget    | hdel                             |
| get                   | set                   | get            | get     | del                              |
| hset                  | hset                  | hget           | hget    | hdel                             | 
| rpush                 | rpush                 | lrange         |         |                                  |
| lpush                 | lpush                 | lrange         |         |                                  |
| incrBy incrByFloat    | incrBy incrByFloat    | get            | get     | 写入相对值，如:incrby 2 -> incryby -2   | 
| hincrBy hincryByFloat | hincrBy hincryByFloat | hget           | hget    | 写入相对值，如:hincrby 2 -> hincryby -2 |
| zincrby               | zincrby               | zscore         | zscore  | 写入相对值，如:zincrby 2 -> zincryby -2 |
| sadd                  | sadd                  | srandmember 10 |         | srem                             |   
| zadd                  | zadd                  | zscore         | zscore  | zrem                             |   
| pfadd(hyperloglog)    | pfadd(hyperloglog)    |                |         |                                  |   
| publish               | publish               |                |         |                                  |
| zrem                  | zrem                  | zscore         | zscore  |                                  |
| srem                  | srem                  | srandmember 10 |         |                                  |
| del                   | del                   | get            | get     |                                  |
| hdel                  | hdel                  | hget           | hget    |                                  |
| decrBy                | decrBy                | get            | get     |                                  | 

注：**为空表示不支持**

### 3.1.2 value.data.structure = column(默认)
无需通过primary key来映射redis中的Key，直接由ddl中的字段顺序来决定Key,如：

```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, score为value.
```
### 3.1.3 value.data.structure = row
整行内容保存至value并以'\01'分割
```
create table sink_redis(username VARCHAR, passport VARCHAR)  with ('command'='set') 
其中username为key, username\01passport为value.

create table sink_redis(name VARCHAR, subject VARCHAR, score VARCHAR)  with ('command'='hset') 
其中name为map结构的key, subject为field, name\01subject\01score为value.
```

## 3.2 sink时ttl相关参数

| Field              | Default | Type    | Description                                                       |
|--------------------|---------|---------|-------------------------------------------------------------------|
| ttl                | (none)  | Integer | key过期时间(秒),每次sink时会设置ttl                                          |
| ttl.on.time        | (none)  | String  | key的过期时间点,格式为LocalTime.toString(), eg: 10:00 12:12:01,当ttl未配置时才生效 |
| ttl.key.not.absent | false   | boolean | 与ttl一起使用,当key不存在时才设置ttl                                           |

## 3.3 sql ttl元数列使用

支持 `ttl` 元数据列，允许在运行时将TTL值动态应用于 Redis key。

### 使用方法

#### 1. 在 Flink SQL 中定义包含 TTL 元数据的表

```sql
CREATE TABLE sink_table (
    key STRING,
    value STRING,
    ttl INT METADATA  -- TTL 元数据列
) WITH (
    'connector' = 'redis',
    'host' = 'localhost',
    'port' = '6379',
    'command' = 'set'
);
```

#### 2. 使用 SQL 插入带 TTL 的数据

```sql
INSERT INTO sink_table 
SELECT 
    user_id,
    user_data,
    CASE 
        WHEN user_type = 'premium' THEN 3600  -- 高级用户数据保留 1 小时
        ELSE 600  -- 普通用户数据保留 10 分钟
    END as ttl
FROM user_events;
```

### 注意事项

1. TTL 元数据列必须是 INT 类型  
2. 当同时配置了连接器级别的 TTL 参数和元数据列 TTL 时，元数据列的值具有更高优先级  
3. 如果元数据列中的 TTL 值为 NULL 或无效，则使用连接器默认的 TTL 设置  
4. TTL 值单位为秒

## 3.3 在线调试SQL时，用于限制sink资源使用的参数:

| Field                 | Default | Type    | Description                             |
|-----------------------|---------|---------|-----------------------------------------|
| sink.limit            | false   | Boolean | 是否打开限制                                  |
| sink.limit.max-num    | 10000   | Integer | taskmanager内每个slot可以写的最大数据量             |
| sink.limit.interval   | 100     | String  | taskmanager内每个slot写入数据间隔   milliseconds |
| sink.limit.max-online | 30 * 60 * 1000L   | Long    | taskmanager内每个slot最大在线时间, milliseconds  |


## 3.4 集群类型为sentinel时额外连接参数:

| 字段                 | 默认值 | 类型   | 说明                                                      |
|--------------------| ------ | ------ |---------------------------------------------------------|
| master.name        | (none) | String | 主名                                                      |
| sentinels.info     | (none) | String | 如：10.11.80.147:7000,10.11.80.147:7001,10.11.80.147:8000 |
| sentinels.password | (none) | String | sentinel进程密码                                            |

# 4 数据类型转换

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


# 5 使用示例:
- ## 5.1 维表查询：
```
create table sink_redis(name varchar, level varchar, age varchar) with ( 'connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single','password'='******','command'='hset');

-- 先在redis中插入数据,相当于redis命令： hset 3 3 100 --
insert into sink_redis select * from (values ('3', '3', '100'));
                
create table dim_table (name varchar, level varchar, age varchar) with ('connector'='redis', 'host'='10.11.80.147','port'='7001', 'redis-mode'='single', 'password'='*****','command'='hget', 'maxIdle'='2', 'minIdle'='1', 'lookup.cache.max-rows'='10', 'lookup.cache.ttl'='10', 'max-retries'='3');
    
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

- ## 5.2 多字段的维表关联查询
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
create table source_table(uid VARCHAR, username VARCHAR, proc_time as ProcTime()) with ('connector'='datagen', 'fields.uid.kind'='sequence', 'fields.uid.start'='1', 'fields.uid.end'='2')

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

- ## 5.3 DataStream查询方式<br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.datastream.DataStreamTest.java<br>
  hset示例，相当于redis命令：*hset tom math 150*

```
        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());
        configuration.setInteger(TTL, 10);

        RedisSinkMapper redisMapper = new RowRedisSinkMapper(RedisCommand.HSET, configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BinaryRowData binaryRowData = new BinaryRowData(3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeString(0, StringData.fromString("tom"));
        binaryRowWriter.writeString(1, StringData.fromString("math"));
        binaryRowWriter.writeString(2, StringData.fromString("152"));

        DataStream<BinaryRowData> dataStream = env.fromElements(binaryRowData, binaryRowData);

        List<String> columnNames = Arrays.asList("name", "subject", "scope");
        List<DataType> columnDataTypes =
                Arrays.asList(DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING());
        ResolvedSchema resolvedSchema = ResolvedSchema.physical(columnNames, columnDataTypes);

        FlinkConfigBase conf =
                new FlinkSingleConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .build();

        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, resolvedSchema, configuration);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");
```



- ## 5.4 redis-cluster写入示例 <br>

  示例代码路径:  src/test/java/org.apache.flink.streaming.connectors.redis.table.SQLInsertTest.java<br>
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

# 6 解决问题联系我

![img.png](img.png)


# 7 开发环境

ide: IntelliJ IDEA 

code format: google-java-format + Save Actions

flink 1.12/1.13/1.14+

jdk1.8 Lettuce 6.2.1

# 8 贡献
Pull Request需要提交至dev分支</br>
提交前请使用mvn spotless:apply进行代码格式化,然后使用maven package打包确认所有测试用例能通过。

# 9 flink 1.12支持
请切换到分支flink-1.12(注：1.12使用jedis)
```
<dependency>
    <groupId>io.github.jeff-zou</groupId>
    <artifactId>flink-connector-redis</artifactId>
    <version>1.1.1-1.12</version>
</dependency>
```
