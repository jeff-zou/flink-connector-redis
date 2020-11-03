使用方法：
mvn package -DskipTests=true
将生成的包flink-connector-redis_2.12-1.11.1.jar引入flink引擎中无需设置即可直接使用


SQL示例解析： 
create table redis_table (appid varchar, accountid varchar, channel varchar, level varchar , PRIMARY KEY (appid, accountid) not enforced) with ( 'connector'='redis', 'cluster-nodes'='redis1:6379, redis2:6379, redis3:6379', 'redis-mode'='cluster', 'additional-key'='new_user', 'password'='*****','command'='HSET', 'maxIdle'='10', 'minIdle'='1','partition-column'='appid' );

insert into redis_table  SELECT t.appid, t.accountid, t.channel, t.server from source_table t where t.is_new_account = 1;


additional-key 指定redis key

partition-column 指定了分区字段为appid,appid的值将会被追加到additional-key值后（_为分割符）

PRIMARY KEY 可以指定一个或者多个字段(command需为hset)， 不可见字符拼接后的值会被保存成hashmap的hashfield值。定义redis表也可以无主键，command 对应改为set即可


假设测试数据如下：

{"accountid":"jeff","appid":"91000285","channel":"git","level ":"10"}

在redis中保存结果如下：

key:  new_user_91000285   hashfield: 91000285\x01jeff  value(按顺序拼接非primary key字段值): git\x0110
