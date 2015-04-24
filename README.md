# 日志分析系统

## 系统架构

	本使用kafka，spark，hbase开发日志分析系统。
	
![architecture](/docs/images/architecture.png "architecture")

### 软件模块

 * Kafka：作为日志事件的消息系统，具有分布式，可分区，可冗余的消息服务功能。
 * Spark：使用spark stream功能，实时分析消息系统中的数据，完成计算分析工作。
 * Hbase：做为后端存储，存储spark计算结构，供其他系统进行调用
 
## 环境部署

### 软件版本
	
 * hadoop 版本 ： Hadoop相关软件如zookeeper、hadoop、hbase，使用的是cloudera的 cdh 5.2.0 版本。
 * Kafka ： 2.9.2-0.8.1.1
	
### 软件安装

a. 部署kafka
	
	tar -xzf kafka_2.9.2-0.8.1.1.tgz

b. 编辑kafka 配置文件
    
	config/server-1.properties:
	    broker.id=0
	    port=9093
	    log.dir=/tmp/kafka-logs
	
	config/server-2.properties:
	    broker.id=1
	    port=9093
	    log.dir=/tmp/kafka-logs
	
	config/server-3.properties:
	    broker.id=2
	    port=9093
	    log.dir=/tmp/kafka-logs

c. 启动kafka

	bin/kafka-server-start.sh config/server-1.properties &
	bin/kafka-server-start.sh config/server-2.properties &
	bin/kafka-server-start.sh config/server-3.properties &

d. 创建kafka topic
	
> bin/kafka-topics.sh --create --zookeeper 10.10.102.191:2181, 10.10.102.192:2181, 10.10.102.193:2181 --replication-factor 3 --partitions 1 --topic recsys

e. 查看是否创建成功

> bin/kafka-topics.sh --list --zookeeper localhost:2181

> bin/kafka-topics.sh --describe --zookeeper localhost:2181 --topic my-replicated-topic
Topic:my-replicated-topic	PartitionCount:1	ReplicationFactor:3	Configs:
	Topic: my-replicated-topic	Partition: 0	Leader: 1	Replicas: 1,2,0	Isr: 1,2,0



f. kafka启动测试

> bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test 
This is a message
This is another message

> bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic test --from-beginning
This is a message
This is another message

g. 注意事项
	
在开发程序的时候，producer客户端必须要配置上broker的host映射信息，即使你的程序中使用的都是ip地址。

## 项目开发

### 程序部署目录

	/libs
	* Logback包：logback-classic-1.1.2.jar，logback-core-1.1.2.jar
	* Kafka包（在kafka安装包lib目录中）
	/conf
	* Logback：logback.xml
	
	/webapps/recsys
	* index.html
	/
	* logcount-1.0.jar
	
### Spark_Streaming 处理数据
### HBase 保存数据

创建hbase表

	create ‘recsys_logs’,’f’

服务器端部署.服务器端启动了一个httpserver，该server需要将jar包中的html页面解压出来，所以先解压，后运行程序

	jar xvf recsys-1.0.jar


#### 系统运行

客户端
	
> java -Dlogback.configurationFile=./conf/logback.xml -classpath .:libs/*:logcount-1.0.jar com.wankun.logcount.kafka.TailService dest.log

 服务端

> spark-submit --class com.wankun.logcount.spark.LogStream --master spark://SparkMaster:7077 logcount-1.0.jar

	
### 注释



