# KafkaOffsetToolsWithTime通过时间获取Kafka topic的offset
<div align="center">
  <p>
    <a href="https://github.com/zhangkuantian/KafkaOffsetToolsWithTime">
        <img alt="stars" src="https://img.shields.io/github/stars/zhangkuantian/KafkaOffsetToolsWithTime?style=social" />
    </a>
    <a href="https://github.com/zhangkuantian/KafkaOffsetToolsWithTime">
        <img alt="forks" src="https://img.shields.io/github/forks/zhangkuantian/KafkaOffsetToolsWithTime?style=social" />
    </a>
  </p>
</div>

[![Star History Chart](https://api.star-history.com/svg?repos=zhangkuantian/KafkaOffsetToolsWithTime)](https://star-history.com/#zhangkuantian/KafkaOffsetToolsWithTime)

### 1、获取指定时间戳的Offset
执行命令格式如下:

```
sh getOffset.sh -gid <group.id> -server <bootstrap.server> -stime <starttime> -topic <topic>
```
参数说明:


参数名|含义
---|---
gid|kafka的group.id
server|kafka的bootstrap.server
stime|开始时间
topic|topic名称

getOffset.sh的内容:

```
#!/bin/base
java -cp ./olap_activity-1.0-SNAPSHOT-jar-with-dependencies.jar com.lucas_hust.kafka.tools.LoadOffsetWithTime $1 $2 $3 $4 $5 $6 $7 $8
```

### 2、获取指定时间戳到当前时间的数据
执行命令格式如下:

```
sh loadDataFromTime.sh -gid <group.id> -server <bootstrap.server> -stime <starttime> -topic <topic>
```

参数说明:

参数名|含义
---|---
gid|kafka的group.id
server|kafka的bootstrap.server
stime|开始时间
topic|topic名称

loadDataFromTime.sh 的内容:

```
#!/bin/base
java -cp ./olap_activity-1.0-SNAPSHOT-jar-with-dependencies.jar com.lucas_hust.kafka.tools.ConsumerKafkaFromTime $1 $2 $3 $4 $5 $6 $7 $8
```

### 3、获取指定时间戳范围内的开始offset、结束offset及数据
执行命令格式如下:

```
sh loadDataFromTime.sh -gid <group.id> -server <bootstrap.server> -stime <starttime> -topic <topic> -etime <endtime>
```

参数说明:

参数名|含义
---|---
gid|kafka的group.id
server|kafka的bootstrap.server
stime|开始时间
topic|topic名称
etime|结束时间


loadDataBetweenTime.sh的内容:

```
#!/bin/base
java -cp ./olap_activity-1.0-SNAPSHOT-jar-with-dependencies.jar com.lucas_hust.kafka.tools.ConsumerKafkaBetweenTime $1 $2 $3 $4 $5 $6 $7 $8 $9 ${10}
```

