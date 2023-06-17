# Elastic Tran Bboss
bboss-datatran由 bboss 开源的数据采集同步ETL工具，提供数据采集、数据清洗转换处理和数据入库以及数据指标统计计算流批一体化处理功能。

​ bboss-datatran 数据同步作业直接采用java语言开发，小巧而精致，同时又可以采用java提供的所有功能和现有组件框架，随心所欲地处理和加工海量存量数据、实时增量数据，实现流批一体数据处理功能；可以根据数据规模及同步性能要求，按需配置和调整数据采集同步作业所需内存、工作线程、线程队列大小；可以将作业独立运行，亦可以将作业嵌入基于java开发的各种应用一起运行；提供了作业任务控制API、作业监控api，支持作业启动、暂停(pause)、继续（resume）、停止控制机制，可轻松定制一款属于自己的ETL管理工具。

工具可以灵活定制具备各种功能的数据采集统计作业

1) 只采集和处理数据作业

2) 采集和处理数据、指标统计计算混合作业

3) 采集数据只做指标统计计算作业

指标计算特点

1) 支持时间维度和非时间维度指标计算

2) 时间维度指标计算：支持指定统计时间窗口，单位到分钟级别

3) 一个指标支持多个维度和多个度量字段计算，多个维度字段值构造成指标的唯一指标key，支持有限基数key和无限基数key指标计算

4) 一个作业可以支持多种类型的指标，每种类型指标支持多个指标计算

![](https://esdoc.bbossgroups.com/images/datasyn.png)

使用文档：<https://esdoc.bbossgroups.com/#/db-es-tool>

Bboss is a good elasticsearch Java rest client and etl&stream metrics tool. It operates and accesses elasticsearch like mybatis to relation database.

<https://esdoc.bbossgroups.com/#/README>

# Environment requirements

JDK requirement: JDK 1.8+

Elasticsearch version requirements: 1.X,2.X,5.X,6.X,7.x,8.x,+

Spring booter 1.x,2.x,+

# Build from source code
First Get source code from 

https://gitee.com/bboss/bboss-elastic

https://gitee.com/bboss/bboss-elastic-tran

Then change to cmd window under directory bboss-elastic-tran and run gradle build command：

```
gradle publishToMavenLocal
```

Build from source code guide:

https://esdoc.bbossgroups.com/#/bboss-build

# How to use Elasticsearch BBoss.

First add the maven dependency of BBoss to your pom.xml:

```xml
       <dependency>
            <groupId>com.bbossgroups.plugins</groupId>
            <artifactId>datatran</artifactId>
            <version>7.0.0</version>
        </dependency>
```

## 技术交流群:166471282 

## 微信公众号:bbossgroup   


## License

The BBoss Framework is released under version 2.0 of the [Apache License][].

[Apache License]: http://www.apache.org/licenses/LICENSE-2.0