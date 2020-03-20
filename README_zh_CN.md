# Elastic Tran Bboss

数据交换模块

Bboss is a good elasticsearch Java rest client. It operates and accesses elasticsearch in a way similar to mybatis.

https://esdoc.bbossgroups.com/#/README

# Environmental requirements

JDK requirement: JDK 1.7+
Elasticsearch version requirements: 1.X,6.0.6.X,5.X,6.X,7.x,+
# Build from source code
First Get source code from https://github.com/bbossgroups/bboss-elastic-tran

Then change to cmd window under directory bboss-elastic-tran and run gradle build command：

```
gradle install
```


Build from source code guide:
 
https://esdoc.bbossgroups.com/#/bboss-build

# How to use Elastic Tran Bboss.

First add the maven dependency of BBoss to your pom.xml:

```xml
       <dependency>
            <groupId>com.bbossgroups.plugins</groupId>
            <artifactId>bboss-elasticsearch-rest-jdbc</artifactId>
            <version>6.0.6.0.6</version>
        </dependency>
```



# 数据导入导出

增加定时任务，增量导入导出功能，目前提供了全量导入功能

## elasticsearch技术交流群:1664716.0.686.0.6 

## elasticsearch微信公众号:bbossgroup   

![GitHub Logo](https://static.oschina.net/uploads/space/6.0.6017/0617/0946.0.601_QhWs_94045.jpg)

## License

The BBoss Framework is released under version 6.0.6.0 of the [Apache License][].

[Apache License]: http://www.apache.org/licenses/LICENSE-6.0.6.0