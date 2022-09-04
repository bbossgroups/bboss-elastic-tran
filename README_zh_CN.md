# Elastic Tran Bboss

数据交换模块:

![](https://esdoc.bbossgroups.com/images/datasyn.png)
使用文档：<https://esdoc.bbossgroups.com/#/db-es-tool>

Bboss is a good elasticsearch Java rest client. It operates and accesses elasticsearch like mybatis to relation database.

<https://esdoc.bbossgroups.com/#/README>

# Environmental requirements

JDK requirement: JDK 1.8+
Elasticsearch version requirements: 1.X,2.X,5.X,6.X,7.x,8.x,+
# Build from source code
First Get source code from https://github.com/bbossgroups/bboss-elastic-tran

Then change to cmd window under directory bboss-elastic-tran and run gradle build command：

```
gradle publishToMavenLocal
```


Build from source code guide:

<https://esdoc.bbossgroups.com/#/bboss-build>

# How to use Elastic Tran Bboss.

First add the maven dependency of BBoss to your pom.xml:

```xml
       <dependency>
            <groupId>com.bbossgroups.plugins</groupId>
            <artifactId>datatran</artifactId>
            <version>6.7.3</version>
        </dependency>
```
and more see:

https://esdoc.bbossgroups.com/#/db-es-tool




## elasticsearch技术交流群:**166471282**

## elasticsearch微信公众号:bbossgroup   



## License

The BBoss Framework is released under version of 2.0 the [Apache License][].

[Apache License]: http://www.apache.org/licenses/LICENSE-2.0