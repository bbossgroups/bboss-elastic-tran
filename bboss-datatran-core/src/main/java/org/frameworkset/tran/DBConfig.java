package org.frameworkset.tran;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.orm.adapter.DBFactory;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 16:42
 * @author biaoping.yin
 * @version 1.0
 */
public class DBConfig {
	private String statusTableDML;



    private String statusHistoryTableDML;
	private Integer jdbcFetchSize;
	private String dbDriver;
	private String dbUrl;
	private String dbUser;

    

    @JsonIgnore
    private DataSource dataSource;
    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true
     *
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     *
     * 另外也可以在DBConf上进行设置，例如：
     * BConf tempConf = new DBConf();
     *         tempConf.setPoolname(ds.getDbname());
     *         tempConf.setDriver(ds.getDbdriver());
     *         tempConf.setJdbcurl( ds.getDburl());
     *         tempConf.setUsername(ds.getDbuser());
     *         tempConf.setPassword(ds.getDbpassword());
     *         tempConf.setValidationQuery(ds.getValidationQuery());
     *         //tempConf.setTxIsolationLevel("READ_COMMITTED");
     *         tempConf.setJndiName("jndi-"+ds.getDbname());
     *         PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
     *         int initialConnections = propertiesContainer.getIntProperty("initialConnections",5);
     *         tempConf.setInitialConnections(initialConnections);
     *         int minimumSize = propertiesContainer.getIntProperty("minimumSize",5);
     *         tempConf.setMinimumSize(minimumSize);
     *         int maximumSize = propertiesContainer.getIntProperty("maximumSize",10);
     *         tempConf.setMaximumSize(maximumSize);
     *         tempConf.setUsepool(true);
     *         tempConf.setExternal(false);
     *         tempConf.setEncryptdbinfo(false);
     *         boolean showsql = propertiesContainer.getBooleanProperty("showsql",true);
     *         tempConf.setShowsql(showsql);
     *         tempConf.setQueryfetchsize(null);
     *         tempConf.setEnableBalance(true);
     *         tempConf.setBalance(DBConf.BALANCE_RANDOM);
     *         return SQLManager.startPool(tempConf);
     */
    private boolean enableBalance;
    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true
     *
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     *
     * 另外也可以在DBConf上进行设置，例如：
     * BConf tempConf = new DBConf();
     *         tempConf.setPoolname(ds.getDbname());
     *         tempConf.setDriver(ds.getDbdriver());
     *         tempConf.setJdbcurl( ds.getDburl());
     *         tempConf.setUsername(ds.getDbuser());
     *         tempConf.setPassword(ds.getDbpassword());
     *         tempConf.setValidationQuery(ds.getValidationQuery());
     *         //tempConf.setTxIsolationLevel("READ_COMMITTED");
     *         tempConf.setJndiName("jndi-"+ds.getDbname());
     *         PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
     *         int initialConnections = propertiesContainer.getIntProperty("initialConnections",5);
     *         tempConf.setInitialConnections(initialConnections);
     *         int minimumSize = propertiesContainer.getIntProperty("minimumSize",5);
     *         tempConf.setMinimumSize(minimumSize);
     *         int maximumSize = propertiesContainer.getIntProperty("maximumSize",10);
     *         tempConf.setMaximumSize(maximumSize);
     *         tempConf.setUsepool(true);
     *         tempConf.setExternal(false);
     *         tempConf.setEncryptdbinfo(false);
     *         boolean showsql = propertiesContainer.getBooleanProperty("showsql",true);
     *         tempConf.setShowsql(showsql);
     *         tempConf.setQueryfetchsize(null);
     *         tempConf.setEnableBalance(true);
     *         tempConf.setBalance(DBConf.BALANCE_RANDOM);
     *         return SQLManager.startPool(tempConf);
     */
    private String balance = DBConf.BALANCE_ROUNDBIN;
    
    


	private boolean removeAbandoned;
	private int connectionTimeout = 5000;
	private int maxWait = 3000;
	private int maxIdleTime = 600;

	@JsonIgnore
	private String dbPassword;

	private int initSize = 10;
	private int minIdleSize = 10;
	private int maxSize = 20;
    public static String sqlitex_createStatusTableSQL = new StringBuilder().append("create table $statusTableName (ID varchar(100),")  //记录标识
					.append( "lasttime number(20),") //最后更新时间
					.append( "lastvalue number(20),")  //增量字段值，值可能是日期类型，也可能是数字类型
                    .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
                    .append( "lastvaluetype number(1),") //值类型 0-数字 1-日期 2-LocalDateTime
					.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
					.append( "filePath varchar(500) ,")  //日志文件路径
					.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径

					.append( "fileId varchar(500) ,")  //日志文件indoe标识
					.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
					.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
					.append( "PRIMARY KEY (ID))").toString();
    public static String sqlitex_createHistoryStatusTableSQL = new StringBuilder().append("create table $historyStatusTableName (ID varchar(100),")  //记录标识
					.append( "lasttime number(20),") //最后更新时间
					.append( "lastvalue number(20),")  //增量字段值，值可能是日期类型，也可能是数字类型
                    .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
					.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期 2-LocalDateTime
					.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
					.append( "filePath varchar(500) ,")  //日志文件路径
					.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径
					.append( "fileId varchar(500) ,")  //日志文件indoe标识
					.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
					.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
					.append( "statusId number(10)) ")  //状态表中使用的主键标识
//					.append( "PRIMARY KEY (ID))")
					.toString();
	public static final String mysql_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar(100) NOT NULL comment '唯一标识', lasttime bigint(20) NOT NULL comment '最后记录时间戳', " )
                            .append("lastvalue bigint(20) NOT NULL comment '最后记录时间戳',")
                            .append( "strLastValue varchar(2000) comment '增量字段值，存储字符串形式的增量值，比如LocalDateTime',")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
                            .append( "lastvaluetype int(1) NOT NULL comment '值类型 0-数字 1-日期 2-LocalDateTime',") //值类型 0-数字 1-日期 2-LocalDateTime
							.append( "status int(1)  comment '数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0',")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500)  comment '日志文件路径',")  //日志文件路径
							.append( "relativeParentDir varchar(500)  comment '日志文件子目录相对路径,relativeParentDir',")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar(500)  comment '日志文件indoe标识',")   //日志文件indoe标识
							.append( "jobId varchar(500)  comment '作业id',")   //作业id 6.7.7版本新增
							.append( "jobType varchar(500)  comment '作业输入插件类型',")   //作业输入插件类型 6.7.7版本新增
							.append( "PRIMARY KEY(ID)) comment '增量状态同步表' ENGINE=InnoDB").toString();
	public static final String oracle_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(20) NOT NULL,")
            .append(" lastvalue NUMBER(20) NOT NULL, " )
            .append( "strLastValue varchar2(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype NUMBER(1) NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0

							.append( "filePath varchar2(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar2(500) ,")   //日志文件indoe标识
							.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();
	public static final String dm_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(20) NOT NULL, lastvalue NUMBER(20) NOT NULL, " )
                            .append( "strLastValue varchar2(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
                            .append("lastvaluetype NUMBER(1) NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar2(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar2(500) ,")   //日志文件indoe标识
							.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();
	public static final String sqlserver_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName (ID varchar(100) NOT NULL,lasttime bigint NOT NULL,lastvalue bigint NOT NULL," )
                            .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
                            .append("lastvaluetype INT NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
							.append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar(500) ,")   //日志文件indoe标识
							.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();

    public static final String postgresql_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName (ID varchar(100) NOT NULL,lasttime bigint NOT NULL,lastvalue bigint NOT NULL," )
            .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype INT NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
            .append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
            .append( "filePath varchar(500) ,")  //日志文件路径
            .append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
            .append( "fileId varchar(500) ,")   //日志文件indoe标识
            .append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
            .append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
            .append( "primary key(ID))").toString();

	public static final String mysql_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar(100) NOT NULL comment '历史记录唯一标识', lasttime bigint(20) NOT NULL comment '最后同步时间戳', lastvalue bigint(20) NOT NULL comment '最后记录值', " )
            .append( "strLastValue varchar(2000) comment '增量字段值，存储字符串形式的增量值，比如LocalDateTime',")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype int(1) NOT NULL comment '值类型 0-数字 1-日期 2-LocalDateTime',") //值类型 0-数字 1-日期 2-LocalDateTime
			.append( "status int(1)  comment '数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0',")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar(500)  comment '日志文件路径',")  //日志文件路径
			.append( "relativeParentDir varchar(500)  comment '日志文件子目录相对路径,relativeParentDir',")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar(500)  comment '日志文件indoe标识',")   //日志文件indoe标识
			.append( "jobId varchar(500)  comment '作业id 6.7.7版本新增',")   //作业id 6.7.7版本新增
			.append( "jobType varchar(500)  comment '作业输入插件类型 6.7.7版本新增',")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar(100)  comment '增量状态唯一标识' ) comment '增量状态同步记录历史表' ENGINE=InnoDB").toString(); 
//			.append( "PRIMARY KEY(ID)) ENGINE=InnoDB").toString();
	public static final String oracle_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(10) NOT NULL, lastvalue NUMBER(10) NOT NULL, " )
            .append( "strLastValue varchar2(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype NUMBER(1) NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
			.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar2(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar2(500) ,")   //日志文件indoe标识
			.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar2(100) )").toString(); //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();
	public static final String dm_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(10) NOT NULL, lastvalue NUMBER(10) NOT NULL," )
            .append( "strLastValue varchar2(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append(" lastvaluetype NUMBER(1) NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
			.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar2(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar2(500) ,")   //日志文件indoe标识
			.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar2(100) )").toString() ; //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();
	public static final String sqlserver_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName (ID varchar(100),lasttime bigint NOT NULL,lastvalue bigint NOT NULL," )
            .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype INT NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
			.append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar(500) ,")   //日志文件indoe标识
			.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar(100) )").toString(); //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();

    public static final String postgresql_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName (ID varchar(100),lasttime bigint NOT NULL,lastvalue bigint NOT NULL," )
            .append( "strLastValue varchar(2000),")  //增量字段值，存储字符串形式的增量值，比如LocalDateTime
            .append("lastvaluetype INT NOT NULL,") //值类型 0-数字 1-日期 2-LocalDateTime
            .append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
            .append( "filePath varchar(500) ,")  //日志文件路径
            .append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
            .append( "fileId varchar(500) ,")   //日志文件indoe标识
            .append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
            .append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
            .append( "statusId varchar(100) )").toString(); //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();

	/**是否启用sql日志，true启用，false 不启用，*/
	private boolean showSql;
	private boolean usePool = false;
	/**
	 * dbtype专用于设置不支持的数据库类型名称和数据库适配器，方便用户扩展不支持的数据库的数据导入
	 * 可选字段，设置了dbAdaptor可以不设置dbtype，默认为数据库driver类路径
	 */
	private String dbtype ;
	/**
	 * dbAdaptor专用于设置不支持的数据库类型名称和数据库适配器，方便用户扩展不支持的数据库的数据导入
	 * dbAdaptor必须继承自com.frameworkset.orm.adapter.DB或者其继承DB的类
	 */
	private String dbAdaptor;
	private boolean columnLableUpperCase ;
	/**
	 * 事务管理机制只有在一次性全量单线程导入的情况下才有用
	 */
	private boolean enableDBTransaction = false;

    private Properties connectionProperties;


	/**
	 * https://doc.bbossgroups.com/#/persistent/encrypt
	 * 同时如果想对账号、口令、url之间的任意两个组合加密的话，用户可以自己继承 com.frameworkset.common.poolman.security.BaseDBInfoEncrypt类，参考默认插件，实现相应的信息加密方法并配置到aop.properties中即可。
	 */
	private String dbInfoEncryptClass;
	public String getDbDriver() {
		return dbDriver;
	}

	public static String getCreateStatusTableSQL(String dbtype){
		if(dbtype.equals("mysql")){
			return mysql_createStatusTableSQL;
		}
		else if(dbtype.equals("oracle")){
			return oracle_createStatusTableSQL;
		}
		else if(dbtype.equals("dm")){
			return dm_createStatusTableSQL;
		}
		else if(dbtype.equals("mssql")){
			return sqlserver_createStatusTableSQL;
		}
        else if(dbtype.equals(DBFactory.SQLITEX)){
            return sqlitex_createStatusTableSQL;
        }
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return postgresql_createStatusTableSQL;
        }

		throw new DataImportException("getCreateStatusTableSQL failed: unsupport dbtype "+ dbtype+". Use ImportBuilder to set StatusTableSQL like:"+mysql_createStatusTableSQL);
	}
	public static String getCreateHistoryStatusTableSQL(String dbtype){
		if(dbtype.equals("mysql")){
			return mysql_createHistoryStatusTableSQL;
		}
		else if(dbtype.equals("oracle")){
			return oracle_createHistoryStatusTableSQL;
		}
		else if(dbtype.equals("dm")){
			return dm_createHistoryStatusTableSQL;
		}
		else if(dbtype.equals("mssql")){
			return sqlserver_createHistoryStatusTableSQL;
		}
        else if(dbtype.equals(DBFactory.SQLITEX)){
            return sqlitex_createHistoryStatusTableSQL;
        }
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return postgresql_createHistoryStatusTableSQL;
        }

		throw new DataImportException("getCreateHistoryStatusTableSQL failed: unsupport dbtype "+ dbtype+". Use ImportBuilder to set HistoryStatusTableSQL like:"+mysql_createHistoryStatusTableSQL);
	}

	public static String getStatusTableDefaultValue(String dbtype){
		if(dbtype.equals("mysql")){
			return "null";
		}
		else if(dbtype.equals("oracle")){
			return "null";
		}
		else if(dbtype.equals("dm")){
			return "null";
		}
		else if(dbtype.equals("sqlserver")){
			return "null";
		}
		else if(dbtype.equals(DBFactory.SQLITEX)){
			return "0";
		}
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return "null";
        }
        else {
            return "null";
        }
//		throw new DataImportException("getCreateHistoryStatusTableSQL failed: unsupport dbtype "+ dbtype);
	}

	public static String getStatusTableType(String dbtype){
		if(dbtype.equals("mysql")){
			return "VARCHAR";
		}
		else if(dbtype.equals("oracle")){
			return "VARCHAR2";
		}
		else if(dbtype.equals("dm")){
			return "VARCHAR2";
		}
		else if(dbtype.equals("sqlserver")){
			return "VARCHAR";
		}
		else if(dbtype.equals(DBFactory.SQLITEX)){
			return "VARCHAR";
		}
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return "VARCHAR";
        }

        else {
            return "VARCHAR";
        }
//		throw new DataImportException("getCreateHistoryStatusTableSQL failed: unsupport dbtype "+ dbtype);
	}

	public static String getStatusTableTypeNumber(String dbtype){
		if(dbtype.equals("mysql")){
			return "int";
		}
		else if(dbtype.equals("oracle")){
			return "number";
		}
		else if(dbtype.equals("dm")){
			return "number";
		}
		else if(dbtype.equals("sqlserver")){
			return "int";
		}
		else if(dbtype.equals(DBFactory.SQLITEX)){
			return "number";
		}
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return "int";
        }
        else{
            return "int";
        }
//		throw new DataImportException("getCreateHistoryStatusTableSQL failed: unsupport dbtype "+ dbtype);
	}

	public static String getStatusTableTypeBigNumber(String dbtype){
		if(dbtype.equals("mysql")){
			return "bigint";
		}
		else if(dbtype.equals("oracle")){
			return "number";
		}
		else if(dbtype.equals("dm")){
			return "number";
		}
		else if(dbtype.equals("sqlserver")){
			return "bigint";
		}
		else if(dbtype.equals(DBFactory.SQLITEX)){
			return "number";
		}
        else if(dbtype.equals(DBFactory.DBPostgres)){
            return "bigint";
        }

        else{
            return "bigint";
        }
//		throw new DataImportException("getCreateHistoryStatusTableSQL failed: unsupport dbtype "+ dbtype);
	}

	public void setDbDriver(String dbDriver) {
		this.dbDriver = dbDriver;
	}

	public String getDbUrl() {
		return dbUrl;
	}

	public void setDbUrl(String dbUrl) {
		this.dbUrl = dbUrl;
	}

	public String getDbUser() {
		return dbUser;
	}

	public void setDbUser(String dbUser) {
		this.dbUser = dbUser;
	}
	@JsonIgnore
	public String getDbPassword() {
		return dbPassword;
	}
	@JsonIgnore
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}

	public String getValidateSQL() {
		return validateSQL;
	}

	public void setValidateSQL(String validateSQL) {
		this.validateSQL = validateSQL;
	}

	private String validateSQL;


	public Integer getJdbcFetchSize() {
		return jdbcFetchSize;
	}

	public void setJdbcFetchSize(Integer jdbcFetchSize) {
		this.jdbcFetchSize = jdbcFetchSize;
	}

	public String getDbName() {
		return dbName;
	}

	public void setDbName(String dbName) {
		this.dbName = dbName;
	}

	private String dbName;

	public boolean isShowSql() {
		return showSql;
	}

	public void setShowSql(boolean showSql) {
		this.showSql = showSql;
	}

	public boolean isUsePool() {
		return usePool;
	}

	public void setUsePool(boolean usePool) {
		this.usePool = usePool;
	}

	public int getMaxSize() {
		return maxSize;
	}

	public void setMaxSize(int maxSize) {
		this.maxSize = maxSize;
	}

	public int getMinIdleSize() {
		return minIdleSize;
	}

	public void setMinIdleSize(int minIdleSize) {
		this.minIdleSize = minIdleSize;
	}

	public int getInitSize() {
		return initSize;
	}

	public void setInitSize(int initSize) {
		this.initSize = initSize;
	}

	public String getStatusTableDML() {
		return statusTableDML;
	}

	public void setStatusTableDML(String statusTableDML) {
		this.statusTableDML = statusTableDML;
	}

	public String getDbtype() {
		return dbtype;
	}

	public void setDbtype(String dbtype) {
		this.dbtype = dbtype;
	}

	public String getDbAdaptor() {
		return dbAdaptor;
	}

	public void setDbAdaptor(String dbAdaptor) {
		this.dbAdaptor = dbAdaptor;
	}

	public boolean isEnableDBTransaction() {
		return enableDBTransaction;
	}

	public void setEnableDBTransaction(boolean enableDBTransaction) {
		this.enableDBTransaction = enableDBTransaction;
	}

	public boolean isColumnLableUpperCase() {
		return columnLableUpperCase;
	}

	public void setColumnLableUpperCase(boolean columnLableUpperCase) {
		this.columnLableUpperCase = columnLableUpperCase;
	}

	public static final String db_name_key = "db.name";
	public static final String db_user_key = "db.user";

	public static final String db_password_key = "db.password";

	public static final String db_driver_key = "db.driver";

	public static final String db_enableDBTransaction_key = "db.enableDBTransaction";

	public static final String db_url_key = "db.url";

	public static final String db_usePool_key = "db.usePool";

	public static final String db_validateSQL_key = "db.validateSQL";
	public static final String db_dbInfoEncryptClass_key = "db.dbInfoEncryptClass";
	public static final String db_removeAbandoned_key = "db.removeAbandoned";
	public static final String db_connectionTimeout_key = "db.connectionTimeout";


	public static final String db_maxWait_key = "db.maxWait";

	public static final String db_maxIdleTime_key = "db.maxIdleTime";
    public static final String db_enableBalance_key = "db.enableBalance";
    public static final String db_balance_key = "db.balance";
    
	public static final String db_showsql_key = "db.showsql";

	public static final String db_jdbcFetchSize_key = "db.jdbcFetchSize";


	public static final String db_initSize_key = "db.initSize";

	public static final String db_minIdleSize_key = "db.minIdleSize";

	public static final String db_maxSize_key = "db.maxSize";

	public static final String db_statusTableDML_key = "db.statusTableDML";

	public static final String db_dbAdaptor_key = "db.dbAdaptor";

	public static final String db_dbtype_key = "db.dbtype";
	public static final String db_columnLableUpperCase_key = "db.columnLableUpperCase";


	public String getDbInfoEncryptClass() {
		return dbInfoEncryptClass;
	}

	public void setDbInfoEncryptClass(String dbInfoEncryptClass) {
		this.dbInfoEncryptClass = dbInfoEncryptClass;
	}
	public boolean isRemoveAbandoned() {
		return removeAbandoned;
	}

	public void setRemoveAbandoned(boolean removeAbandoned) {
		this.removeAbandoned = removeAbandoned;
	}

	public int getConnectionTimeout() {
		return connectionTimeout;
	}

	public void setConnectionTimeout(int connectionTimeout) {
		this.connectionTimeout = connectionTimeout;
	}

	public int getMaxWait() {
		return maxWait;
	}

	public void setMaxWait(int maxWait) {
		this.maxWait = maxWait;
	}

	public int getMaxIdleTime() {
		return maxIdleTime;
	}

	public void setMaxIdleTime(int maxIdleTime) {
		this.maxIdleTime = maxIdleTime;
	}
    public String getStatusHistoryTableDML() {
        return statusHistoryTableDML;
    }

    public void setStatusHistoryTableDML(String statusHistoryTableDML) {
        this.statusHistoryTableDML = statusHistoryTableDML;
    }

    public Properties getConnectionProperties() {
        return connectionProperties;
    }

    public void setConnectionProperties(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }
    public void addConnectionProperty(String name,Object value){
        if(connectionProperties == null)
            connectionProperties = new Properties();
        connectionProperties.put(name,value);
    }

    public String getBalance() {
        return balance;
    }

    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * {@code jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true}
     *
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     *
     * 另外也可以在DBConf上进行设置，例如：
     * BConf tempConf = new DBConf();
     *         tempConf.setPoolname(ds.getDbname());
     *         tempConf.setDriver(ds.getDbdriver());
     *         tempConf.setJdbcurl( ds.getDburl());
     *         tempConf.setUsername(ds.getDbuser());
     *         tempConf.setPassword(ds.getDbpassword());
     *         tempConf.setValidationQuery(ds.getValidationQuery());
     *         //tempConf.setTxIsolationLevel("READ_COMMITTED");
     *         tempConf.setJndiName("jndi-"+ds.getDbname());
     *         PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
     *         int initialConnections = propertiesContainer.getIntProperty("initialConnections",5);
     *         tempConf.setInitialConnections(initialConnections);
     *         int minimumSize = propertiesContainer.getIntProperty("minimumSize",5);
     *         tempConf.setMinimumSize(minimumSize);
     *         int maximumSize = propertiesContainer.getIntProperty("maximumSize",10);
     *         tempConf.setMaximumSize(maximumSize);
     *         tempConf.setUsepool(true);
     *         tempConf.setExternal(false);
     *         tempConf.setEncryptdbinfo(false);
     *         boolean showsql = propertiesContainer.getBooleanProperty("showsql",true);
     *         tempConf.setShowsql(showsql);
     *         tempConf.setQueryfetchsize(null);
     *         tempConf.setEnableBalance(true);
     *         tempConf.setBalance(DBConf.BALANCE_RANDOM);
     *         return SQLManager.startPool(tempConf);  
     * @param balance
     */
    public void setBalance(String balance) {
        this.balance = balance;
    }

    public boolean isEnableBalance() {
        return enableBalance;
    }

    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * {@code jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true}
     *
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     *
     * 另外也可以在DBConf上进行设置，例如：
     * BConf tempConf = new DBConf();
     *         tempConf.setPoolname(ds.getDbname());
     *         tempConf.setDriver(ds.getDbdriver());
     *         tempConf.setJdbcurl( ds.getDburl());
     *         tempConf.setUsername(ds.getDbuser());
     *         tempConf.setPassword(ds.getDbpassword());
     *         tempConf.setValidationQuery(ds.getValidationQuery());
     *         //tempConf.setTxIsolationLevel("READ_COMMITTED");
     *         tempConf.setJndiName("jndi-"+ds.getDbname());
     *         PropertiesContainer propertiesContainer = PropertiesUtil.getPropertiesContainer();
     *         int initialConnections = propertiesContainer.getIntProperty("initialConnections",5);
     *         tempConf.setInitialConnections(initialConnections);
     *         int minimumSize = propertiesContainer.getIntProperty("minimumSize",5);
     *         tempConf.setMinimumSize(minimumSize);
     *         int maximumSize = propertiesContainer.getIntProperty("maximumSize",10);
     *         tempConf.setMaximumSize(maximumSize);
     *         tempConf.setUsepool(true);
     *         tempConf.setExternal(false);
     *         tempConf.setEncryptdbinfo(false);
     *         boolean showsql = propertiesContainer.getBooleanProperty("showsql",true);
     *         tempConf.setShowsql(showsql);
     *         tempConf.setQueryfetchsize(null);
     *         tempConf.setEnableBalance(true);
     *         tempConf.setBalance(DBConf.BALANCE_RANDOM);
     *         return SQLManager.startPool(tempConf); 
     * @param enableBalance
     */
    public void setEnableBalance(boolean enableBalance) {
        this.enableBalance = enableBalance;
    }
    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
