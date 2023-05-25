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
import com.frameworkset.orm.adapter.DBFactory;

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
					.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
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
					.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
					.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
					.append( "filePath varchar(500) ,")  //日志文件路径
					.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径
					.append( "fileId varchar(500) ,")  //日志文件indoe标识
					.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
					.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
					.append( "statusId number(10)) ")  //状态表中使用的主键标识
//					.append( "PRIMARY KEY (ID))")
					.toString();
	public static final String mysql_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar(100) NOT NULL, lasttime bigint(20) NOT NULL, lastvalue bigint(20) NOT NULL, lastvaluetype int(1) NOT NULL,")
							.append( "status int(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar(500) ,")   //日志文件indoe标识
							.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "PRIMARY KEY(ID)) ENGINE=InnoDB").toString();
	public static final String oracle_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(20) NOT NULL, lastvalue NUMBER(20) NOT NULL, lastvaluetype NUMBER(1) NOT NULL,")
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar2(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar2(500) ,")   //日志文件indoe标识
							.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();
	public static final String dm_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(20) NOT NULL, lastvalue NUMBER(20) NOT NULL, lastvaluetype NUMBER(1) NOT NULL,")
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar2(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar2(500) ,")   //日志文件indoe标识
							.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();
	public static final String sqlserver_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName (ID varchar(100) NOT NULL,lasttime bigint NOT NULL,lastvalue bigint NOT NULL,lastvaluetype INT NOT NULL,")
							.append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500) ,")  //日志文件路径
							.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
							.append( "fileId varchar(500) ,")   //日志文件indoe标识
							.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
							.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
							.append( "constraint $statusTableName_PK primary key(ID))").toString();

    public static final String postgresql_createStatusTableSQL = new StringBuilder().append("CREATE TABLE $statusTableName (ID varchar(100) NOT NULL,lasttime bigint NOT NULL,lastvalue bigint NOT NULL,lastvaluetype INT NOT NULL,")
            .append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
            .append( "filePath varchar(500) ,")  //日志文件路径
            .append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
            .append( "fileId varchar(500) ,")   //日志文件indoe标识
            .append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
            .append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
            .append( "primary key(ID))").toString();

	public static final String mysql_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar(100) NOT NULL, lasttime bigint(20) NOT NULL, lastvalue bigint(20) NOT NULL, lastvaluetype int(1) NOT NULL,")
			.append( "status int(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar(500) ,")   //日志文件indoe标识
			.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar(100) ) ENGINE=InnoDB").toString(); //状态表中使用的主键标识
//			.append( "PRIMARY KEY(ID)) ENGINE=InnoDB").toString();
	public static final String oracle_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(10) NOT NULL, lastvalue NUMBER(10) NOT NULL, lastvaluetype NUMBER(1) NOT NULL,")
			.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar2(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar2(500) ,")   //日志文件indoe标识
			.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar2(100) )").toString(); //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();
	public static final String dm_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName ( ID varchar2(100) NOT NULL, lasttime NUMBER(10) NOT NULL, lastvalue NUMBER(10) NOT NULL, lastvaluetype NUMBER(1) NOT NULL,")
			.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar2(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar2(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar2(500) ,")   //日志文件indoe标识
			.append( "jobId varchar2(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar2(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar2(100) )").toString() ; //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();
	public static final String sqlserver_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName (ID varchar(100),lasttime bigint NOT NULL,lastvalue bigint NOT NULL,lastvaluetype INT NOT NULL,")
			.append( "status INT ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
			.append( "filePath varchar(500) ,")  //日志文件路径
			.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径,relativeParentDir,
			.append( "fileId varchar(500) ,")   //日志文件indoe标识
			.append( "jobId varchar(500) ,")   //作业id 6.7.7版本新增
			.append( "jobType varchar(500) ,")   //作业输入插件类型 6.7.7版本新增
			.append( "statusId varchar(100) )").toString(); //状态表中使用的主键标识
//			.append( "constraint $historyStatusTableName_PK primary key(ID))").toString();

    public static final String postgresql_createHistoryStatusTableSQL = new StringBuilder().append("CREATE TABLE $historyStatusTableName (ID varchar(100),lasttime bigint NOT NULL,lastvalue bigint NOT NULL,lastvaluetype INT NOT NULL,")
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
		else if(dbtype.equals("sqlserver")){
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
		else if(dbtype.equals("sqlserver")){
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
}
