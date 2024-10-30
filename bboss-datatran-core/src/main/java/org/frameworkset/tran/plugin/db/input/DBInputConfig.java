package org.frameworkset.tran.plugin.db.input;
/**
 * Copyright 2022 bboss
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.record.RecordBuidler;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public class DBInputConfig extends BaseDBConfig implements InputConfig {

	private String sourceDbname;
	protected String sql;
	private String sqlFilepath;
	protected String sqlName;

	private Boolean enableDBTransaction;
    private Integer fetchSize;
    private boolean enableLocalDate;

    private Boolean parallelDatarefactor;

    private RecordBuidler<ResultSet> recordBuidler;
    
    public DBInputConfig setEnableLocalDate(boolean enableLocalDate){
        this.enableLocalDate = enableLocalDate;
        return this;
    }
    public boolean isParallelDatarefactor(){
        if(parallelDatarefactor != null)
            return parallelDatarefactor;
        return false;
    }

    /**
     * 并行Datarefactor处理需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException
     * @param parallelDatarefactor
     * @return
     */
    public DBInputConfig setParallelDatarefactor(boolean parallelDatarefactor) {
        this.parallelDatarefactor = parallelDatarefactor;
        return this;
    }
    
    

    @Override
    public boolean enableLocalDate() {
        return enableLocalDate;
    }

    public String getSourceDbname() {
		return sourceDbname;
	}
//
//	public DBInputConfig setSourceDbname(String sourceDbname) {
//		this.sourceDbname = sourceDbname;
//		return this;
//	}
	public String getDBName(){
		DBConfig dbConfig = getDbConfig();
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName()) ){
			return dbConfig.getDbName();
		}
		if(sourceDbname != null){
			return sourceDbname;
		}

		return null;
	}
	public DBConfig getDbConfig() {
		if(dbConfig == null ){
			if(sourceDbname != null){
				return dbConfigMap.get(sourceDbname);
			}
		}
		return dbConfig;
	}
	public DBInputConfig setDbConfig(DBConfig dbConfig) {
		this.dbConfig = dbConfig;
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName()) )
			this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);

		return this;
	}

	public DBInputConfig setSql(String sql) {
		this.sql = sql;
		return this;
	}

	public String getSql() {
		return sql;
	}

	public DBInputConfig setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
		return this;
	}

	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public DBInputConfig setSqlName(String sqlName) {
		this.sqlName = sqlName;
		return this;
	}

	public String getSqlName() {
		return sqlName;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {

		if(SimpleStringUtil.isEmpty(this.getSql())){
			if(SimpleStringUtil.isEmpty(getSqlFilepath()) || SimpleStringUtil.isEmpty(getSqlName()) ){
				throw new DataImportException("Input sql is not setted.");
			}
		}
		if(dbConfig == null ){

			dbConfig = importBuilder.getDefaultDBConfig();
			this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);
		}
		if(dbConfig == null){
			throw new DataImportException("Source DB Config not config to dbinputconfig.");
		}
		if(SimpleStringUtil.isEmpty(sourceDbname))
			sourceDbname = dbConfig.getDbName();

        if(importBuilder.isSetFetchSized() && fetchSize == null) {
//            if (dbConfig.getJdbcFetchSize() == null) {
//                setJdbcFetchSize(importBuilder.getFetchSize());
//            }
            fetchSize = importBuilder.getFetchSize();
        }
        if(fetchSize != null){
            _setJdbcFetchSize(fetchSize);
        }
        if(isParallelDatarefactor()){
            if(recordBuidler == null){
                recordBuidler = new DBRecordBuilder();
            }
        }
        if(dbConfig != null){
            dbConfig.setDataSource(dataSource);
        }

	}

    public Integer getFetchSize() {
        return fetchSize;
    }

    @Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new DBInputDataTranPlugin(   importContext );
	}



	public DBInputConfig setColumnLableUpperCase(boolean columnLableUpperCase) {
		_setColumnLableUpperCase(columnLableUpperCase);
		return this;
	}

	public DBInputConfig setDbInitSize(int dbInitSize) {
		_setDbInitSize( dbInitSize);
		return this;
	}
	public DBInputConfig setDbMaxSize(int dbMaxSize) {
		_setDbMaxSize(  dbMaxSize);
		return this;
	}
	public DBInputConfig setDbMinIdleSize(int dbMinIdleSize) {
		_setDbMinIdleSize(  dbMinIdleSize);
		return this;
	}



	public DBInputConfig setDbDriver(String dbDriver) {
		_setDbDriver(  dbDriver);
		return this;
	}
	public DBInputConfig setEnableDBTransaction(boolean enableDBTransaction) {
		_setEnableDBTransaction(  enableDBTransaction);
		return this;
	}


	public DBInputConfig setDbUrl(String dbUrl) {
		_setDbUrl( dbUrl);
		return this;
	}

	public DBInputConfig setDbAdaptor(String dbAdaptor) {
		_setDbAdaptor(  dbAdaptor);
		return this;

	}

	public DBInputConfig setDbtype(String dbtype) {
		_setDbtype(  dbtype);
		return this;
	}

	public DBInputConfig setDbUser(String dbUser) {
		_setDbUser(  dbUser);
		return this;
	}

	public DBInputConfig setDbPassword(String dbPassword) {
		_setDbPassword(  dbPassword);
		return this;
	}

	public DBInputConfig setValidateSQL(String validateSQL) {
		_setValidateSQL(  validateSQL);
		return this;
	}

	public DBInputConfig setUsePool(boolean usePool) {
		_setUsePool(  usePool);
		return this;
	}


	public DBInputConfig setDbInfoEncryptClass(String dbInfoEncryptClass){
		_setDbInfoEncryptClass(dbInfoEncryptClass);
		return this;
	}

    /**
     * 插件查询jdbcFetchSize设置，每次执行查询请求时进行设置
     * @param jdbcFetchSize
     * @return
     */
	public DBInputConfig setJdbcFetchSize(Integer jdbcFetchSize) {
//		_setJdbcFetchSize(  jdbcFetchSize);
        this.fetchSize = jdbcFetchSize;
		return  this;
	}


	public DBInputConfig setRemoveAbandoned(boolean removeAbandoned) {
		_setRemoveAbandoned( removeAbandoned);
		return  this;
	}


    /**
     * The minimum amount of time an object may sit idle in the pool before it is eligible for eviction by the idle
     * object evictor (if any).
     * 单位：毫秒
     */
	public DBInputConfig setConnectionTimeout(int connectionTimeout) {
		_setConnectionTimeout( connectionTimeout);
		return  this;
	}


    /**
     * 申请链接超时时间，单位：毫秒
     */
	public DBInputConfig setMaxWait(int maxWait) {
		_setMaxWait( maxWait);
		return  this;
	}

    /**
     * Set max idle Times in seconds ,if exhaust this times the used connection object will be Abandoned removed if removeAbandoned is true.
     default value is 300 seconds.

     see removeAbandonedTimeout parameter in commons dbcp.
     单位：秒
     */
	public DBInputConfig setMaxIdleTime(int maxIdleTime) {
		_setMaxIdleTime( maxIdleTime);
		return  this;
	}
	public DBConfig getDBConfig(String dbname){
		return dbConfigMap.get(dbname);
	}
	public Boolean getEnableDBTransaction() {
		return enableDBTransaction;
	}
	public DBInputConfig setDbName(String dbName) {
		_setDbName(  dbName);
		this.sourceDbname = dbName;

		return this;
	}

	public DBInputConfig setShowSql(boolean showsql) {
		_setShowSql(  showsql);
		return this;
	}

    public DBInputConfig setConnectionProperties(Properties connectionProperties) {
        _setConnectionProperties( connectionProperties);
        return this;
    }
    public DBInputConfig addConnectionProperty(String name,Object value){
        _addConnectionProperty( name, value);
        return this;
    }

	public DBInputConfig setEnableDBTransaction(Boolean enableDBTransaction) {
		this.enableDBTransaction = enableDBTransaction;
		return this;
	}

 
    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true
     * <p>
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     * <p>
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
     *
     * @param balance
     * @return
     */
    public DBInputConfig setBalance(String balance) {
        _setBalance(balance);
        return this;
    }



    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true
     * <p>
     * b.enableBalance为true时启用负载均衡机制，并具备原有容灾功能，否则只具备容灾功能
     * b.balance 指定负载均衡算法，目前支持random（随机算法，不公平机制）和roundbin(轮询算法，公平机制)两种算法，默认random算法
     * <p>
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
     *
     * @param enableBalance
     * @return
     */
    public DBInputConfig setEnableBalance(boolean enableBalance) {
        _setEnableBalance(enableBalance);
        return this;
    }
    public RecordBuidler<ResultSet> getRecordBuidler() {
        return recordBuidler;
    }
    /**
     * 并行Datarefactor处理标记为parallelDatarefactor=true时，需要设置RecordBuidler，默认为DBRecordBuilder，如果需要自定义resultset record，从DBRecordBuilder继承实现方法即可：
     * public Map<String, Object> build(RecordBuidlerContext<ResultSet> recordBuidlerContext) throws DataImportException
     * @param recordBuidler
     * @return
     */
    public DBInputConfig setRecordBuidler(RecordBuidler recordBuidler) {
        this.recordBuidler = (RecordBuidler<ResultSet>)recordBuidler;
        return this;
    }

    /**
     * 设置外部数据源
     * @param dataSource
     * @return
     */
    public DBInputConfig setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }
}
