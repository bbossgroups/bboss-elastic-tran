package org.frameworkset.tran.plugin.db.output;
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

import com.frameworkset.common.poolman.BatchHandler;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.RecordSpecialConfigsContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBConfig;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.db.input.StatementHandler;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.schedule.TaskContext;

import javax.sql.DataSource;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public class DBOutputConfig extends BaseDBConfig implements OutputConfig {
	private DBConfig targetDBConfig;
	protected String sql;
	private String sqlFilepath;
	protected String sqlName;
	protected String insertSql;
	protected String insertSqlName;
	protected String updateSqlName;
	protected String updateSql;
	protected String deleteSqlName;
	protected String deleteSql;
	protected boolean optimize;
    protected boolean multiSQLConf;
    protected boolean multiSQLConfTargetDBName;
	protected BatchHandler batchHandler;
	protected StatementHandler statementHandler;
	private TranSQLInfo targetSqlInfo;
	private TranSQLInfo targetUpdateSqlInfo;
	private TranSQLInfo targetDeleteSqlInfo;

	private String targetDbname;
    /**
     * 设置不同类型的记录对应的数据库增删改查sql语句信息
     */
    private Map<String,SQLConf> tableSQLConf;
    /**
     * 设置数据库对应的ddl，回放的目标数据源配置
     */
    private Map<String,DDLConf> ddlConfs;
    private SQLConfResolver sqlConfResolver;
    private boolean ignoreDDLSynError;
    

    public static final String SPECIALCONFIG_RECORDPARAMS_NAME = "recordParams";

    /**
     * mysql binlog输入插件对接时，默认使用表名称映射对应的sqlconf配置
     * 其他场景需要通过SQLConfResolver接口从当前记录中获取对应的字段值作为sqlconf配置对应的映射名称
     * @param sqlConfResolver
     */
    public DBOutputConfig setSqlConfResolver(SQLConfResolver sqlConfResolver) {
        this.sqlConfResolver = sqlConfResolver;
        return this;
    }

    /**
     * 为不同类型的记录维护特定的增删改sql配置
     * @param name
     * @param sqlConf
     * @return
     */
    public DBOutputConfig addSQLConf(String name,SQLConf sqlConf){
        if(tableSQLConf == null) {
            tableSQLConf = new LinkedHashMap<>();
        }
        tableSQLConf.put(name,sqlConf);
        return this;
    }
    /**
     * 为不同类型的记录维护特定的增删改sql配置
     * @param ddlConf
     * @return
     */
    public DBOutputConfig addDDLConf(DDLConf ddlConf){
        if(SimpleStringUtil.isEmpty(ddlConf.getDatabase())){
            throw new DataImportException("ddlConf[database="+ddlConf.getDatabase()
                    +",targetDBName="+ddlConf.getTargetDbName()+"] database can't be null.");
        }
        if(ddlConfs == null) {
            ddlConfs = new LinkedHashMap<>();
        }
        ddlConfs.put(ddlConf.getDatabase(),ddlConf);
        return this;
    }

    public String resolveSQLConfKey(TaskContext taskContext,CommonRecord commonRecord){
        if(sqlConfResolver != null){
            return sqlConfResolver.resolver(taskContext,commonRecord);
        }
        return null;
    }


	public DBOutputConfig setDbName(String dbName) {
		_setDbName(  dbName);
//		if(targetDbname == null){
			this.targetDbname = dbName;
//		}

		return this;
	} 
	public String getTargetDbname() {
		return targetDbname;
	}

//	public DBOutputConfig setTargetDbname(String targetDbname) {
//		this.targetDbname = targetDbname;
//		return this;
//	}

	public DBOutputConfig setStatementHandler(StatementHandler statementHandler) {
		this.statementHandler = statementHandler;
		return this;
	}

	public StatementHandler getStatementHandler() {
		return statementHandler;
	}

	public DBOutputConfig setInsertSqlName(String insertSqlName) {
		this.insertSqlName = insertSqlName;
		return this;
	}


	public String getInsertSqlName() {
		return insertSqlName;

	}

	public String getInsertSql() {
		return insertSql;

	}

	public DBOutputConfig setInsertSql(String insertSql) {
		this.insertSql = insertSql;
		return this;
	}
	public DBConfig getTargetDBConfig() {

			return targetDBConfig;


	}

	public DBConfig getTargetDBConfig(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetDBConfig() != null)
			return taskContext.getTargetDBConfig();
		return getTargetDBConfig();
	}

	public DBOutputConfig setTargetDBConfig(DBConfig targetDBConfig) {
		this.targetDBConfig = targetDBConfig;
		return this;
	}

	public String getSql() {
		return sql;
	}


	public DBOutputConfig setSql(String sql) {
		this.sql = sql;
		return this;
	}



	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public DBOutputConfig setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
		return this;
	}

	public DBOutputConfig setShowSql(boolean showsql) {
		_setShowSql(  showsql);
		return this;
	}

	public String getSqlName() {
		return sqlName;
	}

	public DBOutputConfig setSqlName(String sqlName) {
		this.sqlName = sqlName;
		return this;
	}

	public String getDeleteSql() {
		return deleteSql;
	}

	public DBOutputConfig setDeleteSql(String deleteSql) {
		this.deleteSql = deleteSql;
		return this;
	}

	public String getDeleteSqlName() {
		return deleteSqlName;
	}

	public DBOutputConfig setDeleteSqlName(String deleteSqlName) {
		this.deleteSqlName = deleteSqlName;
		return this;
	}

	public String getUpdateSql() {
		return updateSql;
	}

	public DBOutputConfig setUpdateSql(String updateSql) {
		this.updateSql = updateSql;
		return this;
	}

	public String getUpdateSqlName() {
		return updateSqlName;
	}

	public DBOutputConfig setUpdateSqlName(String updateSqlName) {
		this.updateSqlName = updateSqlName;
		return this;
	}

	public DBOutputConfig setOptimize(boolean optimize) {
		this.optimize = optimize;
		return this;
	}

	public boolean optimize() {
		return optimize;
	}

	public DBOutputConfig setBatchHandler(BatchHandler batchHandler) {
		this.batchHandler = batchHandler;
		return this;
	}

	public BatchHandler getBatchHandler() {
		return batchHandler;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		if(targetDBConfig == null ) {
			if(dbConfig == null) {
//				GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml", false);
//				String dbName = propertiesContainer.getExternalProperty("db.name");
//				if (SimpleStringUtil.isNotEmpty(dbName)) {
//					dbConfig = new DBConfig();
//					_buildDBConfig(propertiesContainer, dbName, dbConfig, "");
//					targetDBConfig = dbConfig;
//
//				}
				dbConfig = importBuilder.getDefaultDBConfig();
				if(dbConfig != null)
					targetDBConfig = dbConfig;
			}
			else{
				targetDBConfig = dbConfig;
			}




		}
		if(targetDBConfig == null)
			throw new DataImportException("Target DB Config not config to dboutputconfig.");
		if(SimpleStringUtil.isEmpty(targetDbname)) {
			targetDbname = targetDBConfig.getDbName();
		}

        if(dbConfig != null){
            dbConfig.setDataSource(dataSource);
        }





	}

    public boolean isMultiSQLConfTargetDBName() {
        return multiSQLConfTargetDBName;
    }

    public void initSQLConf(){
        if(this.tableSQLConf != null && this.tableSQLConf.size() > 0 ){
            Iterator<Map.Entry<String, SQLConf>> iterator = tableSQLConf.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String, SQLConf> entry = iterator.next();
                entry.getValue().build();
                org.frameworkset.tran.util.TranUtil.initSQLConf(this,entry.getValue());
                if(entry.getValue().getTargetDbName() != null){
                    multiSQLConfTargetDBName = true;
                }

            }
            multiSQLConf = true;
            if(sqlConfResolver == null){
                sqlConfResolver = new TableSqlConfResolver();
            }
        }
        if(ddlConfs != null && ddlConfs.size() > 0){
            Iterator<Map.Entry<String, DDLConf>> iterator = ddlConfs.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String, DDLConf> entry = iterator.next();
                entry.getValue().build();
                if(entry.getValue().getTargetDbName() != null){
                    multiSQLConfTargetDBName = true;
                }

            }
            multiSQLConf = true;

        }
    }

    public boolean isMultiSQLConf() {
        return multiSQLConf;
    }

    public String getTargetDBName(TaskContext taskContext){
		DBConfig dbConfig = getTargetDBConfig(taskContext);
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName())){
			return dbConfig.getDbName();
		}
		else{
			String dbName = getTargetDbname();
			return dbName;
		}
	}
	public TranSQLInfo getTargetSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetSqlInfo() != null)
			return taskContext.getTargetSqlInfo();
		return targetSqlInfo;
	}

    public TranSQLInfo getTargetSqlInfo(TaskContext taskContext, CommonRecord dbRecord) {
        TranSQLInfo tranSQLInfo = null;
        if(tableSQLConf != null && tableSQLConf.size() > 0) {
            String tableName = sqlConfResolver.resolver(taskContext, dbRecord);
            if(SimpleStringUtil.isNotEmpty(tableName) ) {
                SQLConf sqlConf = tableSQLConf.get(tableName);
                if(sqlConf != null){
                    tranSQLInfo = sqlConf.getTargetSqlInfo();
                }
            }
        }
        if(tranSQLInfo == null) {
            if (taskContext != null && taskContext.getTargetSqlInfo() != null)
                tranSQLInfo = taskContext.getTargetSqlInfo();
            else
                tranSQLInfo = this.targetSqlInfo;
        }
        return tranSQLInfo;
    }


    public DBOutputConfig setTargetSqlInfo(TranSQLInfo targetSqlInfo) {
		this.targetSqlInfo = targetSqlInfo;
		return this;
	}

	public TranSQLInfo getTargetUpdateSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetUpdateSqlInfo() != null)
			return taskContext.getTargetUpdateSqlInfo();
		return targetUpdateSqlInfo;
	}

    public TranSQLInfo getTargetUpdateSqlInfo(TaskContext taskContext, CommonRecord dbRecord) {
        TranSQLInfo tranSQLInfo = null;
        if(tableSQLConf != null && tableSQLConf.size() > 0) {
            String tableName = sqlConfResolver.resolver(taskContext, dbRecord);
            if(SimpleStringUtil.isNotEmpty(tableName) ) {
                SQLConf sqlConf = tableSQLConf.get(tableName);
                if(sqlConf != null){
                    tranSQLInfo = sqlConf.getTargetUpdateSqlInfo();
                }
            }
        }
        if(tranSQLInfo == null) {
            if (taskContext != null && taskContext.getTargetUpdateSqlInfo() != null)
                tranSQLInfo = taskContext.getTargetUpdateSqlInfo();
            else
                tranSQLInfo = this.targetUpdateSqlInfo;
        }
        return tranSQLInfo;
//        if(taskContext != null && taskContext.getTargetUpdateSqlInfo() != null)
//            return taskContext.getTargetUpdateSqlInfo();
//        return targetUpdateSqlInfo;
    }

	public DBOutputConfig setTargetUpdateSqlInfo(TranSQLInfo sqlInfo) {
		this.targetUpdateSqlInfo = sqlInfo;
		return this;
	}
	public TranSQLInfo getTargetDeleteSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetDeleteSqlInfo() != null)
			return taskContext.getTargetDeleteSqlInfo();
		return targetDeleteSqlInfo;
	}
    public DDLConf getDDLConf(TaskContext taskContext, CommonRecord dbRecord){
        String database = (String)dbRecord.getMetaValue("database");
        if(ddlConfs != null){
            return ddlConfs.get(database);
        }
        return null;
    }

    public TranSQLInfo getTargetDeleteSqlInfo(TaskContext taskContext, CommonRecord dbRecord) {
        TranSQLInfo tranSQLInfo = null;
        if(tableSQLConf != null && tableSQLConf.size() > 0) {
            String tableName = sqlConfResolver.resolver(taskContext, dbRecord);
            if(SimpleStringUtil.isNotEmpty(tableName) ) {
                SQLConf sqlConf = tableSQLConf.get(tableName);
                if(sqlConf != null){
                    tranSQLInfo = sqlConf.getTargetDeleteSqlInfo();
                }
            }
        }
        if(tranSQLInfo == null) {
            if (taskContext != null && taskContext.getTargetDeleteSqlInfo() != null)
                tranSQLInfo = taskContext.getTargetDeleteSqlInfo();
            else
                tranSQLInfo = this.targetDeleteSqlInfo;
        }
        return tranSQLInfo;
//        if(taskContext != null && taskContext.getTargetDeleteSqlInfo() != null)
//            return taskContext.getTargetDeleteSqlInfo();
//        return targetDeleteSqlInfo;
    }

	public DBOutputConfig setTargetDeleteSqlInfo(TranSQLInfo sqlInfo) {
		this.targetDeleteSqlInfo = sqlInfo;
		return this;
	}
	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new DBOutputDataTranPlugin(  this,importContext);
	}



	public DBOutputConfig setColumnLableUpperCase(boolean columnLableUpperCase) {
		_setColumnLableUpperCase(columnLableUpperCase);
		return this;
	}

	public DBOutputConfig setDbInitSize(int dbInitSize) {
		_setDbInitSize( dbInitSize);
		return this;
	}
	public DBOutputConfig setDbMaxSize(int dbMaxSize) {
		_setDbMaxSize(  dbMaxSize);
		return this;
	}
	public DBOutputConfig setDbMinIdleSize(int dbMinIdleSize) {
		_setDbMinIdleSize(  dbMinIdleSize);
		return this;
	}



	public DBOutputConfig setDbDriver(String dbDriver) {
		_setDbDriver(  dbDriver);
		return this;
	}
	public DBOutputConfig setEnableDBTransaction(boolean enableDBTransaction) {
		_setEnableDBTransaction(  enableDBTransaction);
		return this;
	}


	public DBOutputConfig setDbUrl(String dbUrl) {
		_setDbUrl( dbUrl);
		return this;
	}

	public DBOutputConfig setDbAdaptor(String dbAdaptor) {
		_setDbAdaptor(  dbAdaptor);
		return this;

	}

	public DBOutputConfig setDbtype(String dbtype) {
		_setDbtype(  dbtype);
		return this;
	}

	public DBOutputConfig setDbUser(String dbUser) {
		_setDbUser(  dbUser);
		return this;
	}

	public DBOutputConfig setDbPassword(String dbPassword) {
		_setDbPassword(  dbPassword);
		return this;
	}

	public DBOutputConfig setValidateSQL(String validateSQL) {
		_setValidateSQL(  validateSQL);
		return this;
	}

	public DBOutputConfig setUsePool(boolean usePool) {
		_setUsePool(  usePool);
		return this;
	}


	public DBOutputConfig setDbInfoEncryptClass(String dbInfoEncryptClass){
		_setDbInfoEncryptClass(dbInfoEncryptClass);
		return this;
	}

//	public DBOutputConfig setJdbcFetchSize(Integer jdbcFetchSize) {
//		_setJdbcFetchSize(  jdbcFetchSize);
//		return  this;
//	}


	public DBOutputConfig setRemoveAbandoned(boolean removeAbandoned) {
		_setRemoveAbandoned( removeAbandoned);
		return  this;
	}


    /**
     * The minimum amount of time an object may sit idle in the pool before it is eligible for eviction by the idle
     * object evictor (if any).
     * 单位：毫秒
     */
	public DBOutputConfig setConnectionTimeout(int connectionTimeout) {
		_setConnectionTimeout( connectionTimeout);
		return  this;
	}


    /**
     * 申请链接超时时间，单位：毫秒
     */
	public DBOutputConfig setMaxWait(int maxWait) {
		_setMaxWait( maxWait);
		return  this;
	}

    /**
     * Set max idle Times in seconds ,if exhaust this times the used connection object will be Abandoned removed if removeAbandoned is true.
     default value is 300 seconds.

     see removeAbandonedTimeout parameter in commons dbcp.
     单位：秒
     */
	public DBOutputConfig setMaxIdleTime(int maxIdleTime) {
		_setMaxIdleTime( maxIdleTime);
		return  this;
	}
    public DBOutputConfig setConnectionProperties(Properties connectionProperties) {
        _setConnectionProperties( connectionProperties);
        return this;
    }
    public DBOutputConfig addConnectionProperty(String name,Object value){
        _addConnectionProperty( name, value);
        return this;
    }

    public boolean isIgnoreDDLSynError() {
        return ignoreDDLSynError;
    }

    public DBOutputConfig setIgnoreDDLSynError(boolean ignoreDDLSynError) {
        this.ignoreDDLSynError = ignoreDDLSynError;
        return this;
    }


    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * {@code jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true}
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
    public DBOutputConfig setBalance(String balance) {
        _setBalance(balance);
        return this;
    }



    /**
     * 1. 为Clickhouse数据源增加负载均衡机制，解决Clickhouse-native-jdbc驱动只有容灾功能而没有负载均衡功能的缺陷，使用方法如下：
     * 在jdbc url地址后面增加b.balance和b.enableBalance参数
     * {@code jdbc:clickhouse://101.13.6.4:29000,101.13.6.7:29000,101.13.6.6:29000/visualops?b.balance=roundbin&b.enableBalance=true}
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
    public DBOutputConfig setEnableBalance(boolean enableBalance) {
        _setEnableBalance(enableBalance);
        return this;
    }
    /**
     * 设置外部数据源
     * @param dataSource
     * @return
     */
    public DBOutputConfig setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    @Override
    public void initRecordSpecialConfigsContext(RecordSpecialConfigsContext recordSpecialConfigsContext, boolean fromMultiOutput){
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = new RecordOutpluginSpecialConfig(this);
        if(!fromMultiOutput){
            recordSpecialConfigsContext.setRecordOutpluginSpecialConfig(recordOutpluginSpecialConfig);
        }
        else{
            recordSpecialConfigsContext.addRecordOutpluginSpecialConfig(outputPlugin,recordOutpluginSpecialConfig);
        }
    }
}
