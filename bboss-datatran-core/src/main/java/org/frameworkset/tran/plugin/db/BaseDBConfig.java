package org.frameworkset.tran.plugin.db;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.plugin.BaseConfig;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/20
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseDBConfig<T extends BaseDBConfig> extends BaseConfig<T> {
	protected Map<String,Object> customDBConfigs = new HashMap<String, Object>();
	protected DBConfig dbConfig;
	protected Map<String,DBConfig> dbConfigMap = new LinkedHashMap<>();


    

    @JsonIgnore
    protected DataSource dataSource;


    public static final String SPECIALCONFIG_JDBCGETVARIABLEVALUE_NAME = "jdbcGetVariableValue";
	public T setJdbcFetchSize(Integer jdbcFetchSize) {
		this.customDBConfigs.put(DBConfig.db_jdbcFetchSize_key,1);
		checkDBConfig();
		dbConfig.setJdbcFetchSize(jdbcFetchSize);
        return (T)this;

	}


    protected void checkDBConfig(){
		if(this.dbConfig == null){
			this.dbConfig = new DBConfig();
		}
	}
	public T setDbPassword(String dbPassword) {
		this.customDBConfigs.put(DBConfig.db_password_key,1);
		checkDBConfig();
		dbConfig.setDbPassword(dbPassword);
        return (T)this;
	}



	public T setShowSql(boolean showSql) {
		this.customDBConfigs.put(DBConfig.db_showsql_key,1);
		checkDBConfig();
		dbConfig.setShowSql(showSql);
        return (T)this;


	}

	public T setDbName(String dbName) {
		this.customDBConfigs.put(DBConfig.db_name_key,1);
		checkDBConfig();

		dbConfig.setDbName(dbName);
        return (T)this;

	}

	public T setColumnLableUpperCase(boolean columnLableUpperCase) {
		this.customDBConfigs.put(DBConfig.db_columnLableUpperCase_key,1);
		checkDBConfig();

		dbConfig.setColumnLableUpperCase(columnLableUpperCase);
        return (T)this;
	}
	public T setDbInitSize(int dbInitSize) {
		this.customDBConfigs.put(DBConfig.db_initSize_key,1);
		checkDBConfig();

		dbConfig.setInitSize(dbInitSize);
        return (T)this;
	}
	public T setDbMaxSize(int dbMaxSize) {
		this.customDBConfigs.put(DBConfig.db_maxSize_key,1);
		checkDBConfig();

		dbConfig.setMaxSize(dbMaxSize);
        return (T)this;
	}
	public T setDbMinIdleSize(int dbMinIdleSize) {
		this.customDBConfigs.put(DBConfig.db_minIdleSize_key,1);
		checkDBConfig();

		dbConfig.setMinIdleSize(dbMinIdleSize);
        return (T)this;
	}

	public T setDbDriver(String dbDriver) {
		this.customDBConfigs.put(DBConfig.db_driver_key,1);
		checkDBConfig();
		this.dbConfig.setDbDriver(dbDriver);
        return (T)this;
	}
	public T setEnableDBTransaction(boolean enableDBTransaction) {
		this.customDBConfigs.put(DBConfig.db_enableDBTransaction_key,1);
		checkDBConfig();
		dbConfig.setEnableDBTransaction(enableDBTransaction);
        return (T)this;
	}

	public T setDbAdaptor(String dbAdaptor) {
		this.customDBConfigs.put(DBConfig.db_dbAdaptor_key,1);
		checkDBConfig();
		this.dbConfig.setDbAdaptor(dbAdaptor);
        return (T)this;
	}

	public T setDbtype(String dbtype) {
		this.customDBConfigs.put(DBConfig.db_dbtype_key,1);
		checkDBConfig();
		this.dbConfig.setDbtype(dbtype);
        return (T)this;
	}

	public T setDbUrl(String dbUrl) {
		this.customDBConfigs.put(DBConfig.db_url_key,1);
		checkDBConfig();
		dbConfig.setDbUrl(dbUrl);
        return (T)this;
	}

	public T setDbUser(String dbUser) {
		this.customDBConfigs.put(DBConfig.db_user_key,1);
		checkDBConfig();
		this.dbConfig.setDbUser(dbUser);
        return (T)this;
	}

	public T setValidateSQL(String validateSQL) {
		this.customDBConfigs.put(DBConfig.db_validateSQL_key,1);
		checkDBConfig();
		dbConfig.setValidateSQL(validateSQL);
        return (T)this;
	}

	public T setUsePool(boolean usePool) {
		this.customDBConfigs.put(DBConfig.db_usePool_key,1);
		checkDBConfig();
		dbConfig.setUsePool(usePool);
        return (T)this;
	}

	public T setDbInfoEncryptClass(String dbInfoEncryptClass) {
		this.customDBConfigs.put(DBConfig.db_dbInfoEncryptClass_key,1);
		checkDBConfig();
		dbConfig.setDbInfoEncryptClass(dbInfoEncryptClass);
        return (T)this;
	}



	public T setRemoveAbandoned(boolean removeAbandoned) {
		this.customDBConfigs.put(DBConfig.db_removeAbandoned_key,1);
		checkDBConfig();
		dbConfig.setRemoveAbandoned(removeAbandoned);
        return (T)this;
	}



	public T setConnectionTimeout(int connectionTimeout) {
		this.customDBConfigs.put(DBConfig.db_connectionTimeout_key,1);
		checkDBConfig();
		dbConfig.setConnectionTimeout(connectionTimeout);
        return (T)this;
	}



	public T setMaxWait(int maxWait) {
		this.customDBConfigs.put(DBConfig.db_maxWait_key,1);
		checkDBConfig();
		dbConfig.setMaxWait(maxWait);
        return (T)this;
	}



	public T setMaxIdleTime(int maxIdleTime) {
		this.customDBConfigs.put(DBConfig.db_maxIdleTime_key,1);
		checkDBConfig();
		dbConfig.setMaxIdleTime(maxIdleTime);
        return (T)this;
	}

    public Properties getConnectionProperties() {
        return dbConfig.getConnectionProperties();
    }

    public T setConnectionProperties(Properties connectionProperties) {
        checkDBConfig();
        dbConfig.setConnectionProperties( connectionProperties);
        return (T)this;
    }
    public T addConnectionProperty(String name,Object value){
        checkDBConfig();
        dbConfig.addConnectionProperty( name, value);
        return (T)this;
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
    public T setBalance(String balance) {
        this.customDBConfigs.put(DBConfig.db_balance_key,1);
        checkDBConfig();
        dbConfig.setBalance(balance);
        return (T)this;
    }


    public boolean isEnableBalance() {
        return dbConfig != null ?dbConfig.isEnableBalance():false;
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
    public T setEnableBalance(boolean enableBalance) {
        this.customDBConfigs.put(DBConfig.db_enableBalance_key,1);
        checkDBConfig();
        dbConfig.setEnableBalance(enableBalance);
        return (T)this;
    }

    public String getBalance() {
        return dbConfig != null ? dbConfig.getBalance() : null;
    }
    public DataSource getDataSource() {
        return dataSource;
    }

    
}
