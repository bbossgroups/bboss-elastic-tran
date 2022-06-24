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

import org.frameworkset.spi.assemble.GetProperties;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.plugin.BaseConfig;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/20
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseDBConfig extends BaseConfig {
	protected Map<String,Object> customDBConfigs = new HashMap<String, Object>();
	protected DBConfig dbConfig;
	protected Map<String,DBConfig> dbConfigMap = new LinkedHashMap<>();

	protected void _buildDBConfig(GetProperties propertiesContainer, String dbName, DBConfig dbConfig, String prefix){


		if(!customDBConfigs.containsKey(prefix+"db.name")) {
			dbConfig.setDbName(dbName);
		}
		if(!customDBConfigs.containsKey(prefix+"db.user")) {
			String dbUser  = propertiesContainer.getExternalProperty(prefix+"db.user");
			dbConfig.setDbUser(dbUser);
		}
		if(!customDBConfigs.containsKey(prefix+"db.password")) {
			String dbPassword  = propertiesContainer.getExternalProperty(prefix+"db.password");
			dbConfig.setDbPassword(dbPassword);
		}
		if(!customDBConfigs.containsKey(prefix+"db.driver")) {
			String dbDriver  = propertiesContainer.getExternalProperty(prefix+"db.driver");
			dbConfig.setDbDriver(dbDriver);
		}

		if(!customDBConfigs.containsKey(prefix+"db.enableDBTransaction")) {
			boolean enableDBTransaction = propertiesContainer.getExternalBooleanProperty(prefix+"db.enableDBTransaction",false);
			dbConfig.setEnableDBTransaction(enableDBTransaction);
		}
		if(!customDBConfigs.containsKey(prefix+"db.url")) {
			String dbUrl  = propertiesContainer.getExternalProperty(prefix+"db.url");
			dbConfig.setDbUrl(dbUrl);
		}
		if(!customDBConfigs.containsKey(prefix+"db.usePool")) {
			String _usePool = propertiesContainer.getExternalProperty(prefix+"db.usePool");
			if(_usePool != null && !_usePool.equals("")) {
				boolean usePool = Boolean.parseBoolean(_usePool);
				dbConfig.setUsePool(usePool);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.validateSQL")) {
			String validateSQL  = propertiesContainer.getExternalProperty(prefix+"db.validateSQL");
			dbConfig.setValidateSQL(validateSQL);
		}

		if(!customDBConfigs.containsKey(prefix+"db.showsql")) {
			String _showSql = propertiesContainer.getExternalProperty(prefix+"db.showsql");
			if(_showSql != null && !_showSql.equals("")) {
				boolean showSql = Boolean.parseBoolean(_showSql);
				dbConfig.setShowSql(showSql);
			}
		}

		if(!customDBConfigs.containsKey(prefix+"db.jdbcFetchSize")) {
			String _jdbcFetchSize = propertiesContainer.getExternalProperty(prefix+"db.jdbcFetchSize");
			if(_jdbcFetchSize != null && !_jdbcFetchSize.equals("")) {
				int jdbcFetchSize = Integer.parseInt(_jdbcFetchSize);
				dbConfig.setJdbcFetchSize(jdbcFetchSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.initSize")) {
			String _initSize = propertiesContainer.getExternalProperty(prefix+"db.initSize");
			if(_initSize != null && !_initSize.equals("")) {
				int initSize = Integer.parseInt(_initSize);
				dbConfig.setInitSize(initSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.minIdleSize")) {
			String _minIdleSize = propertiesContainer.getExternalProperty(prefix+"db.minIdleSize");
			if(_minIdleSize != null && !_minIdleSize.equals("")) {
				int minIdleSize = Integer.parseInt(_minIdleSize);
				dbConfig.setMinIdleSize(minIdleSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.maxSize")) {
			String _maxSize = propertiesContainer.getExternalProperty(prefix+"db.maxSize");
			if(_maxSize != null && !_maxSize.equals("")) {
				int maxSize = Integer.parseInt(_maxSize);
				dbConfig.setMaxSize(maxSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.statusTableDML")) {
			String statusTableDML  = propertiesContainer.getExternalProperty(prefix+"db.statusTableDML");
			dbConfig.setStatusTableDML(statusTableDML);
		}
		if(!customDBConfigs.containsKey(prefix+"db.dbAdaptor")) {
			String dbAdaptor  = propertiesContainer.getExternalProperty(prefix+"db.dbAdaptor");
			dbConfig.setDbAdaptor(dbAdaptor);
		}
		if(!customDBConfigs.containsKey(prefix+"db.dbtype")) {
			String dbtype  = propertiesContainer.getExternalProperty(prefix+"db.dbtype");
			dbConfig.setDbtype(dbtype);
		}
		if(!customDBConfigs.containsKey(prefix+"db.columnLableUpperCase")) {
			String columnLableUpperCase  = propertiesContainer.getExternalProperty(prefix+"db.columnLableUpperCase");
			if(columnLableUpperCase != null){
				boolean _columnLableUpperCase = Boolean.parseBoolean(columnLableUpperCase);
				dbConfig.setColumnLableUpperCase(_columnLableUpperCase);
			}

		}
		if(!customDBConfigs.containsKey(prefix+"db.dbInfoEncryptClass")) {
			String dbInfoEncryptClass  = propertiesContainer.getExternalProperty(prefix+"db.dbInfoEncryptClass");
			dbConfig.setDbInfoEncryptClass(dbInfoEncryptClass);
		}

	}
	protected void _setJdbcFetchSize(Integer jdbcFetchSize) {
		this.customDBConfigs.put(DBConfig.db_jdbcFetchSize_key,1);
		checkDBConfig();
		dbConfig.setJdbcFetchSize(jdbcFetchSize);

	}
	protected void checkDBConfig(){
		if(this.dbConfig == null){
			this.dbConfig = new DBConfig();
		}
	}
	public void _setDbPassword(String dbPassword) {
		this.customDBConfigs.put(DBConfig.db_password_key,1);
		checkDBConfig();
		dbConfig.setDbPassword(dbPassword);

	}



	public void _setShowSql(boolean showSql) {
		this.customDBConfigs.put(DBConfig.db_showsql_key,1);
		checkDBConfig();
		dbConfig.setShowSql(showSql);


	}

	public void _setDbName(String dbName) {
		this.customDBConfigs.put(DBConfig.db_name_key,1);
		checkDBConfig();

		dbConfig.setDbName(dbName);

	}
	public void _setColumnLableUpperCase(boolean columnLableUpperCase) {
		this.customDBConfigs.put(DBConfig.db_columnLableUpperCase_key,1);
		checkDBConfig();

		dbConfig.setColumnLableUpperCase(columnLableUpperCase);
	}
	public void _setDbInitSize(int dbInitSize) {
		this.customDBConfigs.put(DBConfig.db_initSize_key,1);
		checkDBConfig();

		dbConfig.setInitSize(dbInitSize);
	}
	public void _setDbMaxSize(int dbMaxSize) {
		this.customDBConfigs.put(DBConfig.db_maxSize_key,1);
		checkDBConfig();

		dbConfig.setMaxSize(dbMaxSize);
	}
	public void _setDbMinIdleSize(int dbMinIdleSize) {
		this.customDBConfigs.put(DBConfig.db_minIdleSize_key,1);
		checkDBConfig();

		dbConfig.setMinIdleSize(dbMinIdleSize);
	}

	public void _setDbDriver(String dbDriver) {
		this.customDBConfigs.put(DBConfig.db_driver_key,1);
		checkDBConfig();
		this.dbConfig.setDbDriver(dbDriver);
	}
	public void _setEnableDBTransaction(boolean enableDBTransaction) {
		this.customDBConfigs.put(DBConfig.db_enableDBTransaction_key,1);
		checkDBConfig();
		dbConfig.setEnableDBTransaction(enableDBTransaction);
	}

	public void _setDbAdaptor(String dbAdaptor) {
		this.customDBConfigs.put(DBConfig.db_dbAdaptor_key,1);
		checkDBConfig();
		this.dbConfig.setDbAdaptor(dbAdaptor);
	}

	public void _setDbtype(String dbtype) {
		this.customDBConfigs.put(DBConfig.db_dbtype_key,1);
		checkDBConfig();
		this.dbConfig.setDbtype(dbtype);
	}

	public void _setDbUrl(String dbUrl) {
		this.customDBConfigs.put(DBConfig.db_url_key,1);
		checkDBConfig();
		dbConfig.setDbUrl(dbUrl);
	}

	public void _setDbUser(String dbUser) {
		this.customDBConfigs.put(DBConfig.db_user_key,1);
		checkDBConfig();
		this.dbConfig.setDbUser(dbUser);
	}

	public void _setValidateSQL(String validateSQL) {
		this.customDBConfigs.put(DBConfig.db_validateSQL_key,1);
		checkDBConfig();
		dbConfig.setValidateSQL(validateSQL);
	}

	public void _setUsePool(boolean usePool) {
		this.customDBConfigs.put(DBConfig.db_usePool_key,1);
		checkDBConfig();
		dbConfig.setUsePool(usePool);
	}

	public void _setDbInfoEncryptClass(String dbInfoEncryptClass) {
		this.customDBConfigs.put(DBConfig.db_dbInfoEncryptClass_key,1);
		checkDBConfig();
		dbConfig.setDbInfoEncryptClass(dbInfoEncryptClass);
	}
}
