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
	private Integer jdbcFetchsize;

	private Boolean enableDBTransaction;

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
	public void build(ImportBuilder importBuilder) {

		if(SimpleStringUtil.isEmpty(this.getSql())){
			if(SimpleStringUtil.isEmpty(getSqlFilepath()) || SimpleStringUtil.isEmpty(getSqlName()) ){
				throw new DataImportException("Input sql is not setted.");
			}
		}
		if(dbConfig == null ){
//			GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml",false);
//			String dbName  = propertiesContainer.getExternalProperty("db.name");
//			if(SimpleStringUtil.isNotEmpty(dbName)){
//				dbConfig = new DBConfig();
//				_buildDBConfig(propertiesContainer, dbName, dbConfig, "");
//				this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);
//			}
			dbConfig = importBuilder.getDefaultDBConfig();
			this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);
		}
		if(dbConfig == null){
			throw new DataImportException("Source DB Config not config to dbinputconfig.");
		}
		if(SimpleStringUtil.isEmpty(sourceDbname))
			sourceDbname = dbConfig.getDbName();

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

	public DBInputConfig setJdbcFetchSize(Integer jdbcFetchSize) {
		_setJdbcFetchSize(  jdbcFetchSize);
		return  this;
	}


	public DBInputConfig setRemoveAbandoned(boolean removeAbandoned) {
		_setRemoveAbandoned( removeAbandoned);
		return  this;
	}



	public DBInputConfig setConnectionTimeout(int connectionTimeout) {
		_setConnectionTimeout( connectionTimeout);
		return  this;
	}



	public DBInputConfig setMaxWait(int maxWait) {
		_setMaxWait( maxWait);
		return  this;
	}


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

	public DBInputConfig setEnableDBTransaction(Boolean enableDBTransaction) {
		this.enableDBTransaction = enableDBTransaction;
		return this;
	}

	public Integer getJdbcFetchsize() {
		return jdbcFetchsize;
	}

	public DBInputConfig setJdbcFetchsize(Integer jdbcFetchsize) {
		this.jdbcFetchsize = jdbcFetchsize;
		return this;
	}
}
