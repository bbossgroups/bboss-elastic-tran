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
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.db.input.StatementHandler;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBConfig;
import org.frameworkset.tran.schedule.TaskContext;

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
	protected BatchHandler batchHandler;
	protected StatementHandler statementHandler;
	private TranSQLInfo targetSqlInfo;
	private TranSQLInfo targetUpdateSqlInfo;
	private TranSQLInfo targetDeleteSqlInfo;
	private String targetDbname;


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
	public void build(ImportBuilder importBuilder) {
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

	public DBOutputConfig setTargetSqlInfo(TranSQLInfo targetSqlInfo) {
		this.targetSqlInfo = targetSqlInfo;
		return this;
	}

	public TranSQLInfo getTargetUpdateSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetUpdateSqlInfo() != null)
			return taskContext.getTargetUpdateSqlInfo();
		return targetUpdateSqlInfo;
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

	public DBOutputConfig setTargetDeleteSqlInfo(TranSQLInfo sqlInfo) {
		this.targetDeleteSqlInfo = sqlInfo;
		return this;
	}
	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new DBOutputDataTranPlugin(  importContext);
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

	public DBOutputConfig setJdbcFetchSize(Integer jdbcFetchSize) {
		_setJdbcFetchSize(  jdbcFetchSize);
		return  this;
	}

}
