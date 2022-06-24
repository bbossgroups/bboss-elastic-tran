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
import org.frameworkset.spi.DefaultApplicationContext;
import org.frameworkset.spi.assemble.GetProperties;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DefualtExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.StatementHandler;
import org.frameworkset.tran.db.output.TranSQLInfo;
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


	public String getTargetDbname() {
		return targetDbname;
	}

	public void setTargetDbname(String targetDbname) {
		this.targetDbname = targetDbname;
	}

	public void setStatementHandler(StatementHandler statementHandler) {
		this.statementHandler = statementHandler;
	}

	public StatementHandler getStatementHandler() {
		return statementHandler;
	}

	public void setInsertSqlName(String insertSqlName) {
		this.insertSqlName = insertSqlName;
	}


	public String getInsertSqlName() {
		return insertSqlName;

	}

	public String getInsertSql() {
		return insertSql;

	}

	public void setInsertSql(String insertSql) {
		this.insertSql = insertSql;
	}
	public DBConfig getTargetDBConfig() {

			return targetDBConfig;


	}

	public DBConfig getTargetDBConfig(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetDBConfig() != null)
			return taskContext.getTargetDBConfig();
		return getTargetDBConfig();
	}

	public void setTargetDBConfig(DBConfig targetDBConfig) {
		this.targetDBConfig = targetDBConfig;
	}

	public String getSql() {
		return sql;
	}


	public void setSql(String sql) {
		this.sql = sql;
	}



	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public void setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
	}



	public String getSqlName() {
		return sqlName;
	}

	public void setSqlName(String sqlName) {
		this.sqlName = sqlName;
	}

	public String getDeleteSql() {
		return deleteSql;
	}

	public void setDeleteSql(String deleteSql) {
		this.deleteSql = deleteSql;
	}

	public String getDeleteSqlName() {
		return deleteSqlName;
	}

	public void setDeleteSqlName(String deleteSqlName) {
		this.deleteSqlName = deleteSqlName;
	}

	public String getUpdateSql() {
		return updateSql;
	}

	public void setUpdateSql(String updateSql) {
		this.updateSql = updateSql;
	}

	public String getUpdateSqlName() {
		return updateSqlName;
	}

	public void setUpdateSqlName(String updateSqlName) {
		this.updateSqlName = updateSqlName;
	}

	public void setOptimize(boolean optimize) {
		this.optimize = optimize;
	}

	public boolean optimize() {
		return optimize;
	}

	public void setBatchHandler(BatchHandler batchHandler) {
		this.batchHandler = batchHandler;
	}

	public BatchHandler getBatchHandler() {
		return batchHandler;
	}

	@Override
	public void build(ImportBuilder importBuilder) {
		if(targetDBConfig == null) {
			GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml", false);
			String dbName = propertiesContainer.getExternalProperty("db.name");
			if (dbName == null || dbName.equals("")) {
				return;
			} else {

				if (targetDbname == null || targetDbname.equals("")) {
					targetDbname = dbName;
				}

			}
			if (dbConfig == null)
				dbConfig = new DBConfig();
			_buildDBConfig(propertiesContainer, dbName, dbConfig, "");
		}
		else{
			targetDbname = targetDBConfig.getDbName();
			dbConfig = targetDBConfig;
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

	public void setTargetSqlInfo(TranSQLInfo targetSqlInfo) {
		this.targetSqlInfo = targetSqlInfo;
	}

	public TranSQLInfo getTargetUpdateSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetUpdateSqlInfo() != null)
			return taskContext.getTargetUpdateSqlInfo();
		return targetUpdateSqlInfo;
	}

	public void setTargetUpdateSqlInfo(TranSQLInfo sqlInfo) {
		this.targetUpdateSqlInfo = sqlInfo;
	}
	public TranSQLInfo getTargetDeleteSqlInfo(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetDeleteSqlInfo() != null)
			return taskContext.getTargetDeleteSqlInfo();
		return targetDeleteSqlInfo;
	}

	public void setTargetDeleteSqlInfo(TranSQLInfo sqlInfo) {
		this.targetDeleteSqlInfo = sqlInfo;
	}
	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new DBOutputDataTranPlugin(  importContext);
	}
	@Override
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}

}
