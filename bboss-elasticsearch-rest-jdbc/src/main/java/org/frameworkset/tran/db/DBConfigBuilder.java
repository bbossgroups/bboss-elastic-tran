package org.frameworkset.tran.db;
/**
 * Copyright 2020 bboss
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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/21 10:15
 * @author biaoping.yin
 * @version 1.0
 */
public class DBConfigBuilder {
	private DBConfig targetDBConfig;
	private String dbName;
	protected String sqlFilepath;
	protected String sqlName;
	protected String sql;

	private String insertSqlName;
	private String insertSql;
	private String updateSqlName;
	private boolean optimize;
	private String updateSql;
	private String deleteSqlName;
	private String deleteSql;

	 
	private void checkTargetDBConfig(){
		if(targetDBConfig == null)
			targetDBConfig = new DBConfig();
	}
	public String getInsertSqlName() {
		return insertSqlName;
	}

	public DBConfigBuilder setInsertSqlName(String insertSqlName) {
		this.insertSqlName = insertSqlName;
		return this;
	}
	public String getInsertSql() {
		return insertSql;
	}

	public DBConfigBuilder setInsertSql(String insertSql) {
		this.insertSql = insertSql;
		return this;
	}
	public DBImportConfig buildDBImportConfig(){
//		dbImportConfig.setSqlFilepath(sqlFilepath);
//		dbImportConfig.setSqlName(sqlName);
//		dbImportConfig.setSql(this.sql);
//		dbImportConfig.setInsertSql(this.insert);
		DBImportConfig dbImportConfig = new DBImportConfig();
		dbImportConfig.setUseJavaName(false);
		dbImportConfig.setTargetDBConfig(this.targetDBConfig);
		dbImportConfig.setSqlFilepath(this.sqlFilepath);
		dbImportConfig.setSqlName(sqlName);
		if(SimpleStringUtil.isNotEmpty(sql))
			dbImportConfig.setSql(this.sql);
		dbImportConfig.setInsertSqlName(this.insertSqlName);
		dbImportConfig.setInsertSql(this.insertSql);
		dbImportConfig.setUpdateSql(updateSql);
		dbImportConfig.setUpdateSqlName(updateSqlName);
		dbImportConfig.setDeleteSql(deleteSql);
		dbImportConfig.setDeleteSqlName(deleteSqlName);
		dbImportConfig.setOptimize(optimize);

		return dbImportConfig;
	}




	public String getSql() {
		return sql;
	}

	public DBConfigBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}


	public String getSqlName() {
		return sqlName;
	}

	public DBConfigBuilder setSqlName(String sqlName) {
		this.sqlName = sqlName;
		return this;
	}



	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public DBConfigBuilder setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
		return this;
	}

	public String getDeleteSql() {
		return deleteSql;
	}

	public DBConfigBuilder setDeleteSql(String deleteSql) {
		this.deleteSql = deleteSql;
		return this;
	}

	public String getDeleteSqlName() {
		return deleteSqlName;
	}

	public DBConfigBuilder setDeleteSqlName(String deleteSqlName) {
		this.deleteSqlName = deleteSqlName;
		return this;
	}

	public String getUpdateSql() {
		return updateSql;
	}

	public DBConfigBuilder setUpdateSql(String updateSql) {
		this.updateSql = updateSql;
		return this;
	}

	public String getUpdateSqlName() {
		return updateSqlName;
	}

	public DBConfigBuilder setUpdateSqlName(String updateSqlName) {
		this.updateSqlName = updateSqlName;
		return this;
	}

	public boolean isOptimize() {
		return optimize;
	}

	/**
	 * 是否在批处理时，将insert、update、delete记录分组排序
	 * true：分组排序，先执行insert、在执行update、最后执行delete操作
	 * false：按照原始顺序执行db操作，默认值false
	 * @param optimize
	 * @return
	 */
	public DBConfigBuilder setOptimize(boolean optimize) {
		this.optimize = optimize;
		return this;
	}
	public DBConfigBuilder setTargetDbDriver(String targetDbDriver) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbDriver(targetDbDriver);
		return this;
	}

	public DBConfigBuilder setTargetDbUrl(String targetDbUrl) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbUrl(targetDbUrl);
		return this;
	}

	public DBConfigBuilder setTargetDbUser(String targetDbUser) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbUser(targetDbUser);
		return this;
	}

	public DBConfigBuilder setTargetDbPassword(String targetDbPassword) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbPassword(targetDbPassword);
		return this;
	}

	public DBConfigBuilder setTargetInitSize(int targetInitSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setInitSize(targetInitSize);
		return this;
	}

	public DBConfigBuilder setTargetMinIdleSize(int targetMinIdleSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setMinIdleSize(targetMinIdleSize);
		return this;
	}

	public DBConfigBuilder setTargetMaxSize(int targetMaxSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setMaxSize(targetMaxSize);
		return this;
	}

	public DBConfigBuilder setTargetDbName(String targetDbName) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbName(targetDbName);
		return this;
	}

	public DBConfigBuilder setTargetShowSql(boolean targetShowSql) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setShowSql(targetShowSql);
		return this;
	}

	public DBConfigBuilder setTargetUsePool(boolean targetUsePool) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setUsePool(targetUsePool);
		return this;
	}

	public DBConfigBuilder setTargetDbtype(String targetDbtype) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbtype(targetDbtype);
		return this;
	}

	public DBConfigBuilder setTargetDbAdaptor(String targetDbAdaptor) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbAdaptor(targetDbAdaptor);
		return this;
	}




	public DBConfigBuilder setTargetValidateSQL(String validateSQL) {
		this.checkTargetDBConfig();
		targetDBConfig.setValidateSQL(validateSQL);
		return this;
	}
}
