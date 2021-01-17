package org.frameworkset.tran.db;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.config.BaseImportConfig;

public class DBImportConfig extends BaseImportConfig {
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
		if(targetDBConfig != null) {
			return targetDBConfig;
		}
		else{
			return super.getDbConfig();
		}
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
}
