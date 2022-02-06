package org.frameworkset.tran.db;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class DBExportBuilder extends BaseImportBuilder {

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
	public DBExportBuilder setShowSql(boolean showSql) {
		_setShowSql(showSql);

		return this;
	}
	protected ImportContext buildImportContext(BaseImportConfig importConfig){
		DBImportContext dbImportContext = new DBImportContext(importConfig);
		dbImportContext.init();
		return dbImportContext;
	}
	public String getInsertSqlName() {
		return insertSqlName;
	}

	public DBExportBuilder setInsertSqlName(String insertSqlName) {
		this.insertSqlName = insertSqlName;
		return this;
	}
	public String getInsertSql() {
		return insertSql;
	}

	public DBExportBuilder setInsertSql(String insertSql) {
		this.insertSql = insertSql;
		return this;
	}
	protected void buildDBImportConfig(DBImportConfig dbImportConfig){
//		dbImportConfig.setSqlFilepath(sqlFilepath);
//		dbImportConfig.setSqlName(sqlName);
//		dbImportConfig.setSql(this.sql);
//		dbImportConfig.setInsertSql(this.insert);

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
	}




	public String getSql() {
		return sql;
	}

	public DBExportBuilder setSql(String sql) {
		this.sql = sql;
		return this;
	}


	public String getSqlName() {
		return sqlName;
	}

	public DBExportBuilder setSqlName(String sqlName) {
		this.sqlName = sqlName;
		return this;
	}



	public String getSqlFilepath() {
		return sqlFilepath;
	}

	public DBExportBuilder setSqlFilepath(String sqlFilepath) {
		this.sqlFilepath = sqlFilepath;
		return this;
	}

	public String getDeleteSql() {
		return deleteSql;
	}

	public DBExportBuilder setDeleteSql(String deleteSql) {
		this.deleteSql = deleteSql;
		return this;
	}

	public String getDeleteSqlName() {
		return deleteSqlName;
	}

	public DBExportBuilder setDeleteSqlName(String deleteSqlName) {
		this.deleteSqlName = deleteSqlName;
		return this;
	}

	public String getUpdateSql() {
		return updateSql;
	}

	public DBExportBuilder setUpdateSql(String updateSql) {
		this.updateSql = updateSql;
		return this;
	}

	public String getUpdateSqlName() {
		return updateSqlName;
	}

	public DBExportBuilder setUpdateSqlName(String updateSqlName) {
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
	public DBExportBuilder setOptimize(boolean optimize) {
		this.optimize = optimize;
		return this;
	}
}
