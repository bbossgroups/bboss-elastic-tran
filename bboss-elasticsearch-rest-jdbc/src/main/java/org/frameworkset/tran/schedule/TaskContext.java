package org.frameworkset.tran.schedule;
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

import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBImportConfig;
import org.frameworkset.tran.db.output.TranSQLInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/10/15 20:56
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskContext {
	private Map<String,Object> taskDatas;
	/**
	 * 获取任务级别数据库配置
	 */
	public DBImportConfig getDbmportConfig() {
		return dbmportConfig;
	}
	/**
	 * 设置任务级别数据库配置
	 */
	public void setDbmportConfig(DBImportConfig dbmportConfig) {
		this.dbmportConfig = dbmportConfig;
	}

	/**
	 * 设置任务级别数据库配置，适用场景，不同文件到不同数据库表输出插件
	 */
	private DBImportConfig dbmportConfig;
	private TranSQLInfo targetSqlInfo;
	private TranSQLInfo targetUpdateSqlInfo;
	private TranSQLInfo targetDeleteSqlInfo;
	public TaskContext(ImportContext importContext,ImportContext targetImportContext){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
		taskDatas = new HashMap<String, Object>();
	}
	private ImportContext importContext;

	public ImportContext getTargetImportContext() {
		return targetImportContext;
	}
	public TaskContext addTaskData(String name,Object value){
		taskDatas.put(name,value);
		return this;
	}
	public TaskContext addTaskDatas(Map<String,Object> taskDatas){
		this.taskDatas.putAll(taskDatas);
		return this;
	}
	public Object getTaskData(String name){
		return taskDatas.get(name);
	}
	public void setTargetImportContext(ImportContext targetImportContext) {
		this.targetImportContext = targetImportContext;
	}

	private ImportContext targetImportContext;
	public ImportContext getImportContext() {
		return importContext;
	}
	public void release(){
		this.taskDatas.clear();
		this.taskDatas = null;
	}


	public String getSql() {
		return dbmportConfig.getSql();
	}


	public String getSqlFilepath() {
		return dbmportConfig.getSqlFilepath();
	}

	public String getSqlName() {
		return dbmportConfig.getSqlName();
	}

	public void setSql(String sql) {
		dbmportConfig.setSql(sql);
	}

	public String getInsertSqlName() {
		return dbmportConfig.getInsertSqlName();
	}
	public String getInsertSql() {
		return dbmportConfig.getInsertSql();
	}

	public String getDeleteSqlName() {
		return dbmportConfig.getDeleteSqlName();
	}
	public String getDeleteSql(){
		return dbmportConfig.getDeleteSql();
	}

	public String getUpdateSqlName() {
		return dbmportConfig.getUpdateSqlName();
	}
	public String getUpdateSql(){
		return dbmportConfig.getUpdateSql();
	}

	public TranSQLInfo getTargetSqlInfo() {
		return targetSqlInfo;
	}

	public void setTargetSqlInfo(TranSQLInfo targetSqlInfo) {
		this.targetSqlInfo = targetSqlInfo;
	}

	public TranSQLInfo getTargetUpdateSqlInfo() {
		return targetUpdateSqlInfo;
	}

	public void setTargetUpdateSqlInfo(TranSQLInfo sqlInfo) {
		this.targetUpdateSqlInfo = sqlInfo;
	}
	public TranSQLInfo getTargetDeleteSqlInfo() {
		return targetDeleteSqlInfo;
	}

	public void setTargetDeleteSqlInfo(TranSQLInfo sqlInfo) {
		this.targetDeleteSqlInfo = sqlInfo;
	}
	public DBConfig getTargetDBConfig() {
		if(dbmportConfig != null) {
			return dbmportConfig.getTargetDBConfig();
		}
		else{
			return null;
		}
	}
}
