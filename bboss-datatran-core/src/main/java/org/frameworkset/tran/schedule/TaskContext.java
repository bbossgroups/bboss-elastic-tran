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
import org.frameworkset.tran.db.output.TranSQLInfo;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;

import java.util.Date;
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
	private JobTaskMetrics jobTaskMetrics;
	/**
	 * 获取任务级别数据库配置
	 */
	public DBOutputConfig getDbmportConfig() {
		return dbmportConfig;
	}
	/**
	 * 设置任务级别数据库配置
	 */
	public void setDbmportConfig(DBOutputConfig dbmportConfig) {
		this.dbmportConfig = dbmportConfig;
	}

	/**
	 * 设置任务级别数据库配置，适用场景，不同文件到不同数据库表输出插件
	 */
	private DBOutputConfig dbmportConfig;
	private TranSQLInfo targetSqlInfo;
	private TranSQLInfo targetUpdateSqlInfo;
	private TranSQLInfo targetDeleteSqlInfo;
	public TaskContext(ImportContext importContext){
		this.importContext = importContext;
		taskDatas = new HashMap<String, Object>();
		jobTaskMetrics = importContext.createJobTaskMetrics();
	}
	private ImportContext importContext;
	public void await(){
		jobTaskMetrics.await();
	}
	public void await(long waitTime){
		jobTaskMetrics.await(waitTime);
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

	public ImportContext getImportContext() {
		return importContext;
	}
	public void release(){
		this.taskDatas.clear();
		this.taskDatas = null;
	}


	public String getSql() {
		if(dbmportConfig != null)
			return dbmportConfig.getSql();
		return null;
	}


	public String getSqlFilepath() {
		if(dbmportConfig != null)
			return dbmportConfig.getSqlFilepath();
		return null;
	}

	public String getSqlName() {
		if(dbmportConfig != null)
			return dbmportConfig.getSqlName();
		return null;
	}

	public String getInsertSqlName() {
		if(dbmportConfig != null)
			return dbmportConfig.getInsertSqlName();
		return null;
	}
	public String getInsertSql() {
		if(dbmportConfig != null)
			return dbmportConfig.getInsertSql();
		return null;
	}

	public String getDeleteSqlName() {
		if(dbmportConfig != null)
			return dbmportConfig.getDeleteSqlName();
		return null;
	}
	public String getDeleteSql(){
		if(dbmportConfig != null)
			return dbmportConfig.getDeleteSql();
		return null;
	}

	public String getUpdateSqlName() {
		if(dbmportConfig != null)
			return dbmportConfig.getUpdateSqlName();
		return null;
	}
	public String getUpdateSql(){
		if(dbmportConfig != null)
			return dbmportConfig.getUpdateSql();
		return null;
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

	public synchronized void beginTask(TaskMetrics taskMetrics){

		jobTaskMetrics.increamentTasks();
	}

	/**
	 * 添加任务统计数据
	 * @param taskMetrics
	 */
	public synchronized void finishTaskMetrics(TaskMetrics taskMetrics){

		jobTaskMetrics.increamentFailedRecords(taskMetrics.getFailedRecords());
		jobTaskMetrics.increamentIgnoreRecords(taskMetrics.getIgnoreRecords());
		jobTaskMetrics.increamentRecords(taskMetrics.getRecords());
		jobTaskMetrics.increamentSuccessRecords(taskMetrics.getSuccessRecords());
		jobTaskMetrics.putLastValue(importContext.getLastValueType(),taskMetrics.getLastValue());


	}
	public void setJobEndTime(Date jobEndTime) {
		jobTaskMetrics.setJobEndTime(jobEndTime);
	}

	public JobTaskMetrics getJobTaskMetrics() {
		return jobTaskMetrics;
	}
}
