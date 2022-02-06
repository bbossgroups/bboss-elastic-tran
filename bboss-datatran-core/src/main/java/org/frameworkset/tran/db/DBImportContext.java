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

import com.frameworkset.common.poolman.BatchHandler;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.db.output.TranSQLInfo;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public  class DBImportContext extends BaseImportContext implements DBOutPutContext {
	protected DBImportConfig dbImportConfig;
	public DBConfig getTargetDBConfig(TaskContext taskContext) {
		if(taskContext != null && taskContext.getTargetDBConfig() != null)
			return taskContext.getTargetDBConfig();
		return dbImportConfig.getTargetDBConfig();
	}
	public boolean optimize()
	{
		return dbImportConfig.optimize();
	}
	public BatchHandler getBatchHandler(){
		return dbImportConfig.getBatchHandler();
	}
	private TranSQLInfo targetSqlInfo;
	private TranSQLInfo targetUpdateSqlInfo;
	private TranSQLInfo targetDeleteSqlInfo;
	@Override
	public void init(){
		dbImportConfig = (DBImportConfig)baseImportConfig;
	}
	public DBImportContext(){
		super(new DBImportConfig());
	}
	public DBImportContext(BaseImportConfig baseImportConfig){
		super(baseImportConfig);

	}



	@Override
	public String getSql() {
		return dbImportConfig.getSql();
	}


	@Override
	public String getSqlFilepath() {
		return dbImportConfig.getSqlFilepath();
	}

	@Override
	public String getSqlName() {
		return dbImportConfig.getSqlName();
	}

	@Override
	public void setSql(String sql) {
		dbImportConfig.setSql(sql);
	}

	@Override
	public String getInsertSqlName() {
		return dbImportConfig.getInsertSqlName();
	}
	@Override
	public String getInsertSql() {
		return dbImportConfig.getInsertSql();
	}

	public String getDeleteSqlName() {
		return dbImportConfig.getDeleteSqlName();
	}
	public String getDeleteSql(){
		return dbImportConfig.getDeleteSql();
	}

	public String getUpdateSqlName() {
		return dbImportConfig.getUpdateSqlName();
	}
	public String getUpdateSql(){
		return dbImportConfig.getUpdateSql();
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



}
