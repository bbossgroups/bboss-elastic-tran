package org.frameworkset.tran.plugin.db.output;
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.plugin.db.BaseDBPlugin;
import org.frameworkset.tran.db.output.DBOutPutDataTran;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;

import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class DBOutputDataTranPlugin extends BaseDBPlugin implements OutputPlugin {
	/**
	 * 包含所有启动成功的db数据源
	 */
	private DBOutputConfig dbOutputConfig;
	public DBOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		dbOutputConfig = (DBOutputConfig) importContext.getOutputConfig();

	}
	@Override
	public void afterInit(){
//		if(sqlInfo != null
//				&& sqlInfo.getParamSize() > 0
//				&& !this.isIncreamentImport() && this.importContext.getParams() == null){
//			throw new TaskFailedException("1.Parameter variables cannot be set in non-increament import SQL statements："+dbContext.getSql() +"\r\n2.Parameter values must be setted by BaseImportBuilder.addParam(String,Object) method.");
//		}
//		this.externalTimer = this.importContext.isExternalTimer();
	}
	@Override
	public void beforeInit(){

//		super.init(importContext,  targetImportContext);
	}
	protected void initTargetDS2ndOtherDSes(){
		if(dbOutputConfig != null) {
			DBConfig targetDBConfig = dbOutputConfig.getTargetDBConfig();
			if (targetDBConfig != null) {
				DataTranPluginImpl.initDS(dbStartResult,targetDBConfig);
			}
		}
	}
	protected void initDSAndTargetSQLInfo(){
		DBConfig dbConfig = dbOutputConfig.getTargetDBConfig();
		String targetDBName = null;
		if(dbConfig != null) {

			targetDBName = dbConfig.getDbName();

		}
		if(targetDBName == null){
			targetDBName = dbOutputConfig.getTargetDbname();
		}

		TranUtil.initTargetSQLInfo(dbOutputConfig, targetDBName);
	}
//	protected void initDSAndTargetSQLInfo(DBOutPutContext db2DBContext){
//		DBConfig dbConfig = db2DBContext.getTargetDBConfig(null);
//		String targetDBName = null;
//		if(dbConfig != null) {
//			this.initDS(dbConfig);
//			targetDBName = dbConfig.getDbName();
//
//		}
//		else{
//			targetDBName =  db2DBContext.getTargetDBName(null);
//			if(targetDBName == null){
//				targetDBName = importContext.getTargetDBName();
//			}
//		}
//		TranUtil.initTargetSQLInfo(db2DBContext, targetDBName);
//	}

	@Override
	public void init() {
		initTargetDS2ndOtherDSes( );
		initDSAndTargetSQLInfo();
	}

	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, CountDownLatch countDownLatch, Status currentStatus){
		if(countDownLatch == null) {
			DBOutPutDataTran db2DBDataTran = new DBOutPutDataTran(taskContext, tranResultSet, importContext, currentStatus);
			db2DBDataTran.initTran();
			return db2DBDataTran;
		}
		else{
			AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,tranResultSet,importContext,   countDownLatch,  currentStatus);
			asynDBOutPutDataTran.initTran();
			return asynDBOutPutDataTran;
		}
	}




}
