package org.frameworkset.tran.es.input.db;
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

import com.frameworkset.common.poolman.ConfigSQLExecutor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.es.input.ESInputPlugin;
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
public class ES2DBDataTranPlugin extends ESInputPlugin implements DataTranPlugin {

	protected ConfigSQLExecutor executor;

	protected DBOutPutContext dbOutPutContext;
	public ES2DBDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		super(  importContext,  targetImportContext);

		dbOutPutContext = (DBOutPutContext) targetImportContext;
	}

	@Override
	public void beforeInit() {
		super.beforeInit();
		if(dbOutPutContext.getTargetDBConfig() != null)
		{
			this.initDS(dbOutPutContext.getTargetDBConfig());
		}
		if(importContext.getDbConfig() != null) {
			this.initDS(importContext.getDbConfig());
		}
		initOtherDSes(importContext.getConfigs());

	}
	@Override
	public void afterInit(){
		executor = TranUtil.initTargetSQLInfo(dbOutPutContext,dbOutPutContext.getTargetDBConfig() != null?dbOutPutContext.getTargetDBConfig():importContext.getDbConfig());

	}
	protected  BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, CountDownLatch countDownLatch, Status currentStatus){
		AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,jdbcResultSet,importContext,   targetImportContext,countDownLatch,  currentStatus);
		asynDBOutPutDataTran.init();
		return asynDBOutPutDataTran;
	}
	public void doImportData(TaskContext taskContext)  throws ESDataImportException{
		if(dbOutPutContext.getBatchHandler() != null){
			doBatchHandler(  taskContext);
		}
		else{
			super.doImportData( taskContext);
		}
	}
	protected  void doBatchHandler(TaskContext taskContext){
		ESDirectExporterScrollHandler esDirectExporterScrollHandler = new ESDirectExporterScrollHandler(importContext,targetImportContext,
				executor);
		try {
			if (!isIncreamentImport()) {

				commonImportData(  taskContext,esDirectExporterScrollHandler);

			} else {

				increamentImportData( taskContext,esDirectExporterScrollHandler);

			}
		} catch (ESDataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new ESDataImportException(e);
		}
	}



}
