package org.frameworkset.tran.kafka.input.db;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.kafka.input.Kafka2InputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 22:22
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2DBInputPlugin  extends Kafka2InputPlugin {
	protected DBOutPutContext dbOutPutContext;
	public Kafka2DBInputPlugin(ImportContext importContext,ImportContext targetImportContext){
		super(  importContext,  targetImportContext);
		dbOutPutContext = (DBOutPutContext) targetImportContext;

	}
	@Override
	public void beforeInit() {
//		this.initES(importContext.getApplicationPropertiesFile());
//		initOtherDSes(importContext.getConfigs());
//		if(importContext.getDbConfig() != null)
//			this.initDS(importContext.getDbConfig());
		initSourceDatasource();
//		DBConfig dbConfig = dbOutPutContext.getTargetDBConfig(null);
//		if(dbConfig != null) {
//			this.initDS(dbConfig);
////		initOtherDSes(importContext.getConfigs());
//			TranUtil.initTargetSQLInfo(dbOutPutContext, dbConfig.getDbName());
//		}
//		else{
//			TranUtil.initTargetSQLInfo(dbOutPutContext, importContext.getDbConfig().getDbName());
//		}
		initTargetDS2ndOtherDSes( dbOutPutContext);
		initDSAndTargetSQLInfo(dbOutPutContext,false);
		super.beforeInit();
	}
	protected  BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, Status currentStatus) {
		AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		asynDBOutPutDataTran.initTran();
		return asynDBOutPutDataTran;

	}

}
