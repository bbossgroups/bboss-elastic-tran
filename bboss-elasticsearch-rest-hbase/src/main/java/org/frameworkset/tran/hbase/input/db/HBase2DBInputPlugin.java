package org.frameworkset.tran.hbase.input.db;
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

import org.apache.hadoop.hbase.client.ResultScanner;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.hbase.HBaseInputPlugin;
import org.frameworkset.tran.hbase.HBaseResultSet;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * hbase to dabase tran plugin
 */
public class HBase2DBInputPlugin extends HBaseInputPlugin {

	protected DBOutPutContext dbOutPutContext;
	public HBase2DBInputPlugin(ImportContext importContext, ImportContext targetImportContext){
		super(  importContext,  targetImportContext);
		dbOutPutContext = (DBOutPutContext) targetImportContext;

	}

	@Override
	public void beforeInit() {
//		if(importContext.getDbConfig() != null)
//			this.initDS(importContext.getDbConfig());
		initSourceDatasource();
//		DBConfig dbConfig = dbOutPutContext.getTargetDBConfig(null);
//		if(dbConfig != null) {
//			this.initDS(dbConfig);
//			TranUtil.initTargetSQLInfo(dbOutPutContext, dbConfig.getDbName());
//		}
//		else{
//			dbConfig = importContext.getDbConfig();
//			if(dbConfig != null)
//				TranUtil.initTargetSQLInfo(dbOutPutContext, dbConfig.getDbName());
//		}
		initDSAndTargetSQLInfo(dbOutPutContext,true);
		super.beforeInit();

	}

	@Override
	protected void doTran(ResultScanner rs, TaskContext taskContext) {
		HBaseResultSet hBaseResultSet = new HBaseResultSet(importContext,rs);
		AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,hBaseResultSet,importContext,   targetImportContext,  currentStatus);
		asynDBOutPutDataTran.init();
		asynDBOutPutDataTran.tran();
	}


}
