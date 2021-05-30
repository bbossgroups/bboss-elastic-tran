package org.frameworkset.tran.hbase.input;
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
import org.frameworkset.tran.BaseElasticsearchDataTran;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.hbase.HBaseInputPlugin;
import org.frameworkset.tran.hbase.HBaseResultSet;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2ESInputPlugin extends HBaseInputPlugin {
	public HBase2ESInputPlugin(ImportContext importContext, ImportContext targetImportContext) {
		super(importContext, targetImportContext);
	}

	@Override
	protected void doTran(ResultScanner rs,TaskContext taskContext) {
//		MongoDBResultSet mongoDB2ESResultSet = new MongoDBResultSet(importContext,dbCursor);
//		MongoDB2ESDataTran mongoDB2ESDataTran = new MongoDB2ESDataTran(mongoDB2ESResultSet,importContext);
//		mongoDB2ESDataTran.tran();
		HBaseResultSet hBaseResultSet = new HBaseResultSet(importContext,rs);
		BaseElasticsearchDataTran hBase2ESDataTran = new BaseElasticsearchDataTran(taskContext,hBaseResultSet,importContext,targetImportContext,this.currentStatus);
		hBase2ESDataTran.init();
		hBase2ESDataTran.tran();
	}
	@Override
	public void beforeInit() {
		this.initES(importContext.getApplicationPropertiesFile());
		initOtherDSes(importContext.getConfigs());
		super.beforeInit();



	}

}
