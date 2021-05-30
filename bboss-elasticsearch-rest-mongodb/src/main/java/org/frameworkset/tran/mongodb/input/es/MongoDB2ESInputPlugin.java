package org.frameworkset.tran.mongodb.input.es;
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

import com.mongodb.DBCursor;
import org.frameworkset.tran.BaseElasticsearchDataTran;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.MongoDBResultSet;
import org.frameworkset.tran.mongodb.input.MongoDBInputPlugin;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 22:22
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDB2ESInputPlugin extends MongoDBInputPlugin {
	public MongoDB2ESInputPlugin(ImportContext importContext, ImportContext targetImportContext) {
		super(  importContext,   targetImportContext);
	}
	@Override
	public void beforeInit() {
		this.initES(importContext.getApplicationPropertiesFile());
		initOtherDSes(importContext.getConfigs());
		super.beforeInit();



	}
	@Override
	protected void doTran(DBCursor dbCursor, TaskContext taskContext) {
		MongoDBResultSet mongoDB2ESResultSet = new MongoDBResultSet(importContext,dbCursor);
		BaseElasticsearchDataTran mongoDB2ESDataTran = new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
		mongoDB2ESDataTran.init();
		mongoDB2ESDataTran.tran();
	}
}
