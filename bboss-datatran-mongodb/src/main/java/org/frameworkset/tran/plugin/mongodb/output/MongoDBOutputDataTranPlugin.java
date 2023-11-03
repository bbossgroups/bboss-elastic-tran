package org.frameworkset.tran.plugin.mongodb.output;
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

import org.frameworkset.nosql.mongodb.MongoDBConfig;
import org.frameworkset.nosql.mongodb.MongoDBHelper;
import org.frameworkset.nosql.mongodb.MongoDBStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Iterator;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	/**
	 * 包含所有启动成功的db数据源
	 */
	private MongoDBOutputConfig mongoDBOutputConfig;

	private MongoDBStartResult mongoDBStartResult = new MongoDBStartResult();
	public MongoDBOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		mongoDBOutputConfig = (MongoDBOutputConfig) importContext.getOutputConfig();

	}

	@Override
	public void afterInit() {

	}

	@Override
	public void beforeInit(){
		initMongoDB();

	}


	protected void initMongoDB(){
		MongoDBConfig mongoDBConfig = new MongoDBConfig();
		mongoDBConfig.setName(mongoDBOutputConfig.getName());
		mongoDBConfig.setCredentials(mongoDBOutputConfig.getCredentials());
		mongoDBConfig.setServerAddresses(mongoDBOutputConfig.getServerAddresses());
		mongoDBConfig.setOption(mongoDBOutputConfig.getOption());//private String option;
		mongoDBConfig.setWriteConcern(mongoDBOutputConfig.getWriteConcern());//private String writeConcern;
		mongoDBConfig.setReadPreference(mongoDBOutputConfig.getReadPreference());//private String readPreference;

		mongoDBConfig.setConnectionsPerHost(mongoDBOutputConfig.getConnectionsPerHost());//private int connectionsPerHost = 50;

		mongoDBConfig.setMaxWaitTime(mongoDBOutputConfig.getMaxWaitTime());//private int maxWaitTime = 120000;
		mongoDBConfig.setSocketTimeout(mongoDBOutputConfig.getSocketTimeout());//private int socketTimeout = 0;
		mongoDBConfig.setConnectTimeout(mongoDBOutputConfig.getConnectTimeout());//private int connectTimeout = 15000;


		mongoDBConfig.setSocketKeepAlive(mongoDBOutputConfig.getSocketKeepAlive());//private Boolean socketKeepAlive = false;

		mongoDBConfig.setMode( mongoDBOutputConfig.getMode());

		mongoDBConfig.setConnectString(mongoDBOutputConfig.getConnectString());
		if(MongoDBHelper.init(mongoDBConfig)){
			mongoDBStartResult.addDBStartResult(mongoDBConfig.getName());
		}
	}


	@Override
	public void init() {

	}

	@Override

	public void destroy(boolean waitTranStop) {
		Map<String,Object> dbs = mongoDBStartResult.getDbstartResult();
		if (dbs != null && dbs.size() > 0){
			Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<String, Object> entry = iterator.next();
				MongoDBHelper.closeDB(entry.getKey());
			}
		}
	}

	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		if(countDownLatch == null) {
			MongoDBOutPutDataTran db2DBDataTran = new MongoDBOutPutDataTran(taskContext, tranResultSet, importContext, currentStatus);
			db2DBDataTran.initTran();
			return db2DBDataTran;
		}
		else{
			MongoDBOutPutDataTran asynDBOutPutDataTran = new MongoDBOutPutDataTran(  taskContext,tranResultSet,importContext,    currentStatus,countDownLatch);
			asynDBOutPutDataTran.initTran();
			return asynDBOutPutDataTran;
		}
	}




}
