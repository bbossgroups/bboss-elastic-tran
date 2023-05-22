package org.frameworkset.tran.plugin.hbase.output;
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


import org.frameworkset.nosql.hbase.HBaseHelperFactory;
import org.frameworkset.nosql.hbase.HBaseResourceStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;

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
public class HBaseOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	/**
	 * 包含所有启动成功的db数据源
	 */
	private HBaseOutputConfig hBaseOutputConfig;
	private ResourceStartResult resourceStartResult = new HBaseResourceStartResult();
	public HBaseOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		hBaseOutputConfig = (HBaseOutputConfig) importContext.getOutputConfig();

	}

	@Override
	public void afterInit() {

	}

	@Override
	public void beforeInit(){
		initHBase();

	}


	protected void initHBase() {

		boolean build = HBaseHelperFactory.buildHBaseClient(hBaseOutputConfig);

		if(build)
		{
			this.resourceStartResult.addResourceStartResult(hBaseOutputConfig.getName());
		}

	}


	@Override
	public void init() {

	}

	@Override

	public void destroy(boolean waitTranStop) {
		Map<String,Object> dbs = resourceStartResult.getResourceStartResult();
		if (dbs != null && dbs.size() > 0){
			Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<String, Object> entry = iterator.next();
				HBaseHelperFactory.destroy(entry.getKey());
			}
		}
	}

	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		if(countDownLatch == null) {
			HBaseOutPutDataTran db2DBDataTran = new HBaseOutPutDataTran(taskContext, tranResultSet, importContext, currentStatus);
			db2DBDataTran.initTran();
			return db2DBDataTran;
		}
		else{
			HBaseOutPutDataTran asynDBOutPutDataTran = new HBaseOutPutDataTran(  taskContext,tranResultSet,importContext,    currentStatus,countDownLatch);
			asynDBOutPutDataTran.initTran();
			return asynDBOutPutDataTran;
		}
	}




}
