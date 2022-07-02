package org.frameworkset.tran.plugin.http.output;
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

import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;

import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	protected HttpOutputConfig httpOutputConfig ;
	private ResourceStartResult resourceStartResult ;
	public HttpOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();

	}

	@Override
	public void afterInit() {

	}

	@Override
	public void beforeInit() {

	}


	@Override
	public void init() {
		if(httpOutputConfig != null && httpOutputConfig.getHttpConfigs() != null){
			resourceStartResult = HttpRequestProxy.startHttpPools(httpOutputConfig.getHttpConfigs());
		}
	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(resourceStartResult != null){
			HttpRequestProxy.stopHttpClients(resourceStartResult);


		}
	}






	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, CountDownLatch countDownLatch, Status currentStatus){
		BaseDataTran db2ESDataTran = new HttpOutPutDataTran(  taskContext,   tranResultSet,   importContext,   countDownLatch,   currentStatus);
		db2ESDataTran.initTran();
		return db2ESDataTran;
	}


}
