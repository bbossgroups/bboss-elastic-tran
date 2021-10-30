package org.frameworkset.tran.task;
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

import org.frameworkset.elasticsearch.bulk.BulkConfig;
import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.client.ClientUtil;
import org.frameworkset.elasticsearch.handler.ESVoidResponseHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskCommandImpl extends BaseTaskCommand<String,String> {

	public TaskCommandImpl(ImportCount importCount, ImportContext importContext, ImportContext targetImportContext,
						   long dataSize, int taskNo, String jobNo, Object lastValue, Status currentStatus, boolean reachEOFClosed, TaskContext taskContext) {
		super(importCount,importContext,  targetImportContext,  dataSize,  taskNo,  jobNo,  lastValue,  currentStatus,reachEOFClosed,  taskContext);
	}




	private ClientInterface clientInterface;



	public ClientInterface getClientInterface() {
		return clientInterface;
	}

	public String getDatas() {
		return datas;
	}


	private String datas;
	private int tryCount;



	public void setClientInterface(ClientInterface clientInterface) {
		this.clientInterface = clientInterface;
	}

	public void setDatas(String datas) {
		this.datas = datas;
	}


	private static Logger logger = LoggerFactory.getLogger(TaskCommand.class);

	public String execute(){
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;
		if(importContext.isDebugResponse()) {

			data = clientInterface.executeHttp(BuildTool.buildActionUrl(targetImportContext.getClientOptions(), BulkConfig.ERROR_FILTER_PATH), datas, ClientUtil.HTTP_POST);
			finishTask();
			if(logger.isInfoEnabled())
				logger.info(data);

		}
		else{
			if(importContext.isDiscardBulkResponse() && importContext.getExportResultHandler() == null) {
				ESVoidResponseHandler esVoidResponseHandler = new ESVoidResponseHandler();
				clientInterface.executeHttp(BuildTool.buildActionUrl(targetImportContext.getClientOptions(), BulkConfig.ERROR_FILTER_PATH), datas, ClientUtil.HTTP_POST,esVoidResponseHandler);
				if(esVoidResponseHandler.getElasticSearchException() != null)
					throw esVoidResponseHandler.getElasticSearchException();
				finishTask();
				return null;
			}
			else{
				data = clientInterface.executeHttp(BuildTool.buildActionUrl(targetImportContext.getClientOptions(), BulkConfig.ERROR_FILTER_PATH), datas, ClientUtil.HTTP_POST);
				finishTask();
			}
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
