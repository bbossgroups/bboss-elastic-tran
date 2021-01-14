package org.frameworkset.tran.db.input.es;
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

import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.client.ClientUtil;
import org.frameworkset.elasticsearch.handler.ESVoidResponseHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
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
//	private String refreshOption;

	public TaskCommandImpl(ImportCount importCount, ImportContext importContext, long dataSize, int taskNo, String jobNo) {
		super(importCount,importContext,  dataSize,  taskNo,  jobNo);
	}




	private ClientInterface clientInterface;

//	public String getRefreshOption() {
//		return refreshOption;
//	}

	public ClientInterface getClientInterface() {
		return clientInterface;
	}

	public String getDatas() {
		return datas;
	}


	private String datas;
	private int tryCount;

//	public void setRefreshOption(String refreshOption) {
//		this.refreshOption = refreshOption;
//	}

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

			data = clientInterface.executeHttp(BuildTool.buildActionUrl(importContext.getClientOptions()), datas, ClientUtil.HTTP_POST);
			if(logger.isInfoEnabled())
				logger.info(data);

		}
		else{
			if(importContext.isDiscardBulkResponse() && importContext.getExportResultHandler() == null) {
				ESVoidResponseHandler esVoidResponseHandler = new ESVoidResponseHandler();
				clientInterface.executeHttp(BuildTool.buildActionUrl(importContext.getClientOptions()), datas, ClientUtil.HTTP_POST,esVoidResponseHandler);
				if(esVoidResponseHandler.getElasticSearchException() != null)
					throw esVoidResponseHandler.getElasticSearchException();
				return null;
			}
			else{
				data = clientInterface.executeHttp(BuildTool.buildActionUrl(importContext.getClientOptions()), datas, ClientUtil.HTTP_POST);
			}
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
