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
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
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
	private ElasticsearchOutputConfig elasticsearchOutputConfig;
	public TaskCommandImpl(ImportCount importCount, ImportContext importContext, ElasticsearchOutputConfig elasticsearchOutputConfig ,
						   long dataSize, int taskNo, String jobNo, Object lastValue, Status currentStatus, boolean reachEOFClosed, TaskContext taskContext) {
		super(importCount,importContext,    dataSize,  taskNo,  jobNo,  lastValue,  currentStatus,reachEOFClosed,  taskContext);
		this.elasticsearchOutputConfig = elasticsearchOutputConfig;
	}




	private ClientInterface[] clientInterfaces;



	public ClientInterface[] getClientInterfaces() {
		return clientInterfaces;
	}

	public String getDatas() {
		return datas;
	}


	private String datas;
	private int tryCount;



	public void setClientInterfaces(ClientInterface[] clientInterfaces) {
		this.clientInterfaces = clientInterfaces;
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
		String actionUrl = BuildTool.buildActionUrl(elasticsearchOutputConfig.getClientOptions(), BulkConfig.ERROR_FILTER_PATH);
		if(elasticsearchOutputConfig.isDebugResponse()) {

			for (ClientInterface clientInterface : clientInterfaces) {
				data = clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST);
			}
			finishTask();
			if(logger.isInfoEnabled())
				logger.info(data);

		}
		else{
			if(elasticsearchOutputConfig.isDiscardBulkResponse() && importContext.getExportResultHandler() == null) {
				for (ClientInterface clientInterface : clientInterfaces) {
					ESVoidResponseHandler esVoidResponseHandler = new ESVoidResponseHandler();
					clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST, esVoidResponseHandler);

					if (esVoidResponseHandler.getElasticSearchException() != null)
						throw new DataImportException(esVoidResponseHandler.getElasticSearchException());
				}
				finishTask();
				return null;
			}
			else{
				for (ClientInterface clientInterface : clientInterfaces) {
					data = clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST);
				}
				finishTask();
			}
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
