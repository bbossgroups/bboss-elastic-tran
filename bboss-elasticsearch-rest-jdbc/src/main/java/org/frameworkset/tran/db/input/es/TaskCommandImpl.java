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

import org.frameworkset.elasticsearch.client.*;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.task.TaskFailedException;
import org.frameworkset.elasticsearch.handler.ESVoidResponseHandler;
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
	private String refreshOption;

	public TaskCommandImpl(ImportCount importCount, ImportContext importContext, long dataSize, int taskNo, String jobNo) {
		super(importCount,importContext,  dataSize,  taskNo,  jobNo);
	}




	private ClientInterface clientInterface;

	public String getRefreshOption() {
		return refreshOption;
	}

	public ClientInterface getClientInterface() {
		return clientInterface;
	}

	public String getDatas() {
		return datas;
	}


	private String datas;
	private int tryCount;

	public void setRefreshOption(String refreshOption) {
		this.refreshOption = refreshOption;
	}

	public void setClientInterface(ClientInterface clientInterface) {
		this.clientInterface = clientInterface;
	}

	public void setDatas(String datas) {
		this.datas = datas;
	}





	private static Logger logger = LoggerFactory.getLogger(TaskCommand.class);
	private String buildActionUrl(){
		StringBuilder url = new StringBuilder();
		url.append("_bulk");
		if(refreshOption != null)
			url.append("?").append(refreshOption);
		else{
			ClientOptions clientOptions = importContext.getClientOptions();
			String refresh  = clientOptions.getRefresh();
			boolean p = false;
			if(refresh != null) {
				url.append("?refresh=").append(refresh);
				p = true;
			}
			/**
			Long if_seq_no = clientOptions.getIfSeqNo();
			if(if_seq_no != null){
				if(p){
					url.append("&if_seq_no=").append(if_seq_no);
				}
				else{
					url.append("?if_seq_no=").append(if_seq_no);
					p = true;
				}
			}
			Long if_primary_term = clientOptions.getIfPrimaryTerm();
			if(if_primary_term != null){
				if(p){
					url.append("&if_primary_term=").append(if_primary_term);
				}
				else{
					url.append("?if_primary_term=").append(if_primary_term);
					p = true;
				}
			}*/

			/**
			Object retry_on_conflict = clientOptions.getEsRetryOnConflict();
			if(retry_on_conflict != null){
				if(p){
					url.append("&retry_on_conflict=").append(retry_on_conflict);
				}
				else{
					url.append("?retry_on_conflict=").append(retry_on_conflict);
					p = true;
				}
			}*/
			Object routing = clientOptions.getRouting();
			if(routing != null){
				if(p){
					url.append("&routing=").append(routing);
				}
				else{
					url.append("?routing=").append(routing);
					p = true;
				}
			}
			String timeout = clientOptions.getTimeout();
			if(timeout != null){
				if(p){
					url.append("&timeout=").append(timeout);
				}
				else{
					url.append("?timeout=").append(timeout);
					p = true;
				}
			}
			/**
			String master_timeout = clientOptions.getMasterTimeout();
			if(master_timeout != null){
				if(p){
					url.append("&master_timeout=").append(master_timeout);
				}
				else{
					url.append("?master_timeout=").append(master_timeout);
					p = true;
				}
			}*/
			Integer wait_for_active_shards = clientOptions.getWaitForActiveShards();
			if(wait_for_active_shards != null){
				if(p){
					url.append("&wait_for_active_shards=").append(wait_for_active_shards);
				}
				else{
					url.append("?wait_for_active_shards=").append(wait_for_active_shards);
					p = true;
				}
			}
			/**
			String op_type = clientOptions.getOpType();
			if(op_type != null){
				if(p){
					url.append("&op_type=").append(op_type);
				}
				else{
					url.append("?op_type=").append(op_type);
					p = true;
				}
			}*/
			String pipeline = clientOptions.getPipeline();
			if(pipeline != null){
				if(p){
					url.append("&pipeline=").append(pipeline);
				}
				else{
					url.append("?pipeline=").append(pipeline);
					p = true;
				}
			}


		}
		return url.toString();
	}
	public String execute(){
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;
		if(importContext.isDebugResponse()) {

			data = clientInterface.executeHttp(buildActionUrl(), datas, ClientUtil.HTTP_POST);
			if(logger.isInfoEnabled())
				logger.info(data);

		}
		else{
			if(importContext.isDiscardBulkResponse() && importContext.getExportResultHandler() == null) {
				ESVoidResponseHandler esVoidResponseHandler = new ESVoidResponseHandler();
				clientInterface.executeHttp(buildActionUrl(), datas, ClientUtil.HTTP_POST,esVoidResponseHandler);
				if(esVoidResponseHandler.getElasticSearchException() != null)
					throw esVoidResponseHandler.getElasticSearchException();
				return null;
			}
			else{
				data = clientInterface.executeHttp(buildActionUrl(), datas, ClientUtil.HTTP_POST);
			}
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
