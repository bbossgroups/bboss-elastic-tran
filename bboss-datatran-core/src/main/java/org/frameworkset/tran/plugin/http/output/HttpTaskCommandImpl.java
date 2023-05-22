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

import org.apache.http.entity.ContentType;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.DynamicParam;
import org.frameworkset.tran.config.DynamicParamContext;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.http.DynamicHeaderContext;
import org.frameworkset.tran.plugin.http.HttpConfigClientProxy;
import org.frameworkset.tran.plugin.http.HttpProxyHelper;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpTaskCommandImpl extends BaseTaskCommand<String,String> {
//	private String refreshOption;
	private HttpOutputConfig httpOutputConfig ;
	public HttpTaskCommandImpl(ImportCount importCount, ImportContext importContext,
							   long dataSize, int taskNo, String jobNo,
							   Object lastValue, Status currentStatus, boolean reachEOFClosed, TaskContext taskContext) {
		super(importCount,importContext,   dataSize,  taskNo,  jobNo,lastValue,  currentStatus, reachEOFClosed,  taskContext);
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();
	}




	public String getDatas() {
		return datas;
	}


	private String datas;
	private int tryCount;


	public void setDatas(String datas) {
		this.datas = datas;
	}





	private static Logger logger = LoggerFactory.getLogger(TaskCommand.class);

	public String execute(){

		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;

		try {
			String data = null;
			if(httpOutputConfig.isDirectSendData()) {
				data = directSendData();
			}
			else {
				data = unDirectSendData();
			}

			finishTask();
			return data;
		} catch (Exception e) {
			throw new DataImportException(datas,e);
		}

	}
	private String directSendData(){
		String data = null;
		HttpOutputDataTranPlugin httpOutputDataTranPlugin = (HttpOutputDataTranPlugin) importContext.getOutputPlugin();
		DynamicHeaderContext dynamicHeaderContext = null;
		if(httpOutputConfig.getDynamicHeaders() != null) {
			dynamicHeaderContext = new DynamicHeaderContext();
			dynamicHeaderContext.setDatas(datas);
			dynamicHeaderContext.setTaskContext(taskContext);
			dynamicHeaderContext.setImportContext(httpOutputDataTranPlugin.getImportContext());
		}
		if(httpOutputConfig.isPostMethod() )
			data = HttpRequestProxy.sendBody(httpOutputConfig.getTargetHttpPool(),datas,httpOutputConfig.getServiceUrl()
					, HttpProxyHelper.getHttpHeaders(httpOutputConfig,dynamicHeaderContext), ContentType.APPLICATION_JSON);
		else
			data = HttpRequestProxy.putJson(httpOutputConfig.getTargetHttpPool(),datas,httpOutputConfig.getServiceUrl(),
					HttpProxyHelper.getHttpHeaders(httpOutputConfig,dynamicHeaderContext));
		return data;
	}

	private String unDirectSendData(){
		String data = null;
		HttpOutputDataTranPlugin httpOutputDataTranPlugin = (HttpOutputDataTranPlugin) importContext.getOutputPlugin();
		HttpConfigClientProxy httpConfigClientProxy = httpOutputDataTranPlugin.getHttpConfigClientProxy();

		DynamicHeaderContext dynamicHeaderContext = null;
		if(httpOutputConfig.getDynamicHeaders() != null) {
			dynamicHeaderContext = new DynamicHeaderContext();
			dynamicHeaderContext.setDatas(datas);
			dynamicHeaderContext.setTaskContext(taskContext);
			dynamicHeaderContext.setImportContext(httpOutputDataTranPlugin.getImportContext());
		}
		Map<String, DynamicParam> dynamicParams = importContext.getJobDynamicOutputParams();
		DynamicParamContext dynamicParamContext = null;
		if(dynamicParams != null && dynamicParams.size() > 0){
			dynamicParamContext = new DynamicParamContext();
			dynamicParamContext.setImportContext(importContext);
			dynamicParamContext.setTaskContext(taskContext);
			dynamicParamContext.setDatas(datas);
		}
		Map params = importContext.getDataTranPlugin().getJobOutputParams(dynamicParamContext);
		params.put(httpOutputConfig.getDataKey(),datas);
		if(httpOutputConfig.isPostMethod())
			data = httpConfigClientProxy.sendBody(httpOutputDataTranPlugin,dynamicHeaderContext,params);
		else
			data = httpConfigClientProxy.putJson(httpOutputDataTranPlugin,dynamicHeaderContext,params);
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
