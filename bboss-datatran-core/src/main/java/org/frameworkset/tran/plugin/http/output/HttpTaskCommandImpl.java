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
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
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
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;

		try {
			if(httpOutputConfig.getHttpMethod().equals("post"))
				data = HttpRequestProxy.sendJsonBody(httpOutputConfig.getTargetHttpPool(),datas,httpOutputConfig.getServiceUrl());
			else
				data = HttpRequestProxy.putJson(httpOutputConfig.getTargetHttpPool(),datas,httpOutputConfig.getServiceUrl());
			finishTask();
		} catch (Exception e) {
			throw new DataImportException(datas,e);
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
