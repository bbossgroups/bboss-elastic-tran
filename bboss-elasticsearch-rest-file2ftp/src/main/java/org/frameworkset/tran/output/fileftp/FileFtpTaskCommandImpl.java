package org.frameworkset.tran.output.fileftp;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class FileFtpTaskCommandImpl extends BaseTaskCommand<String,String> {
//	private String refreshOption;
	private FileTransfer fileTransfer;

	public FileFtpTaskCommandImpl(ImportCount importCount, ImportContext importContext, ImportContext targetImportContext,
								  long dataSize, int taskNo, String jobNo, FileTransfer fileTransfer,
								  Object lastValue, Status currentStatus,boolean reachEOFClosed) {
		super(importCount,importContext,  targetImportContext,  dataSize,  taskNo,  jobNo,lastValue,  currentStatus, reachEOFClosed);
		this.fileTransfer = fileTransfer;
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
		if(importContext.isDebugResponse()) {
			try {
				fileTransfer.writeData(datas);
				finishTask();
			} catch (IOException e) {
				throw new DataImportException(datas,e);
			}


		}
		else{
			try {
				fileTransfer.writeData(datas);
				finishTask();
			} catch (IOException e) {
				throw new DataImportException(datas,e);
			}
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
