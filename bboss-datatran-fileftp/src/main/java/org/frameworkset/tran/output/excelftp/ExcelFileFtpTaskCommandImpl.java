package org.frameworkset.tran.output.excelftp;
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

import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
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
public class ExcelFileFtpTaskCommandImpl extends BaseTaskCommand< String> {
//	private String refreshOption;
	private ExcelFileTransfer fileTransfer;

	public ExcelFileFtpTaskCommandImpl(TaskCommandContext taskCommandContext, ExcelFileTransfer fileTransfer) {
		super(  taskCommandContext);
		this.fileTransfer = fileTransfer;
	}




 
	private int tryCount;


 




	private static Logger logger = LoggerFactory.getLogger(TaskCommand.class);

	public String execute(){
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;
		try {
			fileTransfer.writeData(this,records);
			finishTask();
		} catch (IOException e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,"writeData failed:",e);
		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
