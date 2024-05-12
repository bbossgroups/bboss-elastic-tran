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

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class FileFtpTaskCommandImpl extends BaseTaskCommand<String> {
	private FileTransfer fileTransfer;
    protected FileOutputConfig fileOutputConfig;
	public FileFtpTaskCommandImpl(ImportCount importCount, ImportContext importContext,
                                  long dataSize, int taskNo, String jobNo, FileTransfer fileTransfer,
                                  LastValueWrapper lastValue, Status currentStatus,  TaskContext taskContext) {
		super(importCount,importContext,   dataSize,  taskNo,  jobNo,lastValue,  currentStatus,   taskContext);
		this.fileTransfer = fileTransfer;
        fileOutputConfig = (FileOutputConfig) importContext.getOutputConfig();
	}


     

	private String datas;
	private int tryCount;


 



    private String buildDatas() throws Exception {
        StringBuilder builder = new StringBuilder();
        BBossStringWriter writer = new BBossStringWriter(builder);
        CommonRecord record = null;
        for(int i = 0; i < records.size(); i ++){
            record = records.get(i);

            fileOutputConfig.generateReocord(taskContext,record, writer);
            writer.write(fileOutputConfig.getLineSeparator());
        }
        return writer.toString();
    }

	private static Logger logger = LoggerFactory.getLogger(FileFtpTaskCommandImpl.class);

	public String execute(){
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;

		try {
            if(datas == null){
                datas = buildDatas();
            }
			fileTransfer.writeData(this,datas,getTotalSize(),dataSize);
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
