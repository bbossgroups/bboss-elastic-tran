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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
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
public class FileFtpTaskCommandImpl extends BaseTaskCommand<String> {
	private FileTransfer fileTransfer;
    protected FileOutputConfig fileOutputConfig;
	public FileFtpTaskCommandImpl(TaskCommandContext taskCommandContext, FileTransfer fileTransfer, OutputConfig outputConfig) {
		super(outputConfig,taskCommandContext);
		this.fileTransfer = fileTransfer;
        fileOutputConfig = (FileOutputConfig) outputConfig;
	}


     

//	private String datas;
	private int tryCount;





    public Object getDatas(){
        return records;
    }
    private String buildDatas() throws Exception {
        StringBuilder builder = new StringBuilder();
        BBossStringWriter writer = new BBossStringWriter(builder);
        CommonRecord record = null;
        for(int i = 0; i < records.size(); i ++){
            record = records.get(i);

            fileOutputConfig.generateReocord(taskContext,   taskMetrics,record, writer);
            writer.write(fileOutputConfig.getLineSeparator());
        }
        return writer.toString();
    }

	private static Logger logger = LoggerFactory.getLogger(FileFtpTaskCommandImpl.class);

	public String execute(){
        if(records != null && records.size() > 0) {
            if (this.importContext.getMaxRetry() > 0) {
                if (this.tryCount >= this.importContext.getMaxRetry())
                    throw new TaskFailedException("task execute failed:reached max retry times " + this.importContext.getMaxRetry());
            }
            this.tryCount++;

            try {
//                if (datas == null) {
//                    datas = buildDatas();
//                }
                fileTransfer.writeData(this, records,  taskContext,taskMetrics);
                
            } catch (Exception e) {
                throw ImportExceptionUtil.buildDataImportException(outputPlugin,importContext, "发送数据失败", e);
            }
        }
        else{
            logNodatas( logger);
        }
        finishTask();
        return null;
	}

	public int getTryCount() {
		return tryCount;
	}


}
