package org.frameworkset.tran.plugin.dummy.output;
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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.util.TranUtil;
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
public class DummyTaskCommandImpl extends BaseTaskCommand<List<CommonRecord>,String> {
	private Logger logger = LoggerFactory.getLogger(DummyTaskCommandImpl.class);
	private DummyOutputConfig dummyOutputConfig ;
	public DummyTaskCommandImpl(ImportCount importCount, ImportContext importContext,
                                long dataSize, int taskNo, String jobNo,
                                LastValueWrapper lastValue, Status currentStatus, TaskContext taskContext) {
		super(importCount,importContext,   dataSize,  taskNo,  jobNo,lastValue,  currentStatus,   taskContext);
//		if(targetImportContext instanceof DummyOupputContext)
//			dummyOupputContext = (DummyOupputContext)targetImportContext;
		dummyOutputConfig = (DummyOutputConfig) importContext.getOutputConfig();
	}




	public List<CommonRecord> getDatas() {
		return records;
	}


    private List<CommonRecord> records;


	public void setDatas(List<CommonRecord> records) {
		this.records = records;
	}



    private String buildDatas() throws Exception {
        StringBuilder builder = new StringBuilder();
        BBossStringWriter writer = new BBossStringWriter(builder);
        for(int i = 0; i < records.size(); i ++){
            dummyOutputConfig.generateReocord(taskContext,records.get(i), writer);
            writer.write(TranUtil.lineSeparator);
        }
        return writer.toString();
    }


	public String execute() throws Exception {

		if(dummyOutputConfig.isPrintRecord()) {
           
            logger.info(buildDatas());
        }
		finishTask();
		return null;
	}

	public int getTryCount() {
		return -1;
	}


}
