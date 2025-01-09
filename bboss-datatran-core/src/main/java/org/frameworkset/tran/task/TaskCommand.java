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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public interface TaskCommand<RESULT> {
	 JobContext getJobContext();
	 LastValueWrapper getLastValue();
    TaskCommandContext getTaskCommandContext();
     void setRecords(List<CommonRecord> records);
     List<CommonRecord> getRecords();
     boolean isMultiOutputTran();
	 void init();
	 TaskContext getTaskContext();
	 TaskMetrics getTaskMetrics();
	 void finishTask();
     RESULT execute() throws Exception;
    
     Object getDatas();

	default  int getTryCount() {
		return -1;
	}
	 ImportContext getImportContext();

    OutputConfig getOutputConfig();

    ImportCount getImportCount();
	 long getDataSize();
    
	 int getTaskNo();
	 String getJobNo();
	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	 long getElapsed();
	 void finished();

    public OutputPlugin getOutputPlugin() ;

}
