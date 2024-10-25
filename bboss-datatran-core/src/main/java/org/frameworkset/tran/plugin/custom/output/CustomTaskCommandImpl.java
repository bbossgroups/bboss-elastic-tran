package org.frameworkset.tran.plugin.custom.output;
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
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
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
public class CustomTaskCommandImpl extends BaseTaskCommand< String> {
	private Logger logger = LoggerFactory.getLogger(CustomTaskCommandImpl.class);
	
	private TaskContext taskContext;
	private CustomOutputConfig customOutputConfig;
	public CustomTaskCommandImpl(TaskCommandContext taskCommandContext) {
		super(  taskCommandContext);
		customOutputConfig = (CustomOutputConfig) importContext.getOutputConfig();
		if(this.taskContext == null) {
            this.taskContext = new TaskContext(importContext);
            taskCommandContext.setTaskContext(taskContext);
        }
	}

	public String execute(){
        if(records != null && records.size() > 0) {
            CustomOutPutContext customOutPutContext = new CustomOutPutContext();
            customOutPutContext.setDatas(records);
            customOutPutContext.setTaskMetrics(taskMetrics);
            customOutPutContext.setTaskContext(taskContext);
            
            customOutputConfig.getCustomOutPutV1().handleData(customOutPutContext);
        }
        else{
            
            logNodatas( logger);
        }
		finishTask();
		return null;
	}



}
