package org.frameworkset.tran.plugin.metrics.output;
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
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.BaseTranJob;
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
public class MetricsTaskCommandImpl extends BaseTaskCommand<  String> {
	private Logger logger = LoggerFactory.getLogger(MetricsTaskCommandImpl.class);
	private TaskContext taskContext;
	private MetricsOutputConfig metricsOutputConfig;
	public MetricsTaskCommandImpl(TaskCommandContext taskCommandContext) {
		super(  taskCommandContext);
		metricsOutputConfig = (MetricsOutputConfig) importContext.getOutputConfig();
		if(this.taskContext == null) {
            this.taskContext = new TaskContext(importContext);
            taskCommandContext.setTaskContext(taskContext);
            
        }
	}



 

	public String execute(){
        if(records.size() > 0) {
            BuildMapDataContext buildMapDataContext = new BuildMapDataContext();
            String dataTimeField = metricsOutputConfig.getDataTimeField();
            buildMapDataContext.setDataTimeField(dataTimeField);
            if (metricsOutputConfig.getTimeWindowType() != null)
                buildMapDataContext.setTimeWindowType(metricsOutputConfig.getTimeWindowType());
            for (CommonRecord commonRecord : records) {
                BaseTranJob.map(commonRecord, buildMapDataContext, metricsOutputConfig.getMetrics(), metricsOutputConfig.isUseDefaultMapData());
            }
        }
        else{
            if (logger.isInfoEnabled()){
                logger.info("All output data is ignored and do nothing.");
            }
        }
		finishTask();
		return null;
	}

}
