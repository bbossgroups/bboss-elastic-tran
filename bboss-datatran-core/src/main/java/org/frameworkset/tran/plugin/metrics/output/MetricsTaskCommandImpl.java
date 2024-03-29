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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.BaseTranJob;
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
public class MetricsTaskCommandImpl extends BaseTaskCommand<List<CommonRecord>, String> {
	private Logger logger = LoggerFactory.getLogger(MetricsTaskCommandImpl.class);
	private List<CommonRecord> datas;
	private TaskContext taskContext;
	private MetricsOutputConfig metricsOutputConfig;
	public MetricsTaskCommandImpl(ImportCount importCount, ImportContext importContext, List<CommonRecord> datas,
                                  int taskNo, String jobNo,
                                  LastValueWrapper lastValue, Status currentStatus, TaskContext taskContext) {
		super(importCount,importContext,   datas.size(),  taskNo,  jobNo,lastValue,  currentStatus, taskContext);
		metricsOutputConfig = (MetricsOutputConfig) importContext.getOutputConfig();
		this.datas = datas;
		if(this.taskContext == null)
			this.taskContext = new TaskContext(importContext);
	}




	public List<CommonRecord> getDatas() {
		return datas;
	}

	public void setDatas(List<CommonRecord> datas) {
		this.datas = datas;
	}

	public String execute(){
		BuildMapDataContext buildMapDataContext = new BuildMapDataContext();
		String dataTimeField = metricsOutputConfig.getDataTimeField();
		buildMapDataContext.setDataTimeField(dataTimeField);
        if(metricsOutputConfig.getTimeWindowType() != null)
            buildMapDataContext.setTimeWindowType(metricsOutputConfig.getTimeWindowType());
		for(CommonRecord commonRecord :datas) {
			BaseTranJob.map(  commonRecord,   buildMapDataContext,   metricsOutputConfig.getMetrics(),  metricsOutputConfig.isUseDefaultMapData());
		}
		finishTask();
		return null;
	}

}
