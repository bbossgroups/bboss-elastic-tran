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
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/4 16:50
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseTaskCommand<RESULT> implements TaskCommand<RESULT> {
	private static Logger logger = LoggerFactory.getLogger(BaseTaskCommand.class);
	protected ImportCount importCount;
	protected ImportContext importContext;
	protected TaskMetrics taskMetrics;
	protected TaskContext taskContext;
	protected LastValueWrapper lastValue;
	protected long totalSize;
	protected Status currentStatus;
    protected List<CommonRecord> records;
    protected TaskCommandContext taskCommandContext;
    protected boolean multiOutputTran;
    protected OutputConfig outputConfig;

    protected OutputPlugin outputPlugin;
    public boolean isMultiOutputTran(){
        return multiOutputTran;
    }
    public void setMultiOutputTran(boolean multiOutputTran){
        this.multiOutputTran = multiOutputTran;
    }

    public OutputPlugin getOutputPlugin() {
        return outputPlugin;
    }
 

    public void setRecords(List<CommonRecord> records) {
        this.records = records;
    }
    protected void logNodatas(Logger logger){
        ImportExceptionUtil.loginfo(logger,importContext,"All output data is ignored and do nothing.",outputPlugin);
       
    }
    public List<CommonRecord> getRecords() {
        return records;
    }

    public Object getDatas(){
        return records;
    }

    public void init(){
		TaskMetrics taskMetrics = getTaskMetrics();		
//		taskMetrics.setTaskStartTime(new Date());
		if(taskContext != null){
			taskContext.beginTask(taskMetrics);
		}

	}
	public JobContext getJobContext(){
		if(importContext != null) {
			return importContext.getJobContext();
		}
		else{
			return null;
		}
	}
	@Override
	public LastValueWrapper getLastValue() {
		return lastValue;
	}

	public long getDataSize(){
		return taskCommandContext.getDataSize();
	}
	public TaskMetrics getTaskMetrics(){
		return taskMetrics;
	}
	public int getTaskNo(){
		return taskMetrics.getTaskNo();
	}
	public String getJobNo(){
		return taskMetrics.getJobNo();
	}
	@Override
	public ImportContext getImportContext() {
		return importContext;
	}
	public void finishTask(){
        //多输出插件创建的子输出任务无需更新增量采集状态
        if(!this.isMultiOutputTran()){
            importContext.flushLastValue(lastValue, currentStatus);
        }
	}

	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	public long getElapsed(){
		if (taskMetrics != null )
			return taskMetrics.getElapsed();
		return -1;
	}

	public long getTotalSize() {
		return totalSize;
	}


	public BaseTaskCommand(OutputConfig outputConfig, TaskCommandContext taskCommandContext){
        this.outputConfig = outputConfig;
        this.outputPlugin = outputConfig.getOutputPlugin();
        this.taskCommandContext = taskCommandContext;
		this.importCount = taskCommandContext.getTotalCount();
		this.importContext =  taskCommandContext.getImportContext();
		totalSize = importCount.getTotalCount();
        taskMetrics = taskCommandContext.getTaskMetrics();
		this.lastValue = taskCommandContext.getLastValue();
		this.currentStatus = taskCommandContext.getCurrentStatus();
		this.taskContext = taskCommandContext.getTaskContext();
	}
	public ImportCount getImportCount(){
		return this.importCount;
	}

    @Override
    public TaskCommandContext getTaskCommandContext() {
        return taskCommandContext;
    }

    @Override
	public TaskContext getTaskContext() {
		return taskContext;
	}
	@Override
	public void finished(){
		try {
			if (taskContext != null && taskMetrics != null) {
				taskContext.finishTaskMetrics(taskMetrics);
			}
		}
		catch (Exception e){
			logger.error("Task finished failed:",e);
		}
	}
}
