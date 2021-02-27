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

import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/4 16:50
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseTaskCommand<DATA,RESULT> implements TaskCommand<DATA,RESULT> {
	protected ImportCount importCount;
	protected ImportContext importContext;
	protected ImportContext targetImportContext;
	protected TaskMetrics taskMetrics;
	protected TaskContext taskContext;
	protected Object lastValue;
	protected long dataSize;
	public long getDataSize(){
		return dataSize;
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
		importContext.flushLastValue(lastValue);
	}

	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	public long getElapsed(){
		if (taskMetrics != null && taskMetrics.getTaskStartTime() != null && taskMetrics.getTaskEndTime() != null)
			return taskMetrics.getTaskEndTime().getTime() - taskMetrics.getTaskStartTime().getTime();
		return -1;
	}
	public BaseTaskCommand(ImportCount importCount, ImportContext importContext,ImportContext targetImportContext,long dataSize,int taskNo,String jobNo,Object lastValue){
		this.importCount = importCount;
		this.importContext =  importContext;
		this.targetImportContext = targetImportContext;
		this.dataSize = dataSize;
		this.taskMetrics = new TaskMetrics();
		taskMetrics.setTaskNo(taskNo);
		taskMetrics.setJobNo(jobNo);
		this.lastValue = lastValue;
	}
	public ImportCount getImportCount(){
		return this.importCount;
	}
}
