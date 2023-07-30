package org.frameworkset.tran.metrics;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.TaskCommand;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/12/20
 * @author biaoping.yin
 * @version 1.0
 */
public class MetricsTaskcommand implements TaskCommand {
	protected TaskMetrics taskMetrics;
	protected JobContext jobContext;
	protected ImportContext importContext;
	protected TaskContext taskContext;
    protected boolean forceFlush;

	@Override
	public JobContext getJobContext() {
		return jobContext;
	}

	@Override
	public LastValueWrapper getLastValue() {
		return taskMetrics.getLastValue();
	}

	@Override
	public Object getDatas() {
		return null;
	}

	@Override
	public void init() {

	}

	@Override
	public TaskContext getTaskContext() {
		return taskContext;
	}

	@Override
	public TaskMetrics getTaskMetrics() {
		return taskMetrics;
	}

	@Override
	public void setDatas(Object datas) {

	}

	@Override
	public void finishTask() {

	}

	@Override
	public Object execute() {
		return null;
	}

	@Override
	public int getTryCount() {
		return 0;
	}

	@Override
	public ImportContext getImportContext() {
		return importContext;
	}

	@Override
	public ImportCount getImportCount() {
		return null;
	}

	@Override
	public long getDataSize() {
		return taskMetrics.getRecords();
	}

	@Override
	public int getTaskNo() {
		return taskMetrics.getTaskNo();
	}

	@Override
	public String getJobNo() {
		return taskMetrics.getJobNo();
	}

	@Override
	public long getElapsed() {
		return taskMetrics.getElapsed();
	}

	@Override
	public void finished() {

	}

	public void setTaskMetrics(TaskMetrics taskMetrics) {
		this.taskMetrics = taskMetrics;
	}

	public void setJobContext(JobContext jobContext) {
		this.jobContext = jobContext;
	}

	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

	public void setTaskContext(TaskContext taskContext) {
		this.taskContext = taskContext;
	}

    @Override
    public void setForceFlush(boolean forceFlush) {
        this.forceFlush = forceFlush;
    }

    @Override
    public boolean isForceFlush() {
        return forceFlush;
    }
}
