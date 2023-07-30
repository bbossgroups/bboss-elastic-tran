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

import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public interface TaskCommand<DATA,RESULT> {
	public JobContext getJobContext();
	public LastValueWrapper getLastValue();
	public DATA getDatas() ;
	public void init();
	public TaskContext getTaskContext();
	public TaskMetrics getTaskMetrics();
	public void setDatas(DATA datas) ;
	public void finishTask();
    public boolean isForceFlush();
    public void setForceFlush(boolean forceFlush);
    public RESULT execute();

	default public int getTryCount() {
		return -1;
	}
	public ImportContext getImportContext();
	public ImportCount getImportCount();
	public long getDataSize();
	public int getTaskNo();
	public String getJobNo();
	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	public long getElapsed();
	public void finished();


}
