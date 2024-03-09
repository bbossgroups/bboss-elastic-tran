package org.frameworkset.tran.plugin;
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

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.schedule.TaskContext;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BasePlugin {
	protected DataTranPlugin dataTranPlugin;
	protected ImportContext importContext;
	/**
	 * 通知输入插件停止采集数据
	 */
	private volatile boolean stopCollectData;
	public BasePlugin(ImportContext importContext){
		this.importContext = importContext;
//		init(importContext,targetImportContext);
//		importContext.setDataTranPlugin(this);
//		targetImportContext.setDataTranPlugin(this);
	}
	public abstract void afterInit();
	public abstract void beforeInit();
	public abstract void init();
	public boolean isMultiTran(){
		return false;
	}
	/**
	 * 通知输入插件停止采集数据
	 */
	public void stopCollectData(){
		stopCollectData = true;
	}
	public boolean isStopCollectData(){
		return stopCollectData;
	}
	public ImportContext getImportContext() {
		return importContext;
	}

	public Object formatLastDateValue(Date date){
		return date;
	}
    public Object formatLastLocalDateTimeValue(LocalDateTime localDateTime){
        return localDateTime;
    }

	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;
	}

	public boolean isEnablePluginTaskIntercept() {
		return true;
	}

	public String getLastValueVarName() {
		return importContext.getLastValueColumn();
	}
	public boolean isEnableAutoPauseScheduled(){
		return true;
	}
	public Context buildContext(TaskContext taskContext,  Record record, BatchContext batchContext){
		return new ContextImpl(  taskContext,importContext,    record,batchContext);
	}
	public Long getTimeRangeLastValue(){
		return null;
	}
	public JobTaskMetrics createJobTaskMetrics(){
//		return getOutputPlugin().createJobTaskMetrics();
		return new JobTaskMetrics();
	}
}
