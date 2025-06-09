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
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.CommonAsynRecordTranJob;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TranJob;

import java.time.LocalDateTime;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/18
 * @author biaoping.yin
 * @version 1.0
 */
public interface InputPlugin {
	public ImportContext getImportContext();
	public Context buildContext(TaskContext taskContext,  Record record, BatchContext batchContext);
	public void initStatusTableId();
	public String getLastValueVarName();
	public Long getTimeRangeLastValue();
    public Object formatLastDateValue(Date date);
    public Object formatLastLocalDateTimeValue(LocalDateTime localDateTime);
	public void doImportData( TaskContext taskContext)  throws DataImportException;
	public void afterInit();
	public void beforeInit();
	public void init();

    /**
     * 标记作业是否是多子任务采集作业：比如多文件采集
     * @return
     */
	public boolean isMultiTran();
    default public boolean isEventMsgTypePlugin(){
        return false;
    }
	/**
	 * 销毁插件
	 */
	public void destroy(boolean waitTranStop);

	/**
	 * 停止采集数据
	 */
	public void stopCollectData();
	public boolean isStopCollectData();
	public boolean isEnablePluginTaskIntercept() ;
	public boolean isEnableAutoPauseScheduled();
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin);

	String getJobType();

    default TranJob getTranJob(){
        return new CommonAsynRecordTranJob();
    }
}
