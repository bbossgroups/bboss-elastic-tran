package org.frameworkset.tran;
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

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.tran.config.DynamicParamContext;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.metrics.DataTranPluginMetricsLogAPI;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.*;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:34
 * @author biaoping.yin
 * @version 1.0
 */
public interface DataTranPlugin extends DataTranPluginMetricsLogAPI {
    public StatusManager getStatusManager();
//    public boolean onlyUseBatchExecute();
    /**
     * 识别任务是否已经完成
     * @param status
     * @return
     */
    public boolean isComplete(Status status);
    public boolean isSingleLastValueType();
    public DestroyPolicy getDestroyPolicy();
    public boolean hasJobInputParamGroups();
    public void initLastValueStatus(Status currentStatus, BaseStatusManager baseStatusManager) throws Exception;

    /**
     * Number ts = (Number)lastValue.getLastValue();
     * 				Number nts = (Number)taskMetrics.getLastValue().getLastValue();
     * 				if(nts.longValue() > ts.longValue())
     * 					this.lastValue = taskMetrics.getLastValue();
     * @param oldValue
     * @param newValue
     * @return
     */
    public LastValueWrapper maxNumberLastValue(LastValueWrapper oldValue, LastValueWrapper newValue);
    public boolean needUpdateLastValueWrapper(Integer lastValueType, LastValueWrapper oldValue,LastValueWrapper newValue);
    public LastValueWrapper maxLastValue(LastValueWrapper oldValue, Record record);
    public List<Map> getJobInputParamGroups(TaskContext taskContext);
	public String getJobType();
	public LoadCurrentStatus getLoadCurrentStatus();
	public void initResources(ResourceStart resourceStart) ;
	public void destroyResources(ResourceEnd resourceEnd) ;
	public int getLastValueType();
	public void initStatusTableId();
	public void loadCurrentStatus(List<Status> statuses);
	public InputPlugin getInputPlugin() ;
	public boolean checkTranToStop();
	public OutputPlugin getOutputPlugin();
	public Object[] putLastParamValue(Map params);
	public boolean isIncreamentImport();
	public Map getJobInputParams(TaskContext taskContext);
	public Map getJobOutputParams(TaskContext taskContext);
	public Map getJobInputParams(DynamicParamContext dynamicParamContext);
	public Map getJobOutputParams(DynamicParamContext dynamicParamContext);
	public Map getParamValue(Map params);
//	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet);

	public boolean isEnableAutoPauseScheduled();

    /**
     * 如果作业处于暂停状态，回阻塞等待，直到consume使作业恢复执行，并返回true
     * @param autoPause
     * @return
     */
	public boolean isSchedulePaussed(boolean autoPause);
	public ScheduleAssert getScheduleAssert();
	public void setScheduleAssert(ScheduleAssert scheduleAssert);
	public void preCall(TaskContext taskContext);
	public void afterCall(TaskContext taskContext);

    public void throwException(TaskContext taskContext,Throwable e);
    
	public Context buildContext(TaskContext taskContext, Record record,BatchContext batchContext);
//	public void forceflushLastValue(Status currentStatus);
	public  void handleOldedTasks(List<Status> olded );
	public  void handleOldedTask(Status olded );
		boolean assertCondition();

	void setErrorWrapper(TranErrorWrapper tranErrorWrapper);

	void importData(ScheduleEndCall scheduleEndCall) throws DataImportException;
	public String getLastValueVarName();
	ScheduleService getScheduleService();
	ImportContext getImportContext();
	public void setImportContext(ImportContext importContext);

	public Long getTimeRangeLastValue();

//	void flushLastValue(LastValueWrapper lastValue,Status currentStatus,boolean reachEOFClosed);
    void flushLastValue(LastValueWrapper lastValue,Status currentStatus,boolean reachEOFClosed);
//	void flushLastValue(LastValueWrapper lastValue,Status currentStatus);




	void destroy(DestroyPolicy destroyPolicy);

	public void setHasTran();
	public void setNoTran();
	public boolean isPluginStopAppending();

	public boolean isStopCollectData();
	boolean isPluginStopREADY();
	void init(ImportContext importContext);

//	Object getValue(String columnName) throws ESDataImportException;
//
//	Object getDateTimeValue(String columnName) throws ESDataImportException;
	void setForceStop();
//	public Object getLastValue() throws ESDataImportException;


	String getLastValueClumnName();

	boolean isContinueOnError();

	Status getCurrentStatus();

	ExportCount getExportCount();

	boolean isMultiTran();

	BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus);

	void doImportData(TaskContext taskContext);
	public void addStatus(Status currentStatus) throws DataImportException;

	boolean useFilePointer();
	SetLastValueType getSetLastValueType();

    void finishAndWaitTran(Throwable throwable);

    void callTran(BaseDataTran baseDataTran);
}
