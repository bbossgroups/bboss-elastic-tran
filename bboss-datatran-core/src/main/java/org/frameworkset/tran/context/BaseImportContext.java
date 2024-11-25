package org.frameworkset.tran.context;
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
import org.frameworkset.tran.Record;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.*;
import org.frameworkset.tran.listener.JobClosedListener;
import org.frameworkset.tran.metrics.*;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;
import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public  class BaseImportContext extends BaseMetricsLogReport implements ImportContext {
	protected BaseImportConfig baseImportConfig;
	protected InputConfig inputConfig;
	protected OutputConfig outputConfig;
	protected JobContext jobContext;
    protected EndAction endAction;
    public int getMetricsLogLevel() {
        return baseImportConfig.getMetricsLogLevel();
    }

    public JobContext getJobContext() {
		return jobContext;
	}
 
    public boolean isSerial(){
        return getStoreBatchSize() <= 0;
    }

    public boolean isParallelBatch(){
        return getStoreBatchSize() > 0 && getThreadCount() > 0 && isParallel();
    }

    public boolean isBatch(){
        return getStoreBatchSize() > 0;
    }

	public void setJobContext(JobContext jobContext) {
		this.jobContext = jobContext;
	}

	public JobTaskMetrics createJobTaskMetrics(){
		return getOutputPlugin().createJobTaskMetrics();
//		return new JobTaskMetrics();
	}



	public ImportStartAction getImportStartAction(){
		return baseImportConfig.getImportStartAction();
	}

	public DBConfig getDefaultDBConfig(){
		return baseImportConfig.getDefaultDBConfig();
	}
	public List<DBConfig> getOhterDBConfigs(){
		return baseImportConfig.getConfigs();
	}
	public DataTranPlugin buildDataTranPlugin(){
		return inputConfig.buildDataTranPlugin(this);
	}
	@Override
	public boolean isIncreamentImport(){
		return dataTranPlugin.isIncreamentImport();
	}
	public void setBaseImportConfig(BaseImportConfig baseImportConfig) {
		this.baseImportConfig = baseImportConfig;
	}

	public void setInputConfig(InputConfig inputConfig) {
		this.inputConfig = inputConfig;
	}

	public void setOutputConfig(OutputConfig outputConfig) {
		this.outputConfig = outputConfig;
	}
	public void afterBuild(ImportBuilder importBuilder){
		inputConfig.afterBuild(importBuilder,this);
		outputConfig.afterBuild(importBuilder,this);
        if(baseImportConfig.isFlushMetricsOnScheduleTaskCompleted()) {
            if (this.getMetrics() != null && this.getMetrics().size() > 0) {
                importBuilder.addCallInterceptor(new ETLMetricsCallInterceptor(this.getMetrics(), this),true);
            } else if(outputConfig instanceof MetricsOutputConfig){
                importBuilder.addCallInterceptor(new ETLMetricsCallInterceptor(((MetricsOutputConfig)outputConfig).getMetrics(), this),true);
            }
            baseImportConfig.setCallInterceptors(importBuilder.taskCallInterceptors());
        }
	}

	@Override
	public ImportContext addResourceStart(ResourceStart resourceStart) {
		if(resourceStart != null)
			dataTranPlugin.initResources(resourceStart);
		return this;
	}

	@Override
	public void destroyResources(ResourceEnd resourceEnd) {
		if(resourceEnd != null)
			dataTranPlugin.destroyResources(resourceEnd);
	}

	@Override
	public InputConfig getInputConfig() {
		return inputConfig;
	}

	@Override
	public OutputConfig getOutputConfig() {
		return outputConfig;
	}

	@Override
	public InputPlugin getInputPlugin() {
		if(dataTranPlugin != null && dataTranPlugin.getInputPlugin() != null){
			return dataTranPlugin.getInputPlugin();
		}
		else {
			return inputConfig.getInputPlugin(this);
		}
	}

	@Override
	public OutputPlugin getOutputPlugin() {
		if(dataTranPlugin != null && dataTranPlugin.getOutputPlugin() != null){
			return dataTranPlugin.getOutputPlugin();
		}
		else {
			return outputConfig.getOutputPlugin(this);
		}

	}

	public boolean isLastValueColumnSetted() {
		return baseImportConfig.isLastValueColumnSetted();
	}
	public Map getJobOutputParams(){
		return baseImportConfig.getJobOutputParams();
	}

    public JobClosedListener getJobClosedListener(){
        return baseImportConfig.getJobClosedListener();
    }
	public Map getJobInputParams(){
		return baseImportConfig.getJobInputParams();
	}
    /**
     * 输入参数组，将输入参数和输入动态参数组装为一个参数组添加到参数组集合中，添加完成后重置输入参数和输入动态参数，为增加新的参数组做准备
     * 通过添加多个参数组，作业调度时，特定的输入插件可以利用参数组中的每组参数发起并发数据请求，比如httpinput插件
     */
    public List<JobInputParamGroup> getJobInputParamGroups() {
        return baseImportConfig.getJobInputParamGroups();
    }
	public Map<String, DynamicParam> getJobDynamicInputParams() {
		return baseImportConfig.getJobDynamicInputParams();
	}
	public Map<String, DynamicParam> getJobDynamicOutputParams() {
		return baseImportConfig.getJobDynamicOutputParams();
	}
 

	public String[] getExportColumns(){
		return  baseImportConfig.getExportColumns();
	}
	public boolean useFilePointer(){
		return dataTranPlugin.useFilePointer();
	}
	//	private JDBCResultSet jdbcResultSet;
	private DataStream dataStream;
	private boolean currentStoped = false;

	public BaseImportContext(){

	}
	public long getLogsendTaskMetric() {
		return baseImportConfig.getLogsendTaskMetric();
	}

	public boolean serialAllData(){
		return baseImportConfig.isSerialAllData();
	}

	/**
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的偏移量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  单位：秒
	 * @return
	 */
	public Integer increamentEndOffset(){
		return baseImportConfig.getIncreamentEndOffset();
	}
    /**
     * 标识数字类型增量字段是否是时间戳，如果是时间戳，那么increamentEndOffset配置将起作用
     * @return
     */
    public boolean isNumberTypeTimestamp(){
        return baseImportConfig.isNumberTypeTimestamp();
    }
	public boolean isAsynFlushStatus(){
		return baseImportConfig.isAsynFlushStatus();
	}
	public long getAsynFlushStatusInterval(){
		return baseImportConfig.getAsynFlushStatusInterval();
	}
	public SplitHandler getSplitHandler() {
		return baseImportConfig.getSplitHandler();
	}
	public String getSplitFieldName() {
		return baseImportConfig.getSplitFieldName();
	}
	public Context buildContext(TaskContext taskContext,  Record record,BatchContext batchContext){
		return dataTranPlugin.buildContext( taskContext, record,batchContext);
	}
	public Long getTimeRangeLastValue(){
		return dataTranPlugin.getTimeRangeLastValue();
	}

	/**
	 * 异步消费数据时，强制刷新检测空闲时间间隔，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作
	 * @return
	 */
	public long getFlushInterval(){
		return baseImportConfig.getFlushInterval();
	}
	/**
	 * 忽略空值字段，elasticsearch数据导入的时候可以统一开启开关
	 * @return
	 */
	public boolean isIgnoreNullValueField(){
		return baseImportConfig.isIgnoreNullValueField();
	}

	public boolean isSortLastValue() {
		return baseImportConfig.isSortLastValue();
	}
	public long getAsynResultPollTimeOut() {
		return baseImportConfig.getAsynResultPollTimeOut();
	}
	@Override
	public Integer getFetchSize() {
		return baseImportConfig.getFetchSize();
	}
	public void init(){

	}
	public int getTranDataBufferQueue(){
		return baseImportConfig.getTranDataBufferQueue();
	}
	public BaseImportContext(BaseImportConfig baseImportConfig){
		this.baseImportConfig = baseImportConfig;
//		init(baseImportConfig);

	}
	public ExportCount getExportCount(){
		return dataTranPlugin.getExportCount();
	}
	@Override
	public void setStatusTableId(int hashCode) {

		baseImportConfig.setStatusTableId(buildStatusId(hashCode));

	}
	@Override
	public String buildStatusId( int hashCode)
	{
		String jobId = this.getJobId();
		String jobType = this.getJobType();
		if(jobId != null) {
            Integer statusIdPolicy = this.getStatusIdPolicy();
            if(statusIdPolicy != null && statusIdPolicy == ImportIncreamentConfig.STATUSID_POLICY_JOBID) {
                return jobType + "-" + jobId;
            }
            else{
                return jobType + "-" + jobId + "-" + hashCode;
            }
		}
		else{
			return jobType+"-"+hashCode;
		}

	}
    @Override
    public Integer getStatusIdPolicy() {
        return getInputConfig().getStatusIdPolicy(this);        
    }

	public DataRefactor getDataRefactor(){
		return baseImportConfig.getDataRefactor();
	}

	public String getApplicationPropertiesFile(){
		return baseImportConfig.getApplicationPropertiesFile();
	}

	public List<DBConfig> getConfigs(){
		return baseImportConfig.getConfigs();
	}



	public boolean isPrintTaskLog(){
		return baseImportConfig.isPrintTaskLog();
	}
	public void setDataRefactor( DataRefactor dataRefactor){
		this.baseImportConfig.setDataRefactor(dataRefactor);
	}

	@Override
	public void destroy(DestroyPolicy destroyPolicy) {
        if(dataTranPlugin != null) {
            this.dataTranPlugin.destroy(  destroyPolicy);
        }

	}

    /**
     * 等待转换结束，后再结束作业
     */
    @Override
    public void finishAndWaitTran(Throwable throwable){
        if(dataTranPlugin != null) {
            this.dataTranPlugin.finishAndWaitTran(  throwable);
        }
    }
    public void registEndAction(EndAction endAction){
        this.endAction = endAction;
    }

    public EndAction getEndAction() {
        return endAction;
    }

    public void cleanResource(){
		try {
			if (blockedExecutor != null) {
				ThreadPoolFactory.shutdownExecutor(blockedExecutor);
			}
		}
		catch(Exception e){

		}
		currentStoped = true;
	}



	public boolean isContinueOnError(){
		return baseImportConfig.isContinueOnError();
	}

	@Override
	public boolean assertCondition() {
		return dataTranPlugin.assertCondition();
	}
	public List<CallInterceptor> getCallInterceptors(){
		return baseImportConfig.getCallInterceptors();
	}
    public MetricsLogReport getMetricsLogReport(){
        return baseImportConfig.getMetricsLogReport();
    }


	public boolean isCurrentStoped(){
		return this.currentStoped;
	}

	public ScheduleConfig getScheduleConfig(){
		return baseImportConfig.getScheduleConfig();
	}
	public Boolean getFixedRate(){
		return baseImportConfig.getScheduleConfig().getFixedRate();
	}
	public ScheduleService getScheduleService(){
		return dataTranPlugin.getScheduleService();
	}
	public BaseImportConfig getImportConfig() {
		return baseImportConfig;
	}

    public Boolean isConfigIncreamentImport(){
        return baseImportConfig.getIncreamentImport();
    }

	public int getMaxRetry(){
		return baseImportConfig.getMaxRetry();
	}

	public boolean isAsyn(){
		return baseImportConfig.isAsyn();
	}
    public boolean validateIncreamentConfig(){
        if(this.isConfigIncreamentImport() != null && isConfigIncreamentImport() == true){
            if(baseImportConfig.getImportIncreamentConfig() == null
                    || !baseImportConfig.getImportIncreamentConfig().validate()){
                return false;
            }
            else{
                return true;
            }
        }
        else{
            if(baseImportConfig.getImportIncreamentConfig()  != null ){
                Integer type  = baseImportConfig.getImportIncreamentConfig().getLastValueType();
                if(type != null && (type == ImportIncreamentConfig.TIMESTAMP_TYPE 
                        || type == ImportIncreamentConfig.LOCALDATETIME_TYPE))
                {
                    String column = baseImportConfig.getImportIncreamentConfig().getLastValueColumn();
                    if(column == null || column.trim().equals("")){
                        return false;
                    }
                }
            }
            return true;
        }
       
    }

	public WrapedExportResultHandler getExportResultHandler(){
		return baseImportConfig.getExportResultHandler();
	}
//	@Override
//	public void flushLastValue(Object lastValue,Status currentStatus){
//		flushLastValue( lastValue, currentStatus,false);
//	}
    @Override
    public void flushLastValue(LastValueWrapper lastValue,Status currentStatus){
        flushLastValue( lastValue, currentStatus,false);
    }

//	@Override
//	public void flushLastValue(Object lastValue,Status currentStatus,boolean reachEOFClosed){
//
//		this.dataTranPlugin.flushLastValue(lastValue,currentStatus,  reachEOFClosed);
//	}

    @Override
    public void flushLastValue(LastValueWrapper lastValue,Status currentStatus,boolean reachEOFClosed){

        this.dataTranPlugin.flushLastValue(lastValue,currentStatus,  reachEOFClosed);
    }
	public boolean isLastValueDateType()
	{
		return baseImportConfig.isLastValueDateType();
	}
    public boolean isLastValueNumberType()
    {
        return baseImportConfig.isLastValueNumberType();
    }

	public boolean isLastValueStringType()
	{
		return baseImportConfig.isLastValueStringType();
	}
    public boolean isLastValueLocalDateTimeType()
    {
        return baseImportConfig.isLastValueLocalDateTimeType();
    }

	public Integer getLastValueType() {
		return baseImportConfig.getLastValueType();
	}

	@Override
	public DBConfig getStatusDbConfig() {
		return baseImportConfig.getStatusDbConfig();
	}

	@Override
	public boolean isExternalTimer() {
		return baseImportConfig.isExternalTimer();
	}
	public String getLastValueColumn(){
		return baseImportConfig.getLastValueColumn();
	}
	public String getLastValueColumnName(){
		return dataTranPlugin.getLastValueClumnName();
	}
	public boolean isImportIncreamentConfigSetted(){
		return baseImportConfig.isImportIncreamentConfigSetted();
	}

	@Override
	public Object getConfigLastValue() {
		return baseImportConfig.getConfigLastValue();
	}

	@Override
	public String getLastValueStoreTableName() {
		return baseImportConfig.getLastValueStoreTableName();
	}

	@Override
	public String getLastValueStorePath() {
		return baseImportConfig.getLastValueStorePath();
	}


    @Override
    public String getLastValueStorePassword(){
        return baseImportConfig.getLastValueStorePassword();
    }


//	public Object getValue(String columnName) throws ESDataImportException {
//		try {
//			return jdbcResultSet.getValue(columnName);
//		}
//		catch (Exception e){
//			throw new ESDataImportException(e);
//		}
//	}
//
//	public Object getDateTimeValue(String columnName) throws ESDataImportException {
//		try {
//			return jdbcResultSet.getDateTimeValue(columnName);
//		}
//		catch (Exception e){
//			throw new ESDataImportException(e);
//		}
//
//	}



	public boolean needUpdateLastValueWrapper(LastValueWrapper oldValue,LastValueWrapper newValue){
		return dataTranPlugin.needUpdateLastValueWrapper(this.getLastValueType(),oldValue,  newValue);

	}

//    public boolean needUpdate(Object oldValue,Object newValue){
//        return BaseStatusManager.needUpdate(this.getLastValueType(),oldValue,  newValue);
//
//    }
    @Override
    public LastValueWrapper max(LastValueWrapper oldValue, Record record){
        return dataTranPlugin.maxLastValue(oldValue,record);
    }

//    @Override
//	public Object max(Object oldValue,Object newValue){
//
//		return BaseStatusManager.max(this.getLastValueType(),oldValue,newValue);
//
//	}
	public void setLastValueDateformat(String lastValueDateformat){
		this.baseImportConfig.setLastValueDateformat(lastValueDateformat);
	}
	public String getLastValueDateformat(){
		return this.baseImportConfig.getLastValueDateformat();
	}
	public void setLastValueType(int lastValueType){
		this.baseImportConfig.setLastValueType(lastValueType);
	}
	public int getThreadCount(){
		return baseImportConfig.getThreadCount();
	}
	public boolean isParallel(){
		return baseImportConfig.isParallel();
	}

	public int getQueue(){
		return baseImportConfig.getQueue();
	}


 
	public void resume(){
		this.currentStoped = false;
	}
	public String getDateFormat(){
		return this.baseImportConfig.getDateFormat();
	}
	public String getLocale(){
		return this.baseImportConfig.getLocale();
	}
	public String getTimeZone(){
		return this.baseImportConfig.getTimeZone();
	}
//	private AtomicInteger rejectCounts = new AtomicInteger();
	private ExecutorService blockedExecutor;
    private Object blockedExecutorLock = new Object();
	public ExecutorService buildThreadPool(){
		if(blockedExecutor != null)
			return blockedExecutor;
		synchronized (blockedExecutorLock) {
			if(blockedExecutor == null) {
				blockedExecutor = ThreadPoolFactory.buildThreadPool("DataTranTaskThread","DataTranTaskThread",
						getThreadCount(),getQueue(),
						-1l
						,1000);
			}
		}
		return blockedExecutor;
	}

    private ExecutorService recordHandlerExecutor;
    private Object recordHandleExecutorLock = new Object();
    public ExecutorService buildRecordHandlerExecutor(){
        if(recordHandlerExecutor != null)
            return recordHandlerExecutor;
        synchronized (recordHandleExecutorLock) {
            if(recordHandlerExecutor == null) {
                recordHandlerExecutor = ThreadPoolFactory.buildThreadPool("DataTranRecordHandlerThread","DataTranRecordHandlerThread",
                        getThreadCount(),getQueue(),
                        -1l
                        ,1000);
            }
        }
        return blockedExecutor;
    }
	public Integer getStoreBatchSize(){
		if(baseImportConfig.getScheduleBatchSize() == null){
			return baseImportConfig.getBatchSize();
		}
		return baseImportConfig.getScheduleBatchSize();
	}
	public String getStatusTableId(){
		return this.baseImportConfig.getStatusTableId();
	}
	public boolean isFromFirst(){
		return baseImportConfig.isFromFirst();
	}



	public void setBatchSize(int batchSize){
		baseImportConfig.setBatchSize(batchSize);
	}
	@Override
	public boolean isSchedulePaused(boolean autoPause){
		return this.dataTranPlugin.isSchedulePaussed(  autoPause);
	}

	public ImportEndAction getImportEndAction(){
		return this.baseImportConfig.getImportEndAction();
	}
	public String getJobName() {
		return baseImportConfig.getJobName();
	}
	public Date getScheduleDate(){
		return this.baseImportConfig.getScheduleDate();
	}

	public Date getScheduleEndDate(){
		return this.baseImportConfig.getScheduleEndDate();
	}

	public Long getDeyLay(){
		return this.baseImportConfig.getDeyLay();
	}
	public String getJobId() {
		return baseImportConfig.getJobId();
	}
	public String getJobType(){
		return this.dataTranPlugin.getJobType();
	}

	public String getDataTimeField(){
		return baseImportConfig.getDataTimeField();
	}

	public List<ETLMetrics> getMetrics(){
		return baseImportConfig.getMetrics();
	}

	@Override
	public boolean isUseDefaultMapData() {
		return baseImportConfig.isUseDefaultMapData();
	}

    public Integer getTimeWindowType() {
        return baseImportConfig.getTimeWindowType();
    }
    /**
     * 在不需要时间窗口的场景下，控制采集和指标计算混合作业定时调度时，是否在任务结束时强制flush metric进行持久化处理
     * true 强制flush
     * false 不强制刷新 默认值
     * @return
     */
    @Override
    public boolean isFlushMetricsOnScheduleTaskCompleted(){
        return baseImportConfig.isFlushMetricsOnScheduleTaskCompleted();
    }

    /**
     * 控制flush metrics时是否清空指标key内存缓存区
     * true 清空
     * false 不清空，默认值
     * @return
     */
    @Override
    public boolean isCleanKeysWhenflushMetricsOnScheduleTaskCompleted() {
        return baseImportConfig.isCleanKeysWhenflushMetricsOnScheduleTaskCompleted();
    }
    /**
     * 控制是否等待flush metric持久化操作完成再返回，还是不等待直接返回（异步flush）
     * true 等待，默认值
     * false 不等待
     * @return
     */
    @Override
    public boolean isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted() {
        return baseImportConfig.isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted();
    }

	@Override
	public DataStream getDataStream() {
		return dataStream;
	}

	public void setDataStream(DataStream dataStream) {
		this.dataStream = dataStream;
	}
	@Override
	public void shutDownJob(Throwable throwable, boolean waitTranStopped,boolean fromScheduleEnd){
		if(dataStream != null)
			this.dataStream.destroy(throwable,waitTranStopped,fromScheduleEnd);
	}
    
    @Override
    public void initETLMetrics(){
        List<ETLMetrics> metrics = baseImportConfig.getMetrics();
        if(metrics == null){
            metrics = outputConfig.getMetrics();
        }
        if(metrics != null && metrics.size() > 0){            
            for(ETLMetrics metrics_: metrics){
                metrics_.setImportContext(this);
                metrics_.init();
            }
        }
        
        
        
         
    }

    public void initJobcontext(){
        jobContext.setDataTranPlugin(dataTranPlugin);
        if(baseImportConfig.getInitJobContextCall() != null){
            try {
                baseImportConfig.getInitJobContextCall().initJobContext(jobContext);
            }
            catch (Exception e){
                throw new DataImportException(e);
            }
        }

    }
    public    void resetMetricsLogLevel(int metricsLogLevel){
        this.baseImportConfig.resetMetricsLogLevel(  metricsLogLevel);
    }
}
