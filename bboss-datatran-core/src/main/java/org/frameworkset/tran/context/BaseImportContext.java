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
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.ouput.custom.CustomOutPut;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.BaseStatusManager;
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
public  class BaseImportContext implements ImportContext {
	protected BaseImportConfig baseImportConfig;
	protected InputConfig inputConfig;
	protected OutputConfig outputConfig;
	public JobTaskMetrics createJobTaskMetrics(){
		return new JobTaskMetrics();
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
		return inputConfig.getInputPlugin(this);
	}

	@Override
	public OutputPlugin getOutputPlugin() {
		return outputConfig.getOutputPlugin(this);
	}

	public boolean isLastValueColumnSetted() {
		return baseImportConfig.isLastValueColumnSetted();
	}

	public Map getParams(){
		return baseImportConfig.getParams();
	}
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;

	}
	public CustomOutPut getCustomOutPut(){
		return baseImportConfig.getCustomOutPut();
	}
	public String[] getExportColumns(){
		return  baseImportConfig.getExportColumns();
	}
	public boolean useFilePointer(){
		return false;
	}
	//	private JDBCResultSet jdbcResultSet;
	private DataTranPlugin dataTranPlugin;
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
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 * @return
	 */
	public Integer increamentEndOffset(){
		return baseImportConfig.getIncreamentEndOffset();
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
	public Context buildContext(TaskContext taskContext,TranResultSet tranResultSet, BatchContext batchContext){
		return dataTranPlugin.buildContext( taskContext,tranResultSet,batchContext);
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
		baseImportConfig.setStatusTableId(hashCode);
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
	public void destroy(boolean waitTranStop) {
//		if(dataTranPlugin != null){
//			dataTranPlugin.destroy();
//		}
		this.dataTranPlugin.destroy(  waitTranStop);
		try {
			if (blockedExecutor != null) {
				blockedExecutor.shutdown();
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

	public int getMaxRetry(){
		return baseImportConfig.getMaxRetry();
	}

	public boolean isAsyn(){
		return baseImportConfig.isAsyn();
	}


	public WrapedExportResultHandler getExportResultHandler(){
		return baseImportConfig.getExportResultHandler();
	}


	@Override
	public void flushLastValue(Object lastValue,Status currentStatus,boolean reachEOFClosed){
		Long timeLastValue = this.getTimeRangeLastValue();
		if(timeLastValue != null){

			lastValue = max(lastValue,new Date(timeLastValue));
		}
		this.dataTranPlugin.flushLastValue(lastValue,currentStatus,  reachEOFClosed);
	}
	public boolean isLastValueDateType()
	{
		return baseImportConfig.isLastValueDateType();
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

	public DataTranPlugin getDataTranPlugin() {
		return dataTranPlugin;
	}

	public boolean needUpdate(Object oldValue,Object newValue){
		return BaseStatusManager.needUpdate(this.getLastValueType(),oldValue,  newValue);

	}
	public Object max(Object oldValue,Object newValue){
		return BaseStatusManager.max(this.getLastValueType(),oldValue,newValue);

	}
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
	public ExecutorService buildThreadPool(){
		if(blockedExecutor != null)
			return blockedExecutor;
		synchronized (this) {
			if(blockedExecutor == null) {
				blockedExecutor = ThreadPoolFactory.buildThreadPool("DataTranThread","DataTranThread",
						getThreadCount(),getQueue(),
						-1l
						,1000);
//				blockedExecutor = new ThreadPoolExecutor(getThreadCount(), getThreadCount(),
//						0L, TimeUnit.MILLISECONDS,
//						new ArrayBlockingQueue<Runnable>(getQueue()),
//						new ThreadFactory() {
//							private AtomicInteger threadCount = new AtomicInteger(0);
//
//							@Override
//							public Thread newThread(Runnable r) {
//								int num = threadCount.incrementAndGet();
//								return new DBESThread(r, num);
//							}
//						}, new BlockedTaskRejectedExecutionHandler(rejectCounts));
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
	public Integer getStatusTableId(){
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

}
