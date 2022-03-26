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
import com.frameworkset.orm.annotation.ESIndexWrapper;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.es.ESConfig;
import org.frameworkset.tran.es.ESField;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.ouput.custom.CustomOutPut;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public abstract  class BaseImportContext implements ImportContext {
	protected BaseImportConfig baseImportConfig;
	public JobTaskMetrics createJobTaskMetrics(){
		return new JobTaskMetrics();
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
	public String getTargetDBName(){
		String dbName = baseImportConfig.getTargetDbname();
		if(dbName == null){
			dbName = getSourceDBName();
		}
		return dbName;

	}
	public String getSourceDBName(){
		return baseImportConfig.getSourceDbname();
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
	public ESConfig getESConfig(){
		return baseImportConfig.getESConfig();
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
	public void setEsIdField(ESField esIdField) {
		baseImportConfig.setEsIdField(  esIdField);
	}
	public void setEsIdField(String esIdField) {
		baseImportConfig.setEsIdField( new ESField(false, esIdField));
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

	@Override
	public DBConfig getDbConfig() {
		return baseImportConfig.getDbConfig();
	}
	@Override
	public Integer getJDBCFetchsize(){
		if(baseImportConfig.getJdbcFetchsize() != null)
			return baseImportConfig.getJdbcFetchsize();
		if(baseImportConfig.getDbConfig() == null){
			return null;
		}
		else{
			return baseImportConfig.getDbConfig().getJdbcFetchSize();
		}
	}
	@Override
	public boolean isEnableDBTransaction(){
		if(baseImportConfig.getEnableDBTransaction() != null)
			return baseImportConfig.getEnableDBTransaction();
		if(baseImportConfig.getDbConfig() == null){
				return false;
		}
		else{
			return baseImportConfig.getDbConfig().isEnableDBTransaction();
		}
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
	public String getTargetElasticsearch(){
		return baseImportConfig.getTargetElasticsearch();
	}
	public String getSourceElasticsearch(){
		return baseImportConfig.getSourceElasticsearch();
	}
	@Override
	public ClientOptions getClientOptions() {
		return baseImportConfig.getClientOptions();
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
	@Override
	public void doImportData(TaskContext taskContext) {
		if(dataTranPlugin != null)
			dataTranPlugin.doImportData(  taskContext);
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

	public boolean isDebugResponse(){
		return baseImportConfig.isDebugResponse();
	}

	public boolean isDiscardBulkResponse(){
		return baseImportConfig.isDiscardBulkResponse();
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
		if(newValue == null)
			return false;

		if(oldValue == null)
			return true;
//		this.getLastValueType()
		if(this.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			Date oldValueDate = (Date)oldValue;
			Date newValueDate = (Date)newValue;
			if(newValueDate.after(oldValueDate))
				return true;
			else
				return false;
		}
		else{
//			Method compareTo = oldValue.getClass().getMethod("compareTo");
			if(oldValue instanceof Integer && newValue instanceof Integer){
				int e = ((Integer)oldValue).compareTo ((Integer)newValue);
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Long || newValue instanceof Long){
				boolean e = ((Number)oldValue).longValue() <= ((Number)newValue).longValue();
				if(e)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof BigDecimal){
				int e = ((BigDecimal)oldValue).compareTo ((BigDecimal)newValue);
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof Integer){
				boolean e = ((BigDecimal)oldValue).longValue() > ((Integer)newValue).intValue();
				if(!e )
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Integer && newValue instanceof BigDecimal){
				boolean e = ((BigDecimal)newValue).longValue() > ((Integer)oldValue).intValue();
				if(!e )
					return false;
				else
					return true;
			}
			else if(oldValue instanceof Double || newValue instanceof Double){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Float || newValue instanceof Float){
				int e = Float.compare(((Number)oldValue).floatValue(), ((Number)newValue).floatValue());
				if(e < 0)
					return true;
				else
					return false;
			}

			else if(oldValue instanceof BigDecimal || newValue instanceof BigDecimal){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return true;
				else
					return false;
			}
			else {
				boolean e = ((Number)oldValue).intValue() <= ((Number)newValue).intValue();
				if(e)
					return true;
				else
					return false;
			}

		}
	}
	public Object max(Object oldValue,Object newValue){
		if(newValue == null)
			return oldValue;

		if(oldValue == null)
			return newValue;
//		this.getLastValueType()
		if(this.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			Date oldValueDate = (Date)oldValue;
			Date newValueDate = (Date)newValue;
			if(newValueDate.after(oldValueDate))
				return newValue;
			else
				return oldValue;
		}
		else{
//			Method compareTo = oldValue.getClass().getMethod("compareTo");
			if(oldValue instanceof Integer && newValue instanceof Integer){
				int e = ((Integer)oldValue).compareTo ((Integer)newValue);
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Long || newValue instanceof Long){
				boolean e = ((Number)oldValue).longValue() <= ((Number)newValue).longValue();
				if(e)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof BigDecimal){
				int e = ((BigDecimal)oldValue).compareTo ((BigDecimal)newValue);
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof Integer){
				boolean e = ((BigDecimal)oldValue).longValue() > ((Integer)newValue).intValue();
				if(!e )
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Integer && newValue instanceof BigDecimal){
				boolean e = ((BigDecimal)newValue).longValue() > ((Integer)oldValue).intValue();
				if(!e )
					return oldValue;
				else
					return newValue;
			}
			else if(oldValue instanceof Double || newValue instanceof Double){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Float || newValue instanceof Float){
				int e = Float.compare(((Number)oldValue).floatValue(), ((Number)newValue).floatValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}

			else if(oldValue instanceof BigDecimal || newValue instanceof BigDecimal){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else {
				boolean e = ((Number)oldValue).intValue() <= ((Number)newValue).intValue();
				if(e)
					return newValue;
				else
					return oldValue;
			}

		}
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
	public ESIndexWrapper getEsIndexWrapper(){
		return baseImportConfig.getEsIndexWrapper();
	}


	public void setEsIndexWrapper(ESIndexWrapper esIndexWrapper) {
		this.baseImportConfig.setEsIndexWrapper( esIndexWrapper);
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
	private AtomicInteger rejectCounts = new AtomicInteger();
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



	public void setRefreshOption(String refreshOption){
		baseImportConfig.setRefreshOption(refreshOption);
	}
	public String getRefreshOption(){
		ClientOptions clientOptions = baseImportConfig.getClientOptions();
		return clientOptions != null?clientOptions.getRefreshOption():null;
	}

	public void setBatchSize(int batchSize){
		baseImportConfig.setBatchSize(batchSize);
	}


}
