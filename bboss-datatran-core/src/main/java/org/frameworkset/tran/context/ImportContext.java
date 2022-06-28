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
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 15:12
 * @author biaoping.yin
 * @version 1.0
 */
public interface ImportContext {
//	BaseImportConfig getBaseImportConfig();
	InputConfig getInputConfig();
	OutputConfig getOutputConfig();
	InputPlugin getInputPlugin();
	OutputPlugin getOutputPlugin();
	DataTranPlugin buildDataTranPlugin();
	public DBConfig getDefaultDBConfig();
	public List<DBConfig> getOhterDBConfigs();
	public boolean isLastValueColumnSetted();
	public String getSplitFieldName();
	public JobTaskMetrics createJobTaskMetrics();
	public SplitHandler getSplitHandler();
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin);
	public String[] getExportColumns();
//	DataTranPlugin buildDataTranPlugin();
//	public String getTargetElasticsearch();
	Context buildContext(TaskContext taskContext,TranResultSet tranResultSet, BatchContext batchContext);
//	ESConfig getESConfig();
	public Long getTimeRangeLastValue();
	public DataTranPlugin getDataTranPlugin();
	Map getParams();

	/**
	 * 判断调度任务是否被暂停
	 * @return
	 */
	public boolean isSchedulePaused(boolean autoPause);

	public boolean isAsynFlushStatus();
	public long getAsynFlushStatusInterval();
	public boolean serialAllData();
	public long getLogsendTaskMetric();
	public String getLastValueDateformat();
	/**
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  单位：秒
	 * @return
	 */
	Integer increamentEndOffset();

	/**
	 * 异步消费数据时，强制刷新检测空闲时间间隔，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作
	 * @return
	 */
	long getFlushInterval();
	public long getAsynResultPollTimeOut();
	/**
	 * 忽略空值字段，elasticsearch数据导入的时候可以统一开启开关
	 * @return
	 */
	boolean isIgnoreNullValueField();
	boolean isPrintTaskLog();
//	void setRefreshOption(String refreshOption);
	void setBatchSize(int batchSize);
	public Integer getFetchSize() ;
//	public void setEsIdField(ESField esIdField);
//	public void setEsIdField(String esIdField);
	void destroy(boolean waitTranStop);
	public ExportCount getExportCount();
	public Object max(Object oldValue,Object newValue);
	boolean isContinueOnError();
	boolean isCurrentStoped();
	boolean assertCondition();

	Boolean getFixedRate();
	public boolean isSortLastValue();
	ScheduleConfig getScheduleConfig();

	List<CallInterceptor> getCallInterceptors();

//	void doImportData(TaskContext taskContext);

	Integer getStatusTableId();

	boolean isFromFirst();



	Object getConfigLastValue();

	String getLastValueStoreTableName();

	String getLastValueStorePath();

	Integer getLastValueType();

	boolean isLastValueDateType();

	DBConfig getStatusDbConfig();

	boolean isExternalTimer();

	void setStatusTableId(int hashCode);
//	String getTargetDBName();
//	String getSourceDBName();
//	DBConfig getDbConfig();

//	Integer getJDBCFetchsize();

//	public boolean isEnableDBTransaction();
	DataRefactor getDataRefactor();

	String getApplicationPropertiesFile();

//	List<DBConfig> getConfigs();

	void setLastValueType(int lastValueType);

	int getMaxRetry();

//	boolean isDebugResponse();
//
//	boolean isDiscardBulkResponse();

	WrapedExportResultHandler getExportResultHandler();

	int getThreadCount();

	int getQueue();


//	String getRefreshOption();

	BaseImportConfig getImportConfig();

	void flushLastValue(Object lastValue,Status currentStatus,boolean reachEOFClosed);
	public boolean needUpdate(Object oldValue,Object newValue);

//	void setEsIndexWrapper(ESIndexWrapper esIndexWrapper);
//
//	ESIndexWrapper getEsIndexWrapper();

	/**
	 * 返回配置的增量字段名称
	 * @return
	 */
	public String getLastValueColumn();
	public boolean isImportIncreamentConfigSetted();

	/**
	 * 返回最终运算出来的增量字段名称
	 * @return
	 */
	String getLastValueColumnName();
	boolean useFilePointer();
	boolean isParallel();

	ScheduleService getScheduleService();

	boolean isAsyn();

	Integer getStoreBatchSize();

	ExecutorService buildThreadPool();

	void resume();

	String getDateFormat();
	public String getLocale();
	public String getTimeZone();
	public int getTranDataBufferQueue();
//	public Object getValue(String columnName) throws ESDataImportException;
//	public Object getDateTimeValue(String columnName) throws ESDataImportException;
	void setDataRefactor( DataRefactor dataRefactor);

//	ClientOptions getClientOptions();
//
//	String getSourceElasticsearch();
	public boolean isIncreamentImport();
}
