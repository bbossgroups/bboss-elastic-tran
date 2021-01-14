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
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ScheduleConfig;
import org.frameworkset.tran.schedule.ScheduleService;
import org.frameworkset.tran.schedule.Status;

import java.util.List;
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
	Context buildContext(TranResultSet jdbcResultSet, BatchContext batchContext);
	ESConfig getESConfig();
	public Long getTimeRangeLastValue();
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
	void setRefreshOption(String refreshOption);
	void setBatchSize(int batchSize);
	public Integer getFetchSize() ;
	public void setEsIdField(ESField esIdField);
	public void setEsIdField(String esIdField);
	void destroy();
	public ExportCount getExportCount();
	void importData();
	public Object max(Object oldValue,Object newValue);
	boolean isContinueOnError();
	boolean isCurrentStoped();
	boolean assertCondition();

	Boolean getFixedRate();
	public boolean isSortLastValue();
	ScheduleConfig getScheduleConfig();

	List<CallInterceptor> getCallInterceptors();

	void doImportData();

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

	DBConfig getDbConfig();

	DataRefactor getDataRefactor();

	String getApplicationPropertiesFile();

	List<DBConfig> getConfigs();

	void setLastValueType(int lastValueType);

	int getMaxRetry();

	boolean isDebugResponse();

	boolean isDiscardBulkResponse();

	WrapedExportResultHandler getExportResultHandler();

	int getThreadCount();

	int getQueue();

	Status getCurrentStatus();

	String getRefreshOption();

	BaseImportConfig getImportConfig();

	void flushLastValue(Object lastValue);

	void stop();

	void setEsIndexWrapper(ESIndexWrapper esIndexWrapper);

	ESIndexWrapper getEsIndexWrapper();

	/**
	 * 返回配置的增量字段名称
	 * @return
	 */
	public String getLastValueColumn();

	/**
	 * 返回最终运算出来的增量字段名称
	 * @return
	 */
	String getLastValueColumnName();

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

	ClientOptions getClientOptions();

	String getTargetElasticsearch();
	String getSourceElasticsearch();
}
