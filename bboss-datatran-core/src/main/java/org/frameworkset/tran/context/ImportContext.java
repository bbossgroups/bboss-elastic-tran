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
import org.frameworkset.tran.metrics.DataTranPluginMetricsLogAPI;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.metrics.MetricsLogReport;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.util.ResourceEnd;
import org.frameworkset.util.ResourceStart;

import java.util.Date;
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
public interface ImportContext extends DataTranPluginMetricsLogAPI {
	public DataStream getDataStream();
    public int getMetricsLogLevel() ;

    public String buildStatusId( int hashCode);
	public void cleanResource();
    public void registEndAction(EndAction endAction);
    public EndAction getEndAction();
    public Map<String, DynamicParam> getJobDynamicInputParams() ;
	public Map<String, DynamicParam> getJobDynamicOutputParams();
	public Map getJobOutputParams();
    public Integer getStatusIdPolicy() ;
    public JobClosedListener getJobClosedListener();

    public boolean isSerial();

    public boolean isParallelBatch();

    public boolean isBatch();
    /**
     * 输入参数组，将输入参数和输入动态参数组装为一个参数组添加到参数组集合中，添加完成后重置输入参数和输入动态参数，为增加新的参数组做准备
     * 通过添加多个参数组，作业调度时，特定的输入插件可以利用参数组中的每组参数发起并发数据请求，比如httpinput插件
     */
    public List<JobInputParamGroup> getJobInputParamGroups();
	public JobContext getJobContext();
    public Object getJobContextData(String name);
	/**
	 * 对作业依赖的资源进行初始化处理
	 * @return
	 */
	public ImportContext addResourceStart(ResourceStart resourceStart);

	/**
	 * 销毁initResources初始化阶段的初始化的依赖资源
	 */
	public void destroyResources(ResourceEnd resourceEnd);

	//	BaseImportConfig getBaseImportConfig();
	InputConfig getInputConfig();
	ImportStartAction getImportStartAction();

    /**
     * 获取输出插件配置
     * @return
     */
	OutputConfig getOutputConfig();

    /**
     * 搜索对应的类型输出配置类，如果是单输出源，直接判别单输出源配置类是否是对应类型，如果不是返回null；
     * 如果是多输出源，则从多输出源配置中筛选出第一个符合类型的配置类
     * @param outputConfigClass
     * @return
     */
    <T extends OutputConfig> T getOutputConfig(Class<T> outputConfigClass);

    /**
     * 搜索对应的类型所有输出配置类，如果是单输出源，直接判别单输出源配置类是否是对应类型，如果不是返回null；
     * 如果是多输出源，则从多输出源配置中筛选出第一个符合类型的配置类
     * @param outputConfigClass
     * @return
     */
    <T extends OutputConfig> List<T> getOutputConfigs(Class<T> outputConfigClass);
    
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
	Context buildContext(TaskContext taskContext, Record record, BatchContext batchContext);
	public Long getTimeRangeLastValue();
	public DataTranPlugin getDataTranPlugin();
	Map getJobInputParams();
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
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的偏移量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  单位：秒
	 * @return
	 */
	Integer increamentEndOffset();

    /**
     * 标识数字类型增量字段是否是时间戳，如果是时间戳，那么increamentEndOffset配置将起作用
     * @return
     */
    boolean isNumberTypeTimestamp();

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
	void destroy(DestroyPolicy destroyPolicy);

	public void shutDownJob(Throwable throwable, boolean waitTranStopped,boolean fromScheduleEnd);
    /**
     * 等待转换结束，后再结束作业
     */
    void finishAndWaitTran(Throwable throwable);

    public ExportCount getExportCount();
	public LastValueWrapper max(LastValueWrapper oldValue, Record record);
//    public Object max(Object oldValue, Object baseDataTran);
	boolean isContinueOnError();
	boolean isCurrentStoped();
	boolean assertCondition();

	Boolean getFixedRate();
	public boolean isSortLastValue();
	ScheduleConfig getScheduleConfig();

	List<CallInterceptor> getCallInterceptors();
    MetricsLogReport getMetricsLogReport();

//	void doImportData(TaskContext taskContext);

	String getStatusTableId();

	boolean isFromFirst();



	Object getConfigLastValue();

	String getLastValueStoreTableName();

	String getLastValueStorePath();
    String getLastValueStorePassword();

	Integer getLastValueType();

	boolean isLastValueDateType();
    boolean isLastValueNumberType();
    boolean isLastValueLocalDateTimeType();
	public boolean isLastValueStringType();
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

    WrapedExportResultHandler getExportResultHandler(OutputConfig outputConfig);

    WrapedExportResultHandler getExportResultHandler();

	int getThreadCount();

	int getQueue();


//	String getRefreshOption();

	BaseImportConfig getImportConfig();

    /**
     * 判断通过配置设置增量导入标记，如果配置为增量导入，但是没有指定增量配置（譬如：增量字段信息），抛出异常
     * @return
     */
    Boolean isConfigIncreamentImport();
    
    boolean validateIncreamentConfig();
//	void flushLastValueWrapper(LastValueWrapper lastValue,Status currentStatus);
    void flushLastValue(LastValueWrapper lastValue,Status currentStatus);
	void flushLastValue(LastValueWrapper lastValue,Status currentStatus,boolean reachEOFClosed);
//    public void flushLastValueWrapper(LastValueWrapper lastValue,Status currentStatus,boolean reachEOFClosed);
	public boolean needUpdateLastValueWrapper(LastValueWrapper oldValue,LastValueWrapper newValue);
//    public boolean needUpdate(Object oldValue,Object newValue);
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
	public ImportEndAction getImportEndAction();

	public Date getScheduleDate();

	public Date getScheduleEndDate();

	public Long getDeyLay();
	public String getJobName();
	public String getJobId();
	public String getJobType();

    String getDataTimeField();
	List<ETLMetrics> getMetrics();

	boolean isUseDefaultMapData();
    public Integer getTimeWindowType();
    /**
     * 在不需要时间窗口的场景下，控制采集和指标计算混合作业定时调度时，是否在任务结束时强制flush metric进行持久化处理
     * true 强制flush
     * false 不强制刷新 默认值
     * @return
     */
    public boolean isFlushMetricsOnScheduleTaskCompleted();
    /**
     * 控制是否等待flush metric持久化操作完成再返回，还是不等待直接返回（异步flush）
     * true 等待，默认值
     * false 不等待
     * @return
     */
    public boolean isWaitCompleteWhenflushMetricsOnScheduleTaskCompleted();
    /**
     * 控制flush metrics时是否清空指标key内存缓存区
     * true 清空
     * false 不清空，默认值
     * @return
     */
    public boolean isCleanKeysWhenflushMetricsOnScheduleTaskCompleted();

    void initETLMetrics();

    void initJobcontext();

    /**
     * 重置日志级别,无需重启作业
     * @param metricsLogLevel
     */
    void resetMetricsLogLevel(int metricsLogLevel);
}
