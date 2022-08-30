package org.frameworkset.tran.config;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.spi.geoip.GeoIPUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.ScheduleConfig;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/24 10:19
 * @author biaoping.yin
 * @version 1.0
 */
public class BaseImportConfig {
	protected  final Logger logger = LoggerFactory.getLogger(this.getClass());
	private List<DBConfig> configs;
	private DBConfig defaultDBConfig ;
	private Map<String,DBConfig> dbConfigMap = new LinkedHashMap<>();
	private boolean sortLastValue ;

	private ImportStartAction importStartAction;

	private ImportEndAction importEndAction;
	/**
	 * 任务开始时间
	 */
	private Date scheduleDate;
	/**
	 * 任务结束时间
	 */
	private Date scheduleEndDate;

	private Long deyLay;
	private String jobName;
	private String jobId;
	public void setImportStartAction(ImportStartAction importStartAction) {
		this.importStartAction = importStartAction;
	}

	public ImportStartAction getImportStartAction() {
		return importStartAction;
	}

	public boolean isLastValueColumnSetted() {
		return lastValueColumnSetted;
	}

	public void setLastValueColumnSetted(boolean lastValueColumnSetted) {
		this.lastValueColumnSetted = lastValueColumnSetted;
	}

	private boolean  lastValueColumnSetted = false;
	public boolean isImportIncreamentConfigSetted(){
		return this.importIncreamentConfig != null;
	}

	public Map getJobInputParams() {
		return jobInputParams;
	}

	public void setJobInputParams(Map jobInputParams) {
		this.jobInputParams = jobInputParams;
	}

	private Map jobInputParams;

	public Map getJobOutputParams() {
		return jobOutputParams;
	}

	public void setJobOutputParams(Map jobOutputParams) {
		this.jobOutputParams = jobOutputParams;
	}
	private Map<String,DynamicParam> jobDynamicInputParams;
	private Map<String,DynamicParam> jobDynamicOutputParams;
	private Map jobOutputParams;
	public boolean isSerialAllData() {
		return serialAllData;
	}

	public void setSerialAllData(boolean serialAllData) {
		this.serialAllData = serialAllData;
	}



	public void setJobDynamicInputParams(Map<String, DynamicParam> jobDynamicInputParams) {
		this.jobDynamicInputParams = jobDynamicInputParams;
	}
	public Map<String, DynamicParam> getJobDynamicInputParams() {
		return jobDynamicInputParams;
	}
	public Map<String, DynamicParam> getJobDynamicOutputParams() {
		return jobDynamicOutputParams;
	}

	public void setJobDynamicOutputParams(Map<String, DynamicParam> jobDynamicOutputParams) {
		this.jobDynamicOutputParams = jobDynamicOutputParams;
	}

	private boolean serialAllData;

	public long getLogsendTaskMetric() {
		return logsendTaskMetric;
	}

	public void setLogsendTaskMetric(long logsendTaskMetric) {
		this.logsendTaskMetric = logsendTaskMetric;
	}

	protected long logsendTaskMetric = 10000l;
	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 */
	private long flushInterval = 8000l;
	private long asynResultPollTimeOut = 3000l;
	/**
	 * 单位：秒
	 * 对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 */
	private Integer increamentEndOffset;
	public String[] getExportColumns() {
		return exportColumns;
	}

	/**
	 * 是否采用异步模式存储增量状态，默认true
	 * true 异步模式
	 * false 同步模式
	 */
	private boolean asynFlushStatus = true;


	private String splitFieldName;


	private SplitHandler splitHandler;
	public long getAsynFlushStatusInterval() {
		return asynFlushStatusInterval;
	}

	public void setAsynFlushStatusInterval(long asynFlushStatusInterval) {
		this.asynFlushStatusInterval = asynFlushStatusInterval;
	}

	private long asynFlushStatusInterval = 10000;
	public void setIncreamentEndOffset(Integer increamentEndOffset) {
		this.increamentEndOffset = increamentEndOffset;
	}

	/**
	 * 单位：秒
	 * @return
	 */
	public Integer getIncreamentEndOffset() {
		return increamentEndOffset;
	}

	public void setExportColumns(String[] exportColumns) {
		this.exportColumns = exportColumns;
	}

	private String[] exportColumns;


	public boolean isIgnoreNullValueField() {
		return ignoreNullValueField;
	}

	public long getAsynResultPollTimeOut() {
		return asynResultPollTimeOut;
	}

	public void setAsynResultPollTimeOut(long asynResultPollTimeOut) {
		this.asynResultPollTimeOut = asynResultPollTimeOut;
	}

	public void setIgnoreNullValueField(boolean ignoreNullValueField) {
		this.ignoreNullValueField = ignoreNullValueField;
	}

	private boolean ignoreNullValueField;
	public Integer getFetchSize() {
		return fetchSize;
	}

	public void setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
	}

	//scroll分页检索，每批查询数据大小
	private Integer fetchSize = 5000;

	public void setTranDataBufferQueue(int tranDataBufferQueue) {
		this.tranDataBufferQueue = tranDataBufferQueue;
	}

	/**
	 * 源数据批量预加载队列大小，需要用到的最大缓冲内存为：
	 *  tranDataBufferQueue * fetchSize * 单条记录mem大小
	 */
	private int tranDataBufferQueue = 10;
//	private Object esDetectNoop;
//	protected List<String> sourceUpdateExcludes;
//	protected List<String> sourceUpdateIncludes;
//	protected String timeout = "30s";
//	protected String masterTimeout = "30s";
//	protected Integer waitForActiveShards;


	/**
	 * 增量导入状态存储数据源
	 */
	private DBConfig statusDbConfig;

	/**
	 * 打印任务日志
	 */
	private boolean printTaskLog = false;

	/**
	 * 定时任务拦截器
	 */
	private List<CallInterceptor> callInterceptors;
	private WrapedExportResultHandler exportResultHandler;


	public String getApplicationPropertiesFile() {
		return applicationPropertiesFile;
	}

	public void setApplicationPropertiesFile(String applicationPropertiesFile) {
		this.applicationPropertiesFile = applicationPropertiesFile;
	}
//	private BaseImportBuilder importBuilder;
	/**
	 * use parallel import:
	 *  true yes
	 *  false no
	 */
	private boolean parallel;
	/**
	 * parallel import work thread nums,default 200
	 */
	private int threadCount = 200;
	private int queue = Integer.MAX_VALUE;
	private String applicationPropertiesFile;
//	private ESField esIdField;
//	private ESField esParentIdField;
//	private Object esParentIdValue;
//	private ESField routingField;
//	private String routingValue;
//	private Boolean esDocAsUpsert;
//	private Integer esRetryOnConflict;
//	private Boolean esReturnSource;
//	private ESField esVersionField;
//	private Object esVersionValue;
//	private String esVersionType;
	private Boolean useJavaName;

	public Boolean getUseLowcase() {
		return useLowcase;
	}

	public void setUseLowcase(Boolean useLowcase) {
		this.useLowcase = useLowcase;
	}

	private Boolean useLowcase;
	private String dateFormat;
	private String locale;
	private String timeZone;
	private DateFormat format;
	/**
	 * 以字段的小写名称为key
	 */
	private Map<String, FieldMeta> fieldMetaMap;
	private List<FieldMeta> fieldValues;

	public Map<String, FieldMeta> getValuesIdxByName() {
		return valuesIdxByName;
	}

	public  FieldMeta getValueIdxByName(String fieldName) {
		return valuesIdxByName != null ?valuesIdxByName.get(fieldName):null;
	}

	public void setValuesIdxByName(Map<String, FieldMeta> valuesIdxByName) {
		this.valuesIdxByName = valuesIdxByName;
	}

	private Map<String,FieldMeta> valuesIdxByName;
	private DataRefactor dataRefactor;
//	private String sql;
//	private String sqlFilepath;
//	private String sqlName;
//	private String refreshOption;
	private int batchSize;
//	private ConfigSQLExecutor executor;



	private Integer scheduleBatchSize ;
//	private String index;




	private boolean asyn;
	private boolean continueOnError;


	private ScheduleConfig scheduleConfig;
	private ImportIncreamentConfig importIncreamentConfig;
	private Map<String, Object> geoipConfig;







//	public String getIndex() {
//		return index;
//	}
//
//	public void setIndex(String index) {
//		this.index = index;
//	}
//
//	public String getIndexType() {
//		return indexType;
//	}
//
//	public void setIndexType(String indexType) {
//		this.indexType = indexType;
//	}



	public String getDateFormat() {
		return dateFormat;
	}

	public void setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
	}

	public String getLocale() {
		return locale;
	}

	public void setLocale(String locale) {
		this.locale = locale;
	}

	public String getTimeZone() {
		return timeZone;
	}

	public void setTimeZone(String timeZone) {
		this.timeZone = timeZone;
	}



//
//	public Integer getEsRetryOnConflict() {
//		return esRetryOnConflict;
//	}
//
//	public void setEsRetryOnConflict(Integer esRetryOnConflict) {
//		this.esRetryOnConflict = esRetryOnConflict;
//	}
//
//	public Boolean getEsDocAsUpsert() {
//		return esDocAsUpsert;
//	}
//
//	public void setEsDocAsUpsert(Boolean esDocAsUpsert) {
//		this.esDocAsUpsert = esDocAsUpsert;
//	}
//
//	public String getRoutingValue() {
//		return routingValue;
//	}
//
//	public void setRoutingValue(String routingValue) {
//		this.routingValue = routingValue;
//	}
//
//	public ESField getRoutingField() {
//		return routingField;
//	}
//
//	public void setRoutingField(ESField routingField) {
//		this.routingField = routingField;
//	}
//
//	public ESField getEsParentIdField() {
//		return esParentIdField;
//	}
//
//	public void setEsParentIdField(ESField esParentIdField) {
//		this.esParentIdField = esParentIdField;
//	}
//
//	public ESField getEsIdField() {
//		return esIdField;
//	}
//
//	public void setEsIdField(ESField esIdField) {
//		this.esIdField = esIdField;
//	}
//
//	public Boolean getEsReturnSource() {
//		return esReturnSource;
//	}
//
//	public void setEsReturnSource(Boolean esReturnSource) {
//		this.esReturnSource = esReturnSource;
//	}
//
//	public ESField getEsVersionField() {
//		return esVersionField;
//	}
//
//	public void setEsVersionField(ESField esVersionField) {
//		this.esVersionField = esVersionField;
//	}
//
//	public String getEsVersionType() {
//		return esVersionType;
//	}
//
//	public void setEsVersionType(String esVersionType) {
//		this.esVersionType = esVersionType;
//	}

	public Boolean getUseJavaName() {
		return useJavaName;
	}

	public void setUseJavaName(Boolean useJavaName) {
		this.useJavaName = useJavaName;
	}
	public DateFormateMeta getDateFormateMeta(){
		return DateFormateMeta.buildDateFormateMeta(this.dateFormat,this.locale,this.timeZone);
	}

	public DateFormat getFormat() {
		if(format == null)
		{
			DateFormateMeta dateFormateMeta = getDateFormateMeta();
			if(dateFormateMeta == null){
				dateFormateMeta = SerialUtil.getDateFormateMeta();
			}
			format = dateFormateMeta.toDateFormat();
		}
		return format;
	}

	public void setFormat(DateFormat format) {
		this.format = format;
	}

	public Map<String, FieldMeta> getFieldMetaMap() {
		return fieldMetaMap;
	}

	public void destroy(){
		this.format = null;

	}

	public void setFieldMetaMap(Map<String, FieldMeta> fieldMetaMap) {
		this.fieldMetaMap = fieldMetaMap;
	}

	public FieldMeta getMappingName(String sourceFieldName){
		if(fieldMetaMap != null)
			return this.fieldMetaMap.get(sourceFieldName);
		return null;
	}



	public boolean isParallel() {
		return parallel;
	}

	public void setParallel(boolean parallel) {
		this.parallel = parallel;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public void setThreadCount(int threadCount) {
		this.threadCount = threadCount;
	}


	public int getQueue() {
		return queue;
	}

	public void setQueue(int queue) {
		this.queue = queue;
	}


	public boolean isAsyn() {
		return asyn;
	}

	public void setAsyn(boolean asyn) {
		this.asyn = asyn;
	}



	public List<FieldMeta> getFieldValues() {
		return fieldValues;
	}

	public void setFieldValues(List<FieldMeta> fieldValues) {
		this.fieldValues = fieldValues;
	}

	public DataRefactor getDataRefactor() {
		return dataRefactor;
	}

	public void setDataRefactor(DataRefactor dataRefactor) {
		this.dataRefactor = dataRefactor;
	}




//	public BaseImportBuilder getImportBuilder() {
//		return importBuilder;
//	}
//
//	public void setImportBuilder(BaseImportBuilder importBuilder) {
//		this.importBuilder = importBuilder;
//	}

//	/**
//	 * 补充额外的字段和值
//	 * @param fieldName
//	 * @param value
//	 * @return
//	 */
//	public BaseImportConfig addFieldValue(String fieldName, Object value){
//		this.importBuilder.addFieldValue(fieldName,value);
//		return this;
//	}

//	/**
//	 * 补充额外的字段和值
//	 * @param fieldName
//	 * @param dateFormat
//	 * @param value
//	 * @return
//	 */
//	public BaseImportConfig addFieldValue(String fieldName, String dateFormat, Object value){
//		this.importBuilder.addFieldValue(fieldName,dateFormat,value);
//		return this;
//	}

//	/**
//	 * 补充额外的字段和值
//	 * @param fieldName
//	 * @param dateFormat
//	 * @param value
//	 * @return
//	 */
//	public BaseImportConfig addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone){
//		this.importBuilder.addFieldValue(fieldName,dateFormat,value,  locale,  timeZone);
//		return this;
//	}
//
//	public BaseImportConfig addFieldMapping(String dbColumnName, String esFieldName){
//		this.importBuilder.addFieldMapping(dbColumnName,  esFieldName);
//		return this;
//	}
//
//	public BaseImportConfig addIgnoreFieldMapping(String dbColumnName){
//		this.importBuilder.addIgnoreFieldMapping(dbColumnName);
//		return this;
//	}
//
//	public BaseImportConfig addFieldMapping(String dbColumnName, String esFieldName, String dateFormat){
//		this.importBuilder.addFieldMapping(dbColumnName,  esFieldName,  dateFormat);
//		return this;
//	}
//
//	public BaseImportConfig addFieldMapping(String dbColumnName, String esFieldName, String dateFormat, String locale, String timeZone){
//		this.importBuilder.addFieldMapping(dbColumnName,  esFieldName,  dateFormat,locale,  timeZone);
//		return this;
//	}

//	public Object getEsParentIdValue() {
//		return esParentIdValue;
//	}
//
//	public void setEsParentIdValue(Object esParentIdValue) {
//		this.esParentIdValue = esParentIdValue;
//	}
//
//	public Object getEsVersionValue() {
//		return esVersionValue;
//	}
//
//	public void setEsVersionValue(Object esVersionValue) {
//		this.esVersionValue = esVersionValue;
//	}



	public ScheduleConfig getScheduleConfig() {
		return scheduleConfig;
	}

	public void setScheduleConfig(ScheduleConfig scheduleConfig) {
		this.scheduleConfig = scheduleConfig;
	}

	public ImportIncreamentConfig getImportIncreamentConfig() {
		return importIncreamentConfig;
	}
	public void setStatusTableId(Integer statusTableId) {
		if(importIncreamentConfig != null)
			importIncreamentConfig.setStatusTableId(statusTableId);
	}
//	public String getLastValueStoreTableName() {
//		return importIncreamentConfig != null?importIncreamentConfig.getLastValueStoreTableName():null;
//	}
//
//	public String getLastValueStorePath() {
//		return importIncreamentConfig != null?importIncreamentConfig.getLastValueStorePath():null;
//	}
//
//	public String getDateLastValueColumn() {
//		return importIncreamentConfig != null?importIncreamentConfig.getDateLastValueColumn():null;
//	}
//	public String getNumberLastValueColumn() {
//		return importIncreamentConfig != null?importIncreamentConfig.getNumberLastValueColumn():null;
//	}
//
//	public Integer getLastValueType() {
//		return importIncreamentConfig != null?importIncreamentConfig.getLastValueType():null;
//	}
//	public void setImportIncreamentConfig(ImportIncreamentConfig importIncreamentConfig) {
//		this.importIncreamentConfig = importIncreamentConfig;
//	}
//
//	public boolean isFromFirst() {
//		return importIncreamentConfig != null?importIncreamentConfig.isFromFirst():false;
//	}
//
//	public Long getConfigLastValue() {
//		return importIncreamentConfig != null?importIncreamentConfig.getLastValue():null;
//	}




	public Integer getScheduleBatchSize() {
		return scheduleBatchSize;
	}

	public void setScheduleBatchSize(Integer scheduleBatchSize) {
		this.scheduleBatchSize = scheduleBatchSize;
	}


	public boolean isFromFirst() {
		return importIncreamentConfig != null && importIncreamentConfig.isFromFirst();
	}

	public String getLastValueStoreTableName() {
		return importIncreamentConfig != null?importIncreamentConfig.getLastValueStoreTableName():null;
	}

	public String getLastValueStorePath() {
		return importIncreamentConfig != null?importIncreamentConfig.getLastValueStorePath():null;
	}

	public String getLastValueColumn() {
		return importIncreamentConfig != null?importIncreamentConfig.getLastValueColumn():null;
	}


	public Integer getLastValueType() {
		return importIncreamentConfig != null?importIncreamentConfig.getLastValueType():null;
	}


	public Object getConfigLastValue() {
		return importIncreamentConfig != null?importIncreamentConfig.getLastValue():null;
	}
	public Integer getStatusTableId() {
		return this.importIncreamentConfig != null ?importIncreamentConfig.getStatusTableId():null;
	}

	public void stop(){
//		if(dataTranPlugin != null) {
//			dataTranPlugin.stop();
//		}
	}

	public List<CallInterceptor> getCallInterceptors() {
		return callInterceptors;
	}

	public void setCallInterceptors(List<CallInterceptor> callInterceptors) {
		this.callInterceptors = callInterceptors;
	}

	public boolean isPrintTaskLog() {
		return printTaskLog;
	}

	public void setPrintTaskLog(boolean printTaskLog) {
		this.printTaskLog = printTaskLog;
	}



//	public IndexPattern getIndexPattern() {
//		return indexPattern;
//	}
//
//	public void setIndexPattern(IndexPattern indexPattern) {
//		this.indexPattern = indexPattern;
//	}

//	public String buildIndexName(){
//		if(this.indexPattern == null){
//			return this.index;
//		}
//		SimpleDateFormat dateFormat = new SimpleDateFormat(this.indexPattern.getDateFormat());
//		String date = dateFormat.format(new Date());
//		StringBuilder builder = new StringBuilder();
//		builder.append(indexPattern.getIndexPrefix()).append(date);
//		if(indexPattern.getIndexEnd() != null){
//			builder.append(indexPattern.getIndexEnd());
//		}
//		return builder.toString();
//	}




	public WrapedExportResultHandler getExportResultHandler() {
		return exportResultHandler;
	}

	public void setExportResultHandler(WrapedExportResultHandler exportResultHandler) {
		this.exportResultHandler = exportResultHandler;
	}
	public int getMaxRetry(){
		if(this.exportResultHandler != null)
			return this.exportResultHandler.getMaxRetry();
		return -1;
	}

	public DBConfig getStatusDbConfig() {
		return statusDbConfig;
	}

	public void setStatusDbConfig(DBConfig statusDbConfig) {
		this.statusDbConfig = statusDbConfig;
		if(statusDbConfig != null && SimpleStringUtil.isNotEmpty(statusDbConfig.getDbName()) )
			this.dbConfigMap.put(statusDbConfig.getDbName(),statusDbConfig);
	}
	public static GeoIPUtil getGeoIPUtil(Map<String, Object> geoipConfig){
		return GeoIPUtil.getGeoIPUtil(geoipConfig);
	}

	public void setExternalTimer(boolean externalTimer) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setExternalTimer(externalTimer);
	}

	public List<DBConfig> getConfigs() {
		return configs;
	}

	public void setConfigs(List<DBConfig> configs) {
		this.configs = configs;
		for(int i = 0; configs != null && i < configs.size(); i ++){
			DBConfig dbConfig = configs.get(i);
			if(SimpleStringUtil.isNotEmpty(dbConfig.getDbName()))
				this.dbConfigMap.put(dbConfig.getDbName(),dbConfig);
		}
	}

//	public String getRefreshOption() {
//		return refreshOption;
//	}
//
//	public void setRefreshOption(String refreshOption) {
//		this.refreshOption = refreshOption;
//	}

	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	public boolean isContinueOnError() {
		return continueOnError;
	}

	public void setContinueOnError(boolean continueOnError) {
		this.continueOnError = continueOnError;
	}

	public void setImportIncreamentConfig(ImportIncreamentConfig importIncreamentConfig) {
		this.importIncreamentConfig = importIncreamentConfig;
	}

	public boolean isExternalTimer() {
		if(this.scheduleConfig != null){
			return scheduleConfig.isExternalTimer();
		}
		return false;
	}

	/**
	 * 回填lastValueType
	 * @param lastValueType
	 */
	public void setLastValueType(int lastValueType) {
		importIncreamentConfig.setLastValueType(lastValueType);
	}

	/**
	 * lastValueDateformat
	 * @param lastValueDateformat
	 */
	public void setLastValueDateformat(String lastValueDateformat) {
		importIncreamentConfig.setLastValueDateformat(lastValueDateformat);
	}

	public String getLastValueDateformat(){
		return this.importIncreamentConfig.getLastValueDateformat();
	}

	public boolean isLastValueDateType() {
		return this.getLastValueType() != null && this.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE;
	}

	public boolean isSortLastValue() {
		return sortLastValue;
	}
	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 * @return
	 */
	public long getFlushInterval(){
		return flushInterval;
	}

	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 * @param sortLastValue
	 */
	public void setSortLastValue(boolean sortLastValue) {
		this.sortLastValue = sortLastValue;
	}

	public void setFlushInterval(long flushInterval) {
		this.flushInterval = flushInterval;
	}

	public int getTranDataBufferQueue() {
		return tranDataBufferQueue;
	}

//
//	public Object getEsDetectNoop() {
//		return esDetectNoop;
//	}
//
//	public void setEsDetectNoop(Object esDetectNoop) {
//		this.esDetectNoop = esDetectNoop;
//	}
//	public String getTimeout() {
//		return timeout;
//	}
//
//	public void setTimeout(String timeout) {
//		this.timeout = timeout;
//	}
//
//	public String getMasterTimeout() {
//		return masterTimeout;
//	}
//
//	public void setMasterTimeout(String masterTimeout) {
//		this.masterTimeout = masterTimeout;
//	}
//
//	public Integer getWaitForActiveShards() {
//		return waitForActiveShards;
//	}
//
//	public void setWaitForActiveShards(Integer waitForActiveShards) {
//		this.waitForActiveShards = waitForActiveShards;
//	}
//
//	public List<String> getSourceUpdateExcludes() {
//		return sourceUpdateExcludes;
//	}
//
//	public List<String> getSourceUpdateIncludes() {
//		return sourceUpdateIncludes;
//	}
//
//	public void setSourceUpdateExcludes(List<String> sourceUpdateExcludes) {
//		this.sourceUpdateExcludes = sourceUpdateExcludes;
//	}
//
//	public void setSourceUpdateIncludes(List<String> sourceUpdateIncludes) {
//		this.sourceUpdateIncludes = sourceUpdateIncludes;
//	}



	public Map<String, Object> getGeoipConfig() {
		return geoipConfig;
	}

	public void setGeoipConfig(Map<String, Object> geoipConfig) {
		this.geoipConfig = geoipConfig;
	}

	public boolean isAsynFlushStatus() {
		return asynFlushStatus;
	}

	public void setAsynFlushStatus(boolean asynFlushStatus) {
		this.asynFlushStatus = asynFlushStatus;
	}
	public String getSplitFieldName() {
		return splitFieldName;
	}

	public void setSplitFieldName(String splitFieldName) {
		this.splitFieldName = splitFieldName;
	}

	public SplitHandler getSplitHandler() {
		return splitHandler;
	}

	public void setSplitHandler(SplitHandler splitHandler) {
		this.splitHandler = splitHandler;
	}


	public DBConfig getDefaultDBConfig() {
		return defaultDBConfig;
	}

	public void setDefaultDBConfig(DBConfig defaultDBConfig) {
		this.defaultDBConfig = defaultDBConfig;
	}

	public Long getDeyLay() {
		return deyLay;
	}

	public void setDeyLay(Long deyLay) {
		this.deyLay = deyLay;
	}

	public Date getScheduleEndDate() {
		return scheduleEndDate;
	}

	public void setScheduleEndDate(Date scheduleEndDate) {
		this.scheduleEndDate = scheduleEndDate;
	}

	public Date getScheduleDate() {
		return scheduleDate;
	}

	public void setScheduleDate(Date scheduleDate) {
		this.scheduleDate = scheduleDate;
	}

	public ImportEndAction getImportEndAction() {
		return importEndAction;
	}

	public void setImportEndAction(ImportEndAction importEndAction) {
		this.importEndAction = importEndAction;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobId() {
		return jobId;
	}
}
