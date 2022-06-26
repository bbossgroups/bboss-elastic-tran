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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.spi.DefaultApplicationContext;
import org.frameworkset.spi.assemble.GetProperties;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:31
 * @author biaoping.yin
 * @version 1.0
 */
public class ImportBuilder {
	protected InputConfig inputConfig;
	protected OutputConfig outputConfig;

	private Map params;
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	private DBConfig statusDbConfig ;
	private String statusDbname;
	private String statusTableDML;
	private Integer fetchSize = 5000;
	private String sourceDbname;

	private Boolean enableDBTransaction;

	public ImportBuilder setInputConfig(InputConfig inputConfig) {
		this.inputConfig = inputConfig;
		return this;
	}

	public ImportBuilder setOutputConfig(OutputConfig outputConfig) {
		this.outputConfig = outputConfig;
		return this;
	}



	public String getSplitFieldName() {
		return splitFieldName;
	}
	public ImportBuilder addParam(String key, Object value){
		if(params == null)
			params = new HashMap();
		this.params.put(key,value);
		return this;
	}


	public ImportBuilder setSplitFieldName(String splitFieldName) {
		this.splitFieldName = splitFieldName;
		return this;
	}

	private String splitFieldName;

	private transient SplitHandler splitHandler;
	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 */
	private long flushInterval = 8000l;
	private boolean ignoreNullValueField;
	private Map<String, Object> geoipConfig;

	private boolean sortLastValue = true;
	private boolean useBatchContextIndexName = false;
//	public abstract InputPlugin buildInputDataTranPlugin(ImportContext importContext);
//	public abstract OutputPlugin buildOutputDataTranPlugin(ImportContext importContext);
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = importContext.buildDataTranPlugin();
		return dataTranPlugin;
	}
	/**
	 * 是否不需要返回响应，不需要的情况下，可以设置为true，
	 * 提升性能，如果debugResponse设置为true，那么强制返回并打印响应到日志文件中
	 */
	private boolean discardBulkResponse = true;
	/**是否调试bulk响应日志，true启用，false 不启用，*/
	private boolean debugResponse;
	private ScheduleConfig scheduleConfig;
	protected ImportIncreamentConfig importIncreamentConfig;
	public boolean isExternalTimer() {
		return externalTimer;
	}

	protected DataStream createDataStream(){
		return new DataStream();
	}
	public Map<String, Object> getGeoipConfig() {
		return geoipConfig;
	}

	public void setStatusTableId(Integer statusTableId) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		importIncreamentConfig.setStatusTableId(statusTableId);
	}
	/**
	 * 采用外部定时任务引擎执行定时任务控制变量：
	 * false 内部引擎，默认值
	 * true 外部引擎
	 */
	private boolean externalTimer;
	/**
	 * 打印任务日志
	 */
	private boolean printTaskLog = false;

	/**
	 * 定时任务拦截器
	 */
	private transient List<CallInterceptor> callInterceptors;
	private transient List<String> callInterceptorClasses;

	private String applicationPropertiesFile;
//	private boolean freezen;
//	private boolean statusFreezen;
	private List<DBConfig> configs;





	/**批量获取数据大小*/
	private int batchSize = 1000;



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
	/**
	 * 并行队列大小，默认1000
	 */
	private int queue = 1000;
	/**
	 * 是否同步等待批处理作业结束，true 等待 false 不等待
	 */
	private boolean asyn;
	/**
	 * 并行执行过程中出现异常终端后续作业处理，已经创建的作业会执行完毕
	 */
	private boolean continueOnError;
	/**
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 *  增量查询截止时间值为：System.currenttime - increamentEndOffset
	 *  对应的变量名称：getLastValueVarName()+"__endTime"
	 * @return
	 */
	private Integer increamentEndOffset;
	/**
	 * 是否采用异步模式存储增量状态，默认true
	 * true 异步模式
	 * false 同步模式
	 */
	private boolean asynFlushStatus = true;
	/**
	 * 单位：毫秒
	 */
	private long asynFlushStatusInterval = 10000;
	/**
	 * 串行多条记录处理时，把所有记录一次性加载导入，还是单条逐条导入 true 一次性加载导入， false 逐条加载导入
	 */
	private boolean serialAllData;

	public long getLogsendTaskMetric() {
		return logsendTaskMetric;
	}

	public ImportBuilder setLogsendTaskMetric(long logsendTaskMetric) {
		this.logsendTaskMetric = logsendTaskMetric;
		return this;
	}

	protected long logsendTaskMetric = 10000l;
	public long getAsynResultPollTimeOut() {
		return asynResultPollTimeOut;
	}

	public ImportBuilder setAsynResultPollTimeOut(long asynResultPollTimeOut) {
		this.asynResultPollTimeOut = asynResultPollTimeOut;
		return this;
	}
	/**
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  对应的变量名称：getLastValueVarName()+"__endTime"
	 * @return
	 */
	public ImportBuilder setIncreamentEndOffset(Integer increamentEndOffset) {
		this.increamentEndOffset = increamentEndOffset;
		return this;
	}

	public ImportBuilder setAsynFlushStatus(boolean asynFlushStatus) {
		this.asynFlushStatus = asynFlushStatus;
		return this;
	}

	private long asynResultPollTimeOut = 1000;
	public Boolean getUseLowcase() {
		return useLowcase;
	}

	public ImportBuilder setUseLowcase(Boolean useLowcase) {
		this.useLowcase = useLowcase;
		return this;
	}

	private Boolean useLowcase;
	/**抽取数据的sql语句*/
//	private String refreshOption;
	private Integer scheduleBatchSize ;

//	/**抽取数据的sql语句*/
//	private String esIdField;
//	/**抽取数据的sql语句*/
//	private String esParentIdField;
//	/**抽取数据的sql语句*/
//	private String esParentIdValue;
//	/**抽取数据的sql语句*/
//	private String routingField;
//	/**抽取数据的sql语句*/
//	private String routingValue;
//	/**抽取数据的sql语句*/
//	private Boolean esDocAsUpsert;
//	/**抽取数据的sql语句*/
//	private Integer esRetryOnConflict;
//	/**抽取数据的sql语句*/
//	private Boolean esReturnSource;
//	/**抽取数据的sql语句*/
//	private String esVersionField;
//	/**抽取数据的sql语句*/
//	private Object esVersionValue;
//	/**抽取数据的sql语句*/
//	private String esVersionType;
	/**抽取数据的sql语句*/
	private Boolean useJavaName;

	protected transient ExportResultHandler exportResultHandler;
	private String exportResultHandlerClass;

	public String getExportResultHandlerClass() {
		return exportResultHandlerClass;
	}

	private Map<String,Object> customDBConfigs = new HashMap<String, Object>();
	public static final String DEFAULT_CONFIG_FILE = "application.properties";


	@JsonIgnore
	public ExportResultHandler getExportResultHandler() {
		return exportResultHandler;
	}

	public ImportBuilder setExportResultHandler(ExportResultHandler exportResultHandler) {
		this.exportResultHandler = exportResultHandler;
		if(exportResultHandler != null){
			exportResultHandlerClass = exportResultHandler.getClass().getName();
		}
		return this;
	}
	private String geoipDatabase;
	private String geoipAsnDatabase;
	private Object geoipIspConverter;
	private String geoip2regionDatabase;
	private Integer geoipCachesize;
	private String geoipTaobaoServiceURL;
	protected void buildGeoipConfig(){
		if(geoipDatabase != null){
			if(this.geoipConfig == null){
				geoipConfig = new HashMap<String, Object>();
			}
			geoipConfig.put("ip.database",
					geoipDatabase);
			if(geoipAsnDatabase != null)
				geoipConfig.put("ip.asnDatabase",
					geoipAsnDatabase);

			if(geoipIspConverter != null)
				geoipConfig.put("ip.ispConverter",
						geoipIspConverter);
			if(geoip2regionDatabase != null)
				geoipConfig.put("ip.ip2regionDatabase",
						geoip2regionDatabase);

			if(geoipCachesize != null)
				geoipConfig.put("ip.cachesize",
						geoipCachesize+"");
			else{
				geoipConfig.put("ip.cachesize",
						"10000");
			}
			if(geoipTaobaoServiceURL != null)
				geoipConfig.put("ip.serviceUrl",
						geoipTaobaoServiceURL);
		}
	}



	protected void buildStatusDBConfig(){

			if(statusDbname == null) {
				GetProperties propertiesContainer = null;
				String prefix = "config.";

				propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml",false);
				String dbName = propertiesContainer.getExternalProperty(prefix + "db.name");
				if (dbName == null || dbName.equals(""))
					return;

				statusDbConfig = new DBConfig();
				_buildDBConfig(propertiesContainer, dbName, statusDbConfig, "config.");
			}
			else{
				statusDbConfig = new DBConfig();
				statusDbConfig.setDbName(statusDbname);
				if(statusTableDML != null && !statusTableDML.equals("")){
					statusDbConfig.setStatusTableDML(statusTableDML);
				}
			}

	}
	private DBConfig defaultDBConfig ;
	protected void buildDBConfig(){

		GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml",false);
		String dbName  = propertiesContainer.getExternalProperty("db.name");
		if(dbName == null || dbName.equals("")) {
			return;
		}


		defaultDBConfig = new DBConfig();
		_buildDBConfig(propertiesContainer,dbName,defaultDBConfig, "");

	}

	public DBConfig getDefaultDBConfig() {
		return defaultDBConfig;
	}

	/**
	 * 在数据导入过程可能需要使用的其他数据名称，需要在配置文件中定义相关名称的db配置
	 */
	protected void buildOtherDBConfigs(){

		GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml",false);
		String thirdDatasources = propertiesContainer.getExternalProperty("thirdDatasources");
		if(thirdDatasources == null || thirdDatasources.equals(""))
			return;
		String[] names = thirdDatasources.split(",");
		List<DBConfig> dbConfigs = new ArrayList<DBConfig>();
		for(int i = 0; i < names.length; i ++ ) {
			String prefix = names[i].trim();
			if(prefix.equals(""))
				continue;


			DBConfig dbConfig = new DBConfig();
			_buildDBConfig(propertiesContainer, prefix, dbConfig, prefix+".");
			dbConfigs.add(dbConfig);
		}
		this.configs = dbConfigs;

	}



	protected void _buildDBConfig(GetProperties propertiesContainer, String dbName,DBConfig dbConfig,String prefix){


		if(!customDBConfigs.containsKey(prefix+"db.name")) {
			dbConfig.setDbName(dbName);
		}
		if(!customDBConfigs.containsKey(prefix+"db.user")) {
			String dbUser  = propertiesContainer.getExternalProperty(prefix+"db.user");
			dbConfig.setDbUser(dbUser);
		}
		if(!customDBConfigs.containsKey(prefix+"db.password")) {
			String dbPassword  = propertiesContainer.getExternalProperty(prefix+"db.password");
			dbConfig.setDbPassword(dbPassword);
		}
		if(!customDBConfigs.containsKey(prefix+"db.driver")) {
			String dbDriver  = propertiesContainer.getExternalProperty(prefix+"db.driver");
			dbConfig.setDbDriver(dbDriver);
		}

		if(!customDBConfigs.containsKey(prefix+"db.enableDBTransaction")) {
			boolean enableDBTransaction = propertiesContainer.getExternalBooleanProperty(prefix+"db.enableDBTransaction",false);
			dbConfig.setEnableDBTransaction(enableDBTransaction);
		}
		if(!customDBConfigs.containsKey(prefix+"db.url")) {
			String dbUrl  = propertiesContainer.getExternalProperty(prefix+"db.url");
			dbConfig.setDbUrl(dbUrl);
		}
		if(!customDBConfigs.containsKey(prefix+"db.usePool")) {
			String _usePool = propertiesContainer.getExternalProperty(prefix+"db.usePool");
			if(_usePool != null && !_usePool.equals("")) {
				boolean usePool = Boolean.parseBoolean(_usePool);
				dbConfig.setUsePool(usePool);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.validateSQL")) {
			String validateSQL  = propertiesContainer.getExternalProperty(prefix+"db.validateSQL");
			dbConfig.setValidateSQL(validateSQL);
		}

		if(!customDBConfigs.containsKey(prefix+"db.showsql")) {
			String _showSql = propertiesContainer.getExternalProperty(prefix+"db.showsql");
			if(_showSql != null && !_showSql.equals("")) {
				boolean showSql = Boolean.parseBoolean(_showSql);
				dbConfig.setShowSql(showSql);
			}
		}

		if(!customDBConfigs.containsKey(prefix+"db.jdbcFetchSize")) {
			String _jdbcFetchSize = propertiesContainer.getExternalProperty(prefix+"db.jdbcFetchSize");
			if(_jdbcFetchSize != null && !_jdbcFetchSize.equals("")) {
				int jdbcFetchSize = Integer.parseInt(_jdbcFetchSize);
				dbConfig.setJdbcFetchSize(jdbcFetchSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.initSize")) {
			String _initSize = propertiesContainer.getExternalProperty(prefix+"db.initSize");
			if(_initSize != null && !_initSize.equals("")) {
				int initSize = Integer.parseInt(_initSize);
				dbConfig.setInitSize(initSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.minIdleSize")) {
			String _minIdleSize = propertiesContainer.getExternalProperty(prefix+"db.minIdleSize");
			if(_minIdleSize != null && !_minIdleSize.equals("")) {
				int minIdleSize = Integer.parseInt(_minIdleSize);
				dbConfig.setMinIdleSize(minIdleSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.maxSize")) {
			String _maxSize = propertiesContainer.getExternalProperty(prefix+"db.maxSize");
			if(_maxSize != null && !_maxSize.equals("")) {
				int maxSize = Integer.parseInt(_maxSize);
				dbConfig.setMaxSize(maxSize);
			}
		}
		if(!customDBConfigs.containsKey(prefix+"db.statusTableDML")) {
			String statusTableDML  = propertiesContainer.getExternalProperty(prefix+"db.statusTableDML");
			dbConfig.setStatusTableDML(statusTableDML);
		}
		if(!customDBConfigs.containsKey(prefix+"db.dbAdaptor")) {
			String dbAdaptor  = propertiesContainer.getExternalProperty(prefix+"db.dbAdaptor");
			dbConfig.setDbAdaptor(dbAdaptor);
		}
		if(!customDBConfigs.containsKey(prefix+"db.dbtype")) {
			String dbtype  = propertiesContainer.getExternalProperty(prefix+"db.dbtype");
			dbConfig.setDbtype(dbtype);
		}
		if(!customDBConfigs.containsKey(prefix+"db.columnLableUpperCase")) {
			String columnLableUpperCase  = propertiesContainer.getExternalProperty(prefix+"db.columnLableUpperCase");
			if(columnLableUpperCase != null){
				boolean _columnLableUpperCase = Boolean.parseBoolean(columnLableUpperCase);
				dbConfig.setColumnLableUpperCase(_columnLableUpperCase);
			}

		}
		if(!customDBConfigs.containsKey(prefix+"db.dbInfoEncryptClass")) {
			String dbInfoEncryptClass  = propertiesContainer.getExternalProperty(prefix+"db.dbInfoEncryptClass");
			dbConfig.setDbInfoEncryptClass(dbInfoEncryptClass);
		}

	}



	public String getApplicationPropertiesFile() {
		return applicationPropertiesFile;
	}

	public void setApplicationPropertiesFile(String applicationPropertiesFile) {
		this.applicationPropertiesFile = applicationPropertiesFile;
	}

	public boolean isParallel() {
		return parallel;
	}

	public ImportBuilder setParallel(boolean parallel) {
		this.parallel = parallel;
		return this;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public ImportBuilder setThreadCount(int threadCount) {
		this.threadCount = threadCount;
		return this;
	}

	public int getQueue() {
		return queue;
	}

	public ImportBuilder setQueue(int queue) {
		this.queue = queue;
		return this;
	}

	public boolean isAsyn() {
		return asyn;
	}

	public ImportBuilder setAsyn(boolean asyn) {
		this.asyn = asyn;
		return this;
	}

	public ImportBuilder setContinueOnError(boolean continueOnError) {
		this.continueOnError = continueOnError;
		return this;
	}








	public ImportBuilder setPeriod(Long period) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setPeriod(period);
		return this;
	}


	public ImportBuilder setDeyLay(Long deyLay) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setDeyLay(deyLay);
		return this;
	}



	public ImportBuilder setScheduleDate(Date scheduleDate) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		else if(scheduleConfig instanceof TimerScheduleConfig)
			return this;
		this.scheduleConfig.setScheduleDate(scheduleDate);
		return this;
	}

	public ImportBuilder setScheduleSelf(){
		if(scheduleConfig == null){
			scheduleConfig = new TimerScheduleConfig();
		}
		return this;
	}
	public ImportBuilder setFixedRate(Boolean fixedRate) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		else if(scheduleConfig instanceof TimerScheduleConfig)
			return this;
		this.scheduleConfig.setFixedRate(fixedRate);
		return this;
	}

	/**
	 * 添加不扫码新文件的时间段
	 * timeRange必须是以下三种类型格式
	 * 11:30-12:30  每天在11:30和12:30之间运行
	 * 11:30-    每天11:30开始执行,到23:59结束
	 * -12:30    每天从00:00开始到12:30
	 * @param timeRange
	 * @return
	 */
	public ImportBuilder addSkipScanNewFileTimeRange(String timeRange){
		if(scheduleConfig == null){
			scheduleConfig = new TimerScheduleConfig();
		}
		else if(scheduleConfig instanceof  TimerScheduleConfig) {
			((TimerScheduleConfig) scheduleConfig).addSkipScanNewFileTimeRange(timeRange);
		}
		return this;
	}

	/**
	 * 添加扫码新文件的时间段，每天扫描新文件时间段，优先级高于不扫码时间段，先计算是否在扫描时间段，如果是则扫描，不是则不扫码
	 * timeRange必须是以下三种类型格式
	 * 11:30-12:30  每天在11:30和12:30之间运行
	 * 11:30-    每天11:30开始执行,到23:59结束
	 * -12:30    每天从00:00开始到12:30
	 * @param timeRange
	 * @return
	 */
	public ImportBuilder addScanNewFileTimeRange(String timeRange){
		if(scheduleConfig == null){
			scheduleConfig = new TimerScheduleConfig();
		}
		else if(scheduleConfig instanceof  TimerScheduleConfig) {
			((TimerScheduleConfig) scheduleConfig).addScanNewFileTimeRange(timeRange);
		}

		return this;
	}
	public ScheduleConfig getScheduleConfig() {
		return scheduleConfig;
	}

	public ImportIncreamentConfig getImportIncreamentConfig() {
		return importIncreamentConfig;
	}

	/**
	 * @See use setLastValueColumn(String numberLastValueColumn)
	 * @param dateLastValueColumn
	 * @return
	 */
	@Deprecated
	public ImportBuilder setDateLastValueColumn(String dateLastValueColumn) {
		return setLastValueColumn(dateLastValueColumn);
	}

	private boolean  lastValueColumnSetted = false;
	public ImportBuilder setLastValueColumn(String numberLastValueColumn) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueColumn(numberLastValueColumn);
		lastValueColumnSetted = true;
		return this;
	}

	/**
	 * @See use setLastValueColumn(String numberLastValueColumn)
	 * @param numberLastValueColumn
	 * @return
	 */
	@Deprecated
	public ImportBuilder setNumberLastValueColumn(String numberLastValueColumn) {
		return setLastValueColumn(numberLastValueColumn);
	}


	public ImportBuilder setLastValueStorePath(String lastValueStorePath) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueStorePath(lastValueStorePath);
		return this;
	}



	public ImportBuilder setLastValueStoreTableName(String lastValueStoreTableName) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueStoreTableName(lastValueStoreTableName);
		return this;
	}

	public ImportBuilder setFromFirst(boolean fromFirst) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setFromFirst(fromFirst);
		return this;
	}

	public ImportBuilder setLastValue(Object lastValue) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValue(lastValue);
		return this;
	}

	public ImportBuilder setLastValueType(int lastValueType) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueType(lastValueType);
		return this;
	}

	/**
	 * 设置增量字段日期格式，默认采用UTC标准日期格式：yyyy-MM-ddTHH:mm:ss.SSSZ
	 * @param lastValueDateformat
	 * @return
	 */
	public ImportBuilder setLastValueDateformat(String lastValueDateformat) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueDateformat(lastValueDateformat);
		return this;
	}



	public Integer getScheduleBatchSize() {
		return scheduleBatchSize;
	}

	public ImportBuilder setScheduleBatchSize(Integer scheduleBatchSize) {
		this.scheduleBatchSize = scheduleBatchSize;
		return this;
	}
	public ImportBuilder addCallInterceptor(CallInterceptor interceptor){
		if(this.callInterceptors == null){
			this.callInterceptors = new ArrayList<CallInterceptor>();
			this.callInterceptorClasses = new ArrayList<String>();
		}
		this.callInterceptors.add(interceptor);
		callInterceptorClasses.add(interceptor.getClass().getName());
		return this;
	}

	public boolean isPrintTaskLog() {
		return printTaskLog;
	}

	public ImportBuilder setPrintTaskLog(boolean printTaskLog) {
		this.printTaskLog = printTaskLog;
		return this;
	}

	private String configString;
	public String toString(){
		if(configString != null)
			return configString;
		try {
			StringBuilder ret = new StringBuilder();
			ret.append(SimpleStringUtil.object2json(this));
			if(splitHandler != null)
				ret.append(",splitHandler="+splitHandler.getClass().getCanonicalName());
			else
				ret.append(",splitHandler=null");
//			if(esIdGenerator != null)
//				ret.append(",esIdGenerator="+esIdGenerator.getClass().getCanonicalName());
//			else
//				ret.append(",esIdGenerator=null");
			if(dataRefactor != null)
				ret.append(",dataRefactor="+dataRefactor.getClass().getCanonicalName());
			else
				ret.append(",dataRefactor=null");

			if(exportResultHandler != null)
				ret.append(",exportResultHandler="+exportResultHandler.getClass().getCanonicalName());
			else
				ret.append(",exportResultHandler=null");

			if(callInterceptorClasses != null)
				ret.append(",callInterceptorClasses="+callInterceptorClasses);
			else
				ret.append(",callInterceptorClasses=null");


			return configString = ret.toString();
		}
		catch (Exception e){
			e.printStackTrace();
			configString = "";
			return configString;
		}
	}

	public List<DBConfig> getConfigs() {
		return configs;
	}

	/**
	 * 补充额外的字段和值
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public static FieldMeta addFieldValue(List<FieldMeta> fieldValues,String fieldName,Object value){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setTargetFieldName(fieldName);
		fieldMeta.setValue(value);
		fieldValues.add(fieldMeta);
		return fieldMeta;
	}


	public static FieldMeta addFieldValue(List<FieldMeta> fieldValues,String fieldName,String dateFormat,Object value,String locale,String timeZone){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setTargetFieldName(fieldName);
		fieldMeta.setValue(value);
		fieldMeta.setDateFormateMeta(buildDateFormateMeta( dateFormat,  locale,  timeZone));
		fieldValues.add(fieldMeta);
		return fieldMeta;

	}


	public ImportBuilder setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	public ImportBuilder setLocale(String locale) {
		this.locale = locale;
		return this;
	}

	public ImportBuilder setDateFormat(String dateFormat) {
		this.dateFormat = dateFormat;
		return this;
	}
	public String getLocale() {
		return locale;
	}

	public String getTimeZone() {
		return timeZone;
	}
	/**抽取数据的sql语句*/
	private String dateFormat;
	/**抽取数据的sql语句*/
	private String locale;
	/**抽取数据的sql语句*/
	private String timeZone;
	private final Map<String,FieldMeta> fieldMetaMap = new HashMap<String,FieldMeta>();
	private final List<FieldMeta> fieldValues = new ArrayList<FieldMeta>();

	protected Map<String,FieldMeta> valuesIdxByName = new LinkedHashMap<>();
	private transient DataRefactor dataRefactor;
	public DateFormateMeta buildDateFormateMeta(String dateFormat){
		return dateFormat == null?null:DateFormateMeta.buildDateFormateMeta(dateFormat,locale,timeZone);
	}

	public static DateFormateMeta buildDateFormateMeta(String dateFormat,String locale,String timeZone){
		return dateFormat == null?null:DateFormateMeta.buildDateFormateMeta(dateFormat,locale,timeZone);
	}

	private FieldMeta buildFieldMeta(String sourceFieldName,String targetFieldName ,String dateFormat){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setSourceFieldName(sourceFieldName);
		fieldMeta.setTargetFieldName(targetFieldName);
		fieldMeta.setIgnore(false);
		fieldMeta.setDateFormateMeta(dateFormat == null?null:DateFormateMeta.buildDateFormateMeta(dateFormat,locale,  timeZone));
		return fieldMeta;
	}

	private static FieldMeta buildIgnoreFieldMeta(String sourceFieldName){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setSourceFieldName(sourceFieldName);

		fieldMeta.setIgnore(true);
		return fieldMeta;
	}
	private FieldMeta buildFieldMeta(String sourceFieldName,String targetFieldName ,String dateFormat,String locale,String timeZone){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setSourceFieldName(sourceFieldName);
		fieldMeta.setTargetFieldName(targetFieldName);
		fieldMeta.setIgnore(false);
		fieldMeta.setDateFormateMeta(dateFormat == null?null:DateFormateMeta.buildDateFormateMeta(dateFormat,locale,timeZone));
		return fieldMeta;
	}
	public ImportBuilder addFieldMapping(String sourceFieldName, String targetFieldName){
		this.fieldMetaMap.put(sourceFieldName,buildFieldMeta(  sourceFieldName,  targetFieldName,null ));
		return this;
	}

	public ImportBuilder addIgnoreFieldMapping(String sourceFieldName){
		addIgnoreFieldMapping(fieldMetaMap, sourceFieldName);
		return this;
	}

	public static void addIgnoreFieldMapping(Map<String,FieldMeta> fieldMetaMap, String sourceColumnName){
		fieldMetaMap.put(sourceColumnName,buildIgnoreFieldMeta(  sourceColumnName));
	}

	public ImportBuilder addFieldMapping(String sourceColumnName, String targetFieldName, String dateFormat){
		this.fieldMetaMap.put(sourceColumnName,buildFieldMeta(  sourceColumnName,  targetFieldName ,dateFormat));
		return this;
	}

	public ImportBuilder addFieldMapping(String sourceColumnName, String targetFieldName, String dateFormat, String locale, String timeZone){
		this.fieldMetaMap.put(sourceColumnName,buildFieldMeta(  sourceColumnName,  targetFieldName ,dateFormat,  locale,  timeZone));
		return this;
	}


	/**
	 * 补充额外的字段和值
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public ImportBuilder addFieldValue(String fieldName, Object value){
		FieldMeta fieldMeta = addFieldValue(  fieldValues,  fieldName,  value);
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}

	/**
	 * 补充额外的字段和值
	 * @param fieldName
	 * @param dateFormat
	 * @param value
	 * @return
	 */
	public ImportBuilder addFieldValue(String fieldName, String dateFormat, Object value){
		FieldMeta fieldMeta = addFieldValue(  fieldValues,  fieldName,  dateFormat,  value,  locale,  timeZone);
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}
	public ImportBuilder addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone){
		FieldMeta fieldMeta = addFieldValue(  fieldValues,  fieldName,  dateFormat,  value,  locale,  timeZone);
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}



	public String getDataRefactorClass() {
		return dataRefactorClass;
	}

	private String dataRefactorClass;
	public ImportBuilder setDataRefactor(DataRefactor dataRefactor) {
		this.dataRefactor = dataRefactor;
		dataRefactorClass = dataRefactor.getClass().getName();
		return this;
	}

	public ImportBuilder setExternalTimer(boolean externalTimer) {
		this.externalTimer = externalTimer;
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setExternalTimer(externalTimer);
		return this;
	}





	public static void main(String[] args){
		System.out.println("meta:_id".substring(5));//""
	}
	protected void buildImportConfig(BaseImportConfig baseImportConfig){
		baseImportConfig.setUseJavaName(false);
		baseImportConfig.setParams(this.params);
		baseImportConfig.setLastValueColumnSetted(this.lastValueColumnSetted);
//		if(getTargetElasticsearch() != null && !getTargetElasticsearch().equals(""))
//			baseImportConfig.setTargetElasticsearch(this.getTargetElasticsearch());
//		if(getSourceElasticsearch() != null && !getSourceElasticsearch().equals(""))
//			baseImportConfig.setSourceElasticsearch(this.getSourceElasticsearch());
//		if(this.esConfig != null){
//			baseImportConfig.setEsConfig(esConfig);
//		}
		if(geoipConfig != null && geoipConfig.size() > 0){
			baseImportConfig.setGeoipConfig(geoipConfig);
		}
		baseImportConfig.setDateFormat(dateFormat);
		baseImportConfig.setLocale(locale);
		baseImportConfig.setTimeZone(this.timeZone);

		baseImportConfig.setFetchSize(this.fetchSize);

//		baseImportConfig.setClientOptions(clientOptions);
//		baseImportConfig.setRoutingField(this.routingField);
		baseImportConfig.setUseJavaName(this.useJavaName);
		baseImportConfig.setFieldMetaMap(this.fieldMetaMap);
		baseImportConfig.setFieldValues(fieldValues);
		baseImportConfig.setValuesIdxByName(valuesIdxByName);
		baseImportConfig.setDataRefactor(this.dataRefactor);
		baseImportConfig.setSortLastValue(this.sortLastValue);
//		baseImportConfig.setDbConfig(dbConfig);
		baseImportConfig.setStatusDbConfig(statusDbConfig);
//		baseImportConfig.setTargetDbname(targetDbname);
//		baseImportConfig.setSourceDbname(sourceDbname);
//		baseImportConfig.setEnableDBTransaction(enableDBTransaction);
//		baseImportConfig.setJdbcFetchsize(jdbcFetchsize);
		baseImportConfig.setConfigs(this.configs);
		baseImportConfig.setBatchSize(this.batchSize);
		baseImportConfig.setDefaultDBConfig(defaultDBConfig);


		baseImportConfig.setApplicationPropertiesFile(this.applicationPropertiesFile);
		baseImportConfig.setParallel(this.parallel);
		baseImportConfig.setThreadCount(this.threadCount);
		baseImportConfig.setQueue(this.queue);
		baseImportConfig.setAsyn(this.asyn);
		baseImportConfig.setContinueOnError(this.continueOnError);
		baseImportConfig.setAsynResultPollTimeOut(this.asynResultPollTimeOut);
//		/**
//		 * 是否不需要返回响应，不需要的情况下，可以设置为true，
//		 * 提升性能，如果debugResponse设置为true，那么强制返回并打印响应到日志文件中
//		 */
//		baseImportConfig.setDiscardBulkResponse(this.discardBulkResponse);
//		/**是否调试bulk响应日志，true启用，false 不启用，*/
//		baseImportConfig.setDebugResponse(this.debugResponse);
		baseImportConfig.setScheduleConfig(this.scheduleConfig);//定时任务配置
		baseImportConfig.setImportIncreamentConfig(this.importIncreamentConfig);//增量数据配置

		if(this.scheduleBatchSize != null)
			baseImportConfig.setScheduleBatchSize(this.scheduleBatchSize);
		else
			baseImportConfig.setScheduleBatchSize(this.batchSize);
		baseImportConfig.setCallInterceptors(this.callInterceptors);
		baseImportConfig.setUseLowcase(this.useLowcase);
		baseImportConfig.setPrintTaskLog(this.printTaskLog);
//		baseImportConfig.setEsIdGenerator(esIdGenerator);

//		baseImportConfig.setPagine(this.pagine);
		baseImportConfig.setTranDataBufferQueue(this.tranDataBufferQueue);
		baseImportConfig.setFlushInterval(this.flushInterval);
		baseImportConfig.setIgnoreNullValueField(this.ignoreNullValueField);
		baseImportConfig.setIncreamentEndOffset(this.increamentEndOffset);
		baseImportConfig.setAsynFlushStatus(this.asynFlushStatus);
		baseImportConfig.setAsynFlushStatusInterval(this.asynFlushStatusInterval);
		baseImportConfig.setSerialAllData(this.serialAllData);
		baseImportConfig.setLogsendTaskMetric(logsendTaskMetric);
		baseImportConfig.setSplitHandler(this.getSplitHandler());
		baseImportConfig.setSplitFieldName(getSplitFieldName());
//		baseImportConfig.setEsDetectNoop(this.esDetectNoop);

	}



	public ImportBuilder setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}








	public int getBatchSize() {
		return batchSize;
	}





	public Boolean getUseJavaName() {
		return useJavaName;
	}






	public ImportBuilder setUseJavaName(Boolean useJavaName) {
		this.useJavaName = useJavaName;
		return this;
	}



	public boolean isContinueOnError() {
		return continueOnError;
	}


	public Integer getFetchSize() {
		return fetchSize;
	}

	public ImportBuilder setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
		return this;
	}


	protected DataStream innerBuilder(){
		this.buildGeoipConfig();
		buildDBConfig();
		this.buildOtherDBConfigs();
		this.buildStatusDBConfig();


		try {
			if(logger.isInfoEnabled()) {
				logger.info("DB2DB Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		BaseImportContext importContext = new BaseImportContext();
		BaseImportConfig baseImportConfig = new BaseImportConfig() ;
		buildImportConfig(baseImportConfig);
		importContext.setBaseImportConfig(baseImportConfig);
		inputConfig.build(this);
		outputConfig.build(this);
		importContext.setInputConfig(inputConfig);
		importContext.setOutputConfig(outputConfig);
		if(this.exportResultHandler != null){

			baseImportConfig.setExportResultHandler(outputConfig.buildExportResultHandler( exportResultHandler));
		}
		importContext.afterBuild(this);

//		DBImportConfig db2DBImportConfig = new DBImportConfig();
//		super.buildImportConfig(db2DBImportConfig);

//		db2DBImportConfig.setTargetDBConfig(this.targetDBConfig);
//		super.buildDBImportConfig(db2DBImportConfig);

		DataStream dataStream = this.createDataStream();
//		ImportContext sourceImportContext = this.buildImportContext(inputConfig);
//		ImportContext targetImportContext = this.buildTargetImportContext(outputConfig);

//		dataStream.setImportConfig(db2DBImportConfig);
		dataStream.setImportContext(importContext);
//		dataStream.setTargetImportContext(this.buildTargetImportContext(db2DBImportConfig));
//		dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(importContext));
		return dataStream;
	}
	public static ImportBuilder newInstance(){
		return new ImportBuilder();
	}


	/**
	 * 创建持续运行的数据同步作业
	 * @return
	 */
	public DataStream builder(){
		return builder(false);
	}

	/**
	 * 创建数据同步作业
	 * enableSchdulePause为true时，创建具备暂停功能的数据同步作业，控制调度执行后将作业自动标记为暂停状态，等待下一个resumeShedule指令才继续允许作业调度执行，
	 * enableSchdulePause为false时，创建持续运行的数据同步作业
	 * @param enableSchdulePause
	 * @return
	 */
	public DataStream builder(boolean enableSchdulePause){
		ScheduleAssert scheduleAssert = null;
		if(enableSchdulePause){

			if(logger.isInfoEnabled())
				logger.info("Use AutopauseScheduleAssert.");
			scheduleAssert = new AutopauseScheduleAssert();
		}
		return builder(scheduleAssert);
	}
	/**
	 * 创建创建具备暂停功能的数据同步作业，设置调度暂停器ScheduleAssert，接口scheduleAssert.assertSchedule方法为true时，等待下一个resumeShedule指令才继续允许作业调度执行
	 * 可以直接通过scheduleAssert的pauseSchedule方法发出暂停调度指令，通过resumeSchedule发出继续调度指令
	 * 亦可以通过DataStream的pauseSchedule方法发出暂停调度指令，通过resumeSchedule发出继续调度指令
	 * @param scheduleAssert
	 * @return
	 */
	public DataStream builder(ScheduleAssert scheduleAssert){
		if(inputConfig == null){
			throw new DataImportException("InputConfig is null and must be set by ImportBuilder.");
		}
		if(outputConfig == null){
			throw new DataImportException("OutputConfig is null and must be set by ImportBuilder.");
		}
		DataStream dataStream = innerBuilder();
		if(scheduleAssert != null){
			if(scheduleAssert instanceof WrappedScheduleAssert)
				dataStream.setScheduleAssert(scheduleAssert);
			else{
				dataStream.setScheduleAssert(new WrappedScheduleAssert(scheduleAssert));
			}
		}
		return dataStream;
	}
	public ImportBuilder setTranDataBufferQueue(int tranDataBufferQueue) {
		this.tranDataBufferQueue = tranDataBufferQueue;
		return this;
	}

	/**
	 * 源数据批量预加载队列大小，需要用到的最大缓冲内存为：
	 *  tranDataBufferQueue * fetchSize * 单条记录mem大小
	 */
	private int tranDataBufferQueue = 10;



	public long getFlushInterval() {
		return flushInterval;
	}

	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 * @param flushInterval
	 * @return
	 */
	public ImportBuilder setFlushInterval(long flushInterval) {
		this.flushInterval = flushInterval;
		return this;
	}

	public boolean isIgnoreNullValueField() {
		return ignoreNullValueField;
	}

	public ImportBuilder setIgnoreNullValueField(boolean ignoreNullValueField) {
		this.ignoreNullValueField = ignoreNullValueField;
		return this;
	}




	public boolean isSortLastValue() {
		return sortLastValue;
	}

	public ImportBuilder setSortLastValue(boolean sortLastValue) {
		this.sortLastValue = sortLastValue;
		return this;
	}



	public boolean isUseBatchContextIndexName() {
		return useBatchContextIndexName;
	}

	public ImportBuilder setUseBatchContextIndexName(boolean useBatchContextIndexName) {
		this.useBatchContextIndexName = useBatchContextIndexName;
		return this;
	}

	public ImportBuilder setGeoipDatabase(String geoipDatabase) {
		this.geoipDatabase = geoipDatabase;
		return this;
	}

	public ImportBuilder setGeoipAsnDatabase(String geoipAsnDatabase) {
		this.geoipAsnDatabase = geoipAsnDatabase;
		return this;
	}

	public ImportBuilder setGeoip2regionDatabase(String geoip2regionDatabase) {
		this.geoip2regionDatabase = geoip2regionDatabase;
		return this;
	}

	public ImportBuilder setGeoipCachesize(int geoipCachesize) {
		this.geoipCachesize = geoipCachesize;
		return this;
	}

	public ImportBuilder setGeoipTaobaoServiceURL(String geoipTaobaoServiceURL) {
		this.geoipTaobaoServiceURL = geoipTaobaoServiceURL;
		return this;
	}

	public String getStatusDbname() {
		return statusDbname;
	}

	public ImportBuilder setStatusDbname(String statusDbname) {
		this.statusDbname = statusDbname;
		return this;
	}

	public String getStatusTableDML() {
		return statusTableDML;
	}

	public ImportBuilder setStatusTableDML(String statusTableDML) {
		this.statusTableDML = statusTableDML;
		return this;
	}

	public ImportBuilder setGeoipIspConverter(Object geoipIspConverter) {
		this.geoipIspConverter = geoipIspConverter;
		return this;
	}

	public long getAsynFlushStatusInterval() {
		return asynFlushStatusInterval;
	}

	public ImportBuilder setAsynFlushStatusInterval(long asynFlushStatusInterval) {
		this.asynFlushStatusInterval = asynFlushStatusInterval;
		return this;
	}
	@JsonIgnore
	public SplitHandler getSplitHandler() {
		return splitHandler;
	}

	public ImportBuilder setSplitHandler(SplitHandler splitHandler) {
		this.splitHandler = splitHandler;
		return this;
	}


	public String getSourceDbname() {
		return sourceDbname;
	}

	public ImportBuilder setSourceDbname(String sourceDbname) {
		this.sourceDbname = sourceDbname;
		return this;
	}
	public Boolean getEnableDBTransaction() {
		return enableDBTransaction;
	}

	public ImportBuilder setEnableDBTransaction(Boolean enableDBTransaction) {
		this.enableDBTransaction = enableDBTransaction;
		return this;
	}

	public boolean isSerialAllData() {
		return serialAllData;
	}

	/**
	 * 串行多条记录处理时，把所有记录一次性加载导入，还是单条逐条导入 true 一次性加载导入， false 逐条加载导入
	 * @param serialAllData
	 * @return
	 */
	public ImportBuilder setSerialAllData(boolean serialAllData) {
		this.serialAllData = serialAllData;
		return this;
	}
}