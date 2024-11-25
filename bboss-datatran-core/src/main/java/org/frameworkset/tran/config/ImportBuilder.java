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
import org.frameworkset.spi.DefaultApplicationContext;
import org.frameworkset.spi.assemble.GetProperties;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.InitJobContextCall;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.listener.AsynJobClosedListener;
import org.frameworkset.tran.listener.AsynJobClosedListenerImpl;
import org.frameworkset.tran.listener.JobClosedListener;
import org.frameworkset.tran.metrics.MetricsLogLevel;
import org.frameworkset.tran.metrics.MetricsLogReport;
import org.frameworkset.tran.metrics.job.Metrics;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputConfig;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.frameworkset.tran.DBConfig.*;

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
	protected ImportStartAction importStartAction;
	protected ImportEndAction importEndAction;
	private Map jobInputParams;

	private Map jobOutputParams;


    private JobClosedListener jobClosedListener;
    private int metricsLogLevel = MetricsLogLevel.INFO;

    private boolean numberTypeTimestamp;
    /**
     * 输入参数组，将输入参数和输入动态参数组装为一个参数组添加到参数组集合中，添加完成后重置输入参数和输入动态参数，为增加新的参数组做准备
     * 通过添加多个参数组，作业调度时，特定的输入插件可以利用参数组中的每组参数发起并发数据请求，比如httpinput插件
     */
    private List<JobInputParamGroup> jobInputParamGroups;
	private Map<String,DynamicParam> jobDynamicInputParams;
	private Map<String,DynamicParam> jobDynamicOutputParams;
	protected static Logger logger = LoggerFactory.getLogger(ImportBuilder.class);
	private DBConfig statusDbConfig ;
	private String statusDbname;
	private String statusTableDML;
    private String statusHistoryTableDML;
	private Integer fetchSize = 5000;
	private String jobName;
	private String jobId;
    /**
     * 设置增量状态ID生成策略，在设置jobId的情况下起作用
     * STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
     * STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用jobType+jobId+作业查询语句hashcode或者文件名称，作为增量id作为增量状态id
     * 默认值STATUSID_POLICY_JOBID_QUERYSTATEMENT 
     */
    private Integer statusIdPolicy ;


    private Boolean increamentImport;
	private List<ETLMetrics> metrics;
	/**
	 * 指标时间维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
	 */
	private String dataTimeField;



    private Integer timeWindowType;
	private boolean useDefaultMapData = false;



    private boolean flushMetricsOnScheduleTaskCompleted;


    private boolean cleanKeysWhenflushMetricsOnScheduleTaskCompleted;



    private boolean waitCompleteWhenflushMetricsOnScheduleTaskCompleted = true;

    public String getDataTimeField() {
		return dataTimeField;
	}
	/**
	 * 设置指标时间维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
	 * @param dataTimeField
	 * @return
	 */
	public ImportBuilder setDataTimeField(String dataTimeField) {
		this.dataTimeField = dataTimeField;
		return this;
	}
	public ImportBuilder setImportEndAction(ImportEndAction importEndAction) {
		this.importEndAction = importEndAction;
		return this;
	}

	public ImportEndAction getImportEndAction() {
		return importEndAction;
	}

    public InputConfig getInputConfig() {
        return inputConfig;
    }

    public OutputConfig getOutputConfig() {
        return outputConfig;
    }

    public List<ETLMetrics> getMetrics() {
		return metrics;
	}

	public ImportBuilder setInputConfig(InputConfig inputConfig) {
		this.inputConfig = inputConfig;
		return this;
	}

	public ImportBuilder setImportStartAction(ImportStartAction importStartAction) {
		this.importStartAction = importStartAction;
		return this;
	}

	public ImportStartAction getImportStartAction() {
		return importStartAction;
	}

	public ImportBuilder setOutputConfig(OutputConfig outputConfig) {
		this.outputConfig = outputConfig;
		return this;
	}



	public String getSplitFieldName() {
		return splitFieldName;
	}

	/**
	 * 添加作业提取数据输入插件条件
	 * use addJobInputParam(String key, Object value)
	 * @param key
	 * @param value
	 * @return
	 *
	 */
	@Deprecated
	public ImportBuilder addParam(String key, Object value){
		return addJobInputParam(key, value);
	}

	/**
	 *  添加作业提取数据输入插件条件
	 * @param key
	 * @param value
	 * @return
	 */
	public ImportBuilder addJobInputParam(String key, Object value){
		if(jobInputParams == null)
			jobInputParams = new LinkedHashMap();
		this.jobInputParams.put(key,value);
		return this;
	}

	/**
	 *  添加作业提取数据输入插件动态条件
	 * @param key
	 * @param dynamicParam
	 * @return
	 */
	public ImportBuilder addJobDynamicInputParam(String key, DynamicParam dynamicParam){
		if(jobDynamicInputParams == null)
			jobDynamicInputParams = new LinkedHashMap();
		this.jobDynamicInputParams.put(key,dynamicParam);
		return this;
	}
	/**
	 *  添加作业输出插件变量参数
	 * @param key
	 * @param value
	 * @return
	 */
	public ImportBuilder addJobOutputParam(String key, Object value){
		if(jobOutputParams == null)
			jobOutputParams = new LinkedHashMap();
		this.jobOutputParams.put(key,value);
		return this;
	}

	/**
	 *  添加作业输出插件动态变量参数
	 * @param key
	 * @param dynamicParam
	 * @return
	 */
	public ImportBuilder addJobDynamicOutputParam(String key, DynamicParam dynamicParam){
		if(jobDynamicOutputParams == null)
			jobDynamicOutputParams = new LinkedHashMap();
		this.jobDynamicOutputParams.put(key,dynamicParam);
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
	 * 任务开始时间
	 */
	private Date scheduleDate;
	/**
	 * 任务结束时间
	 */
	private Date scheduleEndDate;

	private Long deyLay;

	public Date getScheduleDate() {
		return scheduleDate;
	}

	public ImportBuilder setScheduleDate(Date scheduleDate) {
		this.scheduleDate = scheduleDate;
		return this;
	}

	public Long getDeyLay() {
		return deyLay;
	}

//	public void setDeyLay(Long deyLay) {
//		this.deyLay = deyLay;
//	}

	private ScheduleConfig scheduleConfig;
	protected ImportIncreamentConfig importIncreamentConfig = new ImportIncreamentConfig();;
    private String lastValueStorePassword;
	public boolean isExternalTimer() {
		return externalTimer;
	}

	protected DataStream createDataStream(){
		return new DataStream();
	}
	public Map<String, Object> getGeoipConfig() {
		return geoipConfig;
	}

	public void setStatusTableId(String statusTableId) {
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


    private MetricsLogReport metricsLogReport;

	/**
	 * 定时任务拦截器
	 */
	private transient List<CallInterceptor> callInterceptors;
	private transient List<String> callInterceptorClasses;

	private String applicationPropertiesFile;
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
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的偏移量
	 *  增量查询截止时间值为：System.currenttime - increamentEndOffset
	 *  对应的变量名称：getLastValueVarName()+"__endTime"
	 *  单位：秒
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
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的偏移量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  对应的变量名称：getLastValueVarName()+"__endTime"
	 *  单位：秒
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

	private long asynResultPollTimeOut = 1000L;
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

                if(statusHistoryTableDML != null && !statusHistoryTableDML.equals("")){
                    statusDbConfig.setStatusHistoryTableDML(statusHistoryTableDML);
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
        if(!customDBConfigs.containsKey(prefix+"db.statusHistoryTableDML")) {
            String statusHistoryTableDML  = propertiesContainer.getExternalProperty(prefix+"db.statusHistoryTableDML");
            dbConfig.setStatusHistoryTableDML(statusHistoryTableDML);
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

		if(!customDBConfigs.containsKey(prefix+db_removeAbandoned_key)) {
			String removeAbandoned  = propertiesContainer.getExternalProperty(prefix+db_removeAbandoned_key);
			dbConfig.setRemoveAbandoned(removeAbandoned != null && removeAbandoned.equals("true"));
		}


		if(!customDBConfigs.containsKey(prefix+db_connectionTimeout_key)) {
			String connectionTimeout  = propertiesContainer.getExternalProperty(prefix+db_connectionTimeout_key);
			if(connectionTimeout != null && !connectionTimeout.equals("")) {
				int _connectionTimeout = Integer.parseInt(connectionTimeout);
				dbConfig.setConnectionTimeout(_connectionTimeout);
			}
		}


		if(!customDBConfigs.containsKey(prefix+db_maxWait_key)) {
			String maxWait  = propertiesContainer.getExternalProperty(prefix+db_maxWait_key);
			if(maxWait != null && !maxWait.equals("")) {
				int _maxWait = Integer.parseInt(maxWait);
				dbConfig.setMaxWait(_maxWait);
			}
		}


		if(!customDBConfigs.containsKey(prefix+db_maxIdleTime_key)) {
			String maxIdleTime  = propertiesContainer.getExternalProperty(prefix+db_maxIdleTime_key);
			if(maxIdleTime != null && !maxIdleTime.equals("")) {
				int _maxIdleTime = Integer.parseInt(maxIdleTime);
				dbConfig.setMaxIdleTime(_maxIdleTime);
			}
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

    @Deprecated
    /**
     * 废弃参数，不起作用
     */
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
	/**
	 * 任务结束时间
	 */
	public ImportBuilder setScheduleEndDate(Date scheduleEndDate) {
//		if(scheduleConfig == null){
//			scheduleConfig = new ScheduleConfig();
//		}
//		this.scheduleConfig.setScheduleEndDate(scheduleEndDate);
		this.scheduleEndDate = scheduleEndDate;
		return this;
	}



	public ImportBuilder setDeyLay(Long deyLay) {
//		if(scheduleConfig == null){
//			scheduleConfig = new ScheduleConfig();
//		}
//		this.scheduleConfig.setDeyLay(deyLay);
		this.deyLay = deyLay;
		return this;
	}


//
//	public ImportBuilder setScheduleDate(Date scheduleDate) {
//		if(scheduleConfig == null){
//			scheduleConfig = new ScheduleConfig();
//		}
//		else if(scheduleConfig instanceof TimerScheduleConfig)
//			return this;
//		this.scheduleConfig.setScheduleDate(scheduleDate);
//		return this;
//	}

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

    /**
     * 设置增量起始值
     * @param lastValue
     * @return
     */
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

    public ImportBuilder addCallInterceptor(CallInterceptor interceptor,boolean firsted){
        if(this.callInterceptors == null){
            this.callInterceptors = new ArrayList<CallInterceptor>();
            this.callInterceptorClasses = new ArrayList<String>();
        }
        if(!firsted) {
            this.callInterceptors.add(interceptor);
            callInterceptorClasses.add(interceptor.getClass().getName());
        }
        else{
            this.callInterceptors.add(0,interceptor);
            callInterceptorClasses.add(0,interceptor.getClass().getName());
        }
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
			ret.append("parallel=").append(parallel);
			if(scheduleConfig != null)
				ret.append(",scheduleConfig=").append(scheduleConfig.toString());
			if(importIncreamentConfig != null)
				ret.append(",importIncreamentConfig=").append(importIncreamentConfig.toString());


			if(geoipConfig != null) {
				ret.append(",geoipConfig=").append(this.geoipConfig);
			}

			ret.append(",useJavaName=").append(this.useJavaName);
			ret.append(",useLowcase=").append(this.useLowcase);
			ret.append(",fetchSize=").append(this.fetchSize);
			ret.append(",batchSize=").append(this.batchSize);

			ret.append(",continueOnError=").append(this.continueOnError);
			ret.append(",flushInterval=").append(this.flushInterval);

			ret.append(",asynFlushStatus=").append(this.asynFlushStatus);
			ret.append(",asynFlushStatusInterval=").append(this.asynFlushStatusInterval);
			ret.append(",asyn=").append(this.asyn);

			ret.append(",statusDbname=").append(this.statusDbname);
			ret.append(",statusTableDML=").append(this.statusTableDML);

//			ret.append(SimpleStringUtil.object2json(this));
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
	private transient InitJobContextCall initJobContextCall;
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
		logger.info("meta:_id".substring(5));//""
	}

	private void initMetrics(BaseImportConfig baseImportConfig){
		if(metrics != null && metrics.size() > 0){
			if(outputConfig != null && outputConfig instanceof MetricsOutputConfig) {
				throw new DataImportException("指标输出插件不支持作业级别指标计算器，不能从importbuilder设置metrics！");
			}
//            迁移至DataTranPluginImpl.init---》importContext.initETLMetrics()
//			for(Metrics metrics: this.metrics){
//				metrics.init();
//			}
			baseImportConfig.setMetrics(this.metrics);
			baseImportConfig.setDataTimeField(dataTimeField);
			baseImportConfig.setUseDefaultMapData(this.useDefaultMapData);
            baseImportConfig.setTimeWindowType(this.timeWindowType);

		}
        if(this.flushMetricsOnScheduleTaskCompleted) {
            baseImportConfig.setFlushMetricsOnScheduleTaskCompleted(true);
            baseImportConfig.setCleanKeysWhenflushMetricsOnScheduleTaskCompleted(this.cleanKeysWhenflushMetricsOnScheduleTaskCompleted);
            baseImportConfig.setWaitCompleteWhenflushMetricsOnScheduleTaskCompleted(this.waitCompleteWhenflushMetricsOnScheduleTaskCompleted);

        }

	}
    private JobClosedListener handleJobClosedListener(){
        if(jobClosedListener != null && jobClosedListener instanceof AsynJobClosedListener){
            return new AsynJobClosedListenerImpl(jobClosedListener);
        }
        else {
            return jobClosedListener;
        }
    }

    public int getMetricsLogLevel() {
        return metricsLogLevel;
    }

    public ImportBuilder setMetricsLogLevel(int metricsLogLevel) {
        this.metricsLogLevel = metricsLogLevel;
        return this;
    }
	protected void buildImportConfig(BaseImportConfig baseImportConfig){
        baseImportConfig.setInitJobContextCall(this.initJobContextCall);
        baseImportConfig.setMetricsLogLevel(this.metricsLogLevel);
        baseImportConfig.setNumberTypeTimestamp(this.numberTypeTimestamp);
        baseImportConfig.setMetricsLogReport(this.metricsLogReport);
		baseImportConfig.setImportStartAction(importStartAction);
		baseImportConfig.setImportEndAction(importEndAction);
		baseImportConfig.setUseJavaName(false);
        baseImportConfig.setJobClosedListener(handleJobClosedListener());
        baseImportConfig.setStatusIdPolicy(this.statusIdPolicy);
        baseImportConfig.setIncreamentImport(increamentImport);
		initMetrics(  baseImportConfig);

		baseImportConfig.setJobInputParams(this.jobInputParams);
		baseImportConfig.setJobOutputParams(jobOutputParams);
        baseImportConfig.setJobInputParamGroups(jobInputParamGroups);

		baseImportConfig.setJobDynamicInputParams(jobDynamicInputParams);
		baseImportConfig.setJobDynamicOutputParams(jobDynamicOutputParams);
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

		baseImportConfig.setDeyLay(deyLay);
		baseImportConfig.setScheduleDate(scheduleDate);
		baseImportConfig.setScheduleEndDate(scheduleEndDate);
		if(jobId != null) {
			baseImportConfig.setJobId(jobId);
		}
//		else{
//			baseImportConfig.setJobId(SimpleStringUtil.getUUID());
//		}
		if(jobName != null)
			baseImportConfig.setJobName(jobName);
		else{
			if(baseImportConfig.getJobId() != null) {
				baseImportConfig.setJobName("Datatran-Job-" + baseImportConfig.getJobId());
			}
			else{
				baseImportConfig.setJobName("Datatran-Job");
			}
		}

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

    public ImportBuilder setMetricsLogReport(MetricsLogReport metricsLogReport) {
        this.metricsLogReport = metricsLogReport;
        return this;
    }

    public boolean isContinueOnError() {
		return continueOnError;
	}


	public Integer getFetchSize() {
		return fetchSize;
	}

    private boolean setFetchSized;
	public ImportBuilder setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
        setFetchSized = true;
		return this;
	}

    public boolean isSetFetchSized() {
        return setFetchSized;
    }


	protected DataStream innerBuilder(){
		
		this.buildGeoipConfig();
		buildDBConfig();
		this.buildOtherDBConfigs();
		this.buildStatusDBConfig();


		try {
			if(logger.isInfoEnabled()) {
				logger.info("Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		BaseImportContext importContext = new BaseImportContext();
        JobContext jobContext = new JobContext();
		importContext.setJobContext(jobContext);
		BaseImportConfig baseImportConfig = new BaseImportConfig() ;
		buildImportConfig(baseImportConfig);
		importContext.setBaseImportConfig(baseImportConfig);
		inputConfig.build(importContext,this);
		outputConfig.build(importContext,this);
		importContext.setInputConfig(inputConfig);
		importContext.setOutputConfig(outputConfig);
		if(this.exportResultHandler != null){

			baseImportConfig.setExportResultHandler(outputConfig.buildExportResultHandler( exportResultHandler));
		}
		importContext.afterBuild(this);


		DataStream dataStream = this.createDataStream();
		dataStream.setImportContext(importContext);
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(importContext));
		dataStream.initDatastream();
        importContext.registEndAction(new EndAction() {
            @Override
            public void endAction() {
                dataStream.endAction(null);
            }
        });
		importContext.setDataStream(dataStream);
		return dataStream;
	}
	public static ImportBuilder newInstance(){
		return new ImportBuilder();
	}

    public MetricsLogReport getMetricsLogReport() {
        return metricsLogReport;
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
			throw new DataImportException("InputConfig is null and must be set to ImportBuilder.");
		}
		if(outputConfig == null){
			throw new DataImportException("OutputConfig is null and must be set to ImportBuilder.");
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

	public ImportBuilder setJobName(String jobName) {
		this.jobName = jobName;
		return this;
	}

	public String getJobName() {
		return jobName;
	}

	public String getJobId() {
		return jobId;
	}

	public ImportBuilder setJobId(String jobId) {
		this.jobId = jobId;
		return this;
	}

	public ImportBuilder setInitJobContextCall(InitJobContextCall initJobContextCall){
		this.initJobContextCall = initJobContextCall;
		return this;
	}

	/**
	 * 添加指标计算构建器,支撑流处理机制
	 * @param metrics
	 * @return
	 */
	public ImportBuilder addMetrics(ETLMetrics metrics){
		if(this.metrics == null)
			this.metrics = new ArrayList<>();
		this.metrics.add(metrics);
		return this;
	}

	public ImportBuilder setUseDefaultMapData(boolean useDefaultMapData) {
		this.useDefaultMapData = useDefaultMapData;
		return this;
	}


    /**
     * 在不需要时间窗口的场景下，控制采集和指标计算混合作业定时调度时，是否在任务结束时强制flush metric进行持久化处理
     * true 强制flush
     * false 不强制刷新 默认值
     * @return
     */
    public ImportBuilder setFlushMetricsOnScheduleTaskCompleted(boolean flushMetricsOnScheduleTaskCompleted) {
        this.flushMetricsOnScheduleTaskCompleted = flushMetricsOnScheduleTaskCompleted;
        return this;
    }


    /**
     * 控制flush metrics时是否清空指标key内存缓存区
     * true 清空
     * false 不清空，默认值
     * @return
     */
    public ImportBuilder setCleanKeysWhenflushMetricsOnScheduleTaskCompleted(boolean cleanKeysWhenflushMetricsOnScheduleTaskCompleted) {
        this.cleanKeysWhenflushMetricsOnScheduleTaskCompleted = cleanKeysWhenflushMetricsOnScheduleTaskCompleted;
        return this;
    }

    /**
     * 控制是否等待flush metric持久化操作完成再返回，还是不等待直接返回（异步flush）
     * true 等待，默认值
     * false 不等待
     * @param waitCompleteWhenflushMetricsOnScheduleTaskCompleted
     * @return
     */
    public ImportBuilder setWaitCompleteWhenflushMetricsOnScheduleTaskCompleted(boolean waitCompleteWhenflushMetricsOnScheduleTaskCompleted) {
        this.waitCompleteWhenflushMetricsOnScheduleTaskCompleted = waitCompleteWhenflushMetricsOnScheduleTaskCompleted;
        return this;
    }

    public List<CallInterceptor> taskCallInterceptors() {
        return callInterceptors;
    }


    /**
     * 输入参数组，将输入参数和输入动态参数组装为一个参数组添加到参数组集合中，添加完成后重置输入参数和输入动态参数，为增加新的参数组做准备
     * 通过添加多个参数组，作业调度时，特定的输入插件可以利用参数组中的每组参数发起并发数据请求，比如httpinput插件
     * @return
     */
    public ImportBuilder makeParamGroup(){
        if(jobInputParams == null && jobDynamicInputParams == null){
            return this;
        }
        JobInputParamGroup jobInputParamGroup = new JobInputParamGroup();
        jobInputParamGroup.setJobInputParams(jobInputParams);
        jobInputParamGroup.setJobDynamicInputParams(jobDynamicInputParams);
        if(this.jobInputParamGroups == null)
        {
            this.jobInputParamGroups = new ArrayList<>();
        }
        this.jobInputParamGroups.add(jobInputParamGroup);
        jobInputParams = null;
        jobDynamicInputParams = null;
        return this;

    }

    public String getLastValueStorePassword() {
        return lastValueStorePassword;
    }

    public ImportBuilder setLastValueStorePassword(String lastValueStorePassword) {
        this.lastValueStorePassword = lastValueStorePassword;
        return this;
    }
    public String getStatusHistoryTableDML() {
        return statusHistoryTableDML;
    }

    public ImportBuilder setStatusHistoryTableDML(String statusHistoryTableDML) {
        this.statusHistoryTableDML = statusHistoryTableDML;
        return this;
    }

    public JobClosedListener getJobClosedListener() {
        return jobClosedListener;
    }

    public ImportBuilder setJobClosedListener(JobClosedListener jobClosedListener) {
        this.jobClosedListener = jobClosedListener;
        return this;
    }
    public Integer getTimeWindowType() {
        return timeWindowType;
    }

    public ImportBuilder setTimeWindowType(Integer timeWindowType) {
        this.timeWindowType = timeWindowType;
        return this;
    }

    public Integer getStatusIdPolicy() {
        return statusIdPolicy;
    }
    /**
     * 设置增量状态ID生成策略，在设置jobId的情况下起作用
     * STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
     * STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用jobType+jobId+作业查询语句hashcode或者文件名称，作为增量id作为增量状态id
     * 默认值STATUSID_POLICY_JOBID_QUERYSTATEMENT 
     */
    public ImportBuilder setStatusIdPolicy(Integer statusIdPolicy) {
        this.statusIdPolicy = statusIdPolicy;
        return this;
    }

    public Boolean getIncreamentImport() {
        return increamentImport;
    }

    public ImportBuilder setIncreamentImport(Boolean increamentImport) {
        this.increamentImport = increamentImport;
        return this;
    }

    public boolean isNumberTypeTimestamp() {
        return numberTypeTimestamp;
    }

    public ImportBuilder setNumberTypeTimestamp(boolean numberTypeTimestamp) {
        this.numberTypeTimestamp = numberTypeTimestamp;
        return this;
    }
}
