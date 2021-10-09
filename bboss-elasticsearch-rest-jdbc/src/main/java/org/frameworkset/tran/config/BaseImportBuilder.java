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
import com.frameworkset.orm.annotation.ESIndexWrapper;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.boot.ElasticSearchPropertiesFilePlugin;
import org.frameworkset.spi.DefaultApplicationContext;
import org.frameworkset.spi.assemble.GetProperties;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.ESConfig;
import org.frameworkset.tran.es.ESField;
import org.frameworkset.tran.ouput.custom.CustomOutPut;
import org.frameworkset.tran.record.SplitHandler;
import org.frameworkset.tran.schedule.CallInterceptor;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.ScheduleConfig;
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
public abstract class BaseImportBuilder {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	private DBConfig dbConfig ;
	private DBConfig statusDbConfig ;
	private String statusDbname;
	private String statusTableDML;
	private Integer fetchSize = 5000;

	public String getSplitFieldName() {
		return splitFieldName;
	}

	public BaseImportBuilder setSplitFieldName(String splitFieldName) {
		this.splitFieldName = splitFieldName;
		return this;
	}

	private String splitFieldName;

	private transient SplitHandler splitHandler;
	private transient CustomOutPut customOutPut;
	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 */
	private long flushInterval = 8000l;
	private boolean ignoreNullValueField;
	private String targetElasticsearch = "default";
	private String sourceElasticsearch = "default";
	private ESConfig esConfig;
	private ClientOptions clientOptions;
	private Map<String, Object> geoipConfig;

	private boolean sortLastValue = true;
	private boolean useBatchContextIndexName = false;
	public abstract DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext);
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
	public long getAsynResultPollTimeOut() {
		return asynResultPollTimeOut;
	}

	public BaseImportBuilder setAsynResultPollTimeOut(long asynResultPollTimeOut) {
		this.asynResultPollTimeOut = asynResultPollTimeOut;
		return this;
	}
	/**
	 *  对于有延迟的数据源，指定增量截止时间与当前时间的便宜量
	 *  增量查询截止时间为：System.currenttime - increamentEndOffset
	 *  对应的变量名称：getLastValueVarName()+"__endTime"
	 * @return
	 */
	public BaseImportBuilder setIncreamentEndOffset(Integer increamentEndOffset) {
		this.increamentEndOffset = increamentEndOffset;
		return this;
	}

	public BaseImportBuilder setAsynFlushStatus(boolean asynFlushStatus) {
		this.asynFlushStatus = asynFlushStatus;
		return this;
	}

	private long asynResultPollTimeOut = 1000;
	public Boolean getUseLowcase() {
		return useLowcase;
	}

	public BaseImportBuilder setUseLowcase(Boolean useLowcase) {
		this.useLowcase = useLowcase;
		return this;
	}

	private Boolean useLowcase;
	/**抽取数据的sql语句*/
//	private String refreshOption;
	private Integer scheduleBatchSize ;
	private String index;
	/**抽取数据的sql语句*/
	private String indexType;
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
	protected void buildDBConfig(){

//			PropertiesContainer propertiesContainer = new PropertiesContainer();
//
//			if(this.applicationPropertiesFile == null) {
//				propertiesContainer.addConfigPropertiesFile(DEFAULT_CONFIG_FILE);
//			}
//			else{
//				propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
//			}
			if (this.applicationPropertiesFile == null) {
//					propertiesContainer.addConfigPropertiesFile("application.properties");

			} else {
				ElasticSearchPropertiesFilePlugin.init(applicationPropertiesFile);
//					propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
			}
			GetProperties propertiesContainer = DefaultApplicationContext.getApplicationContext("conf/elasticsearch-boot-config.xml",false);
			String dbName  = propertiesContainer.getExternalProperty("db.name");
			if(dbName == null || dbName.equals(""))
				return;
			if(dbConfig == null)
				dbConfig = new DBConfig();
			_buildDBConfig(propertiesContainer,dbName,dbConfig, "");

	}
	protected void buildESConfig(){

	}
	@JsonIgnore
	public ExportResultHandler getExportResultHandler() {
		return exportResultHandler;
	}

	public BaseImportBuilder setExportResultHandler(ExportResultHandler exportResultHandler) {
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


	public BaseImportBuilder setEsIdGenerator(EsIdGenerator esIdGenerator) {
		if(esIdGenerator != null) {
			this.esIdGenerator = esIdGenerator;
			this.esIdGeneratorClass = esIdGenerator.getClass().getName();
		}
		return this;
	}
	protected void buildStatusDBConfig(){

			if(statusDbname == null) {
				GetProperties propertiesContainer = null;
				String prefix = "config.";
				if (this.applicationPropertiesFile == null) {
//					propertiesContainer.addConfigPropertiesFile("application.properties");

				} else {
					ElasticSearchPropertiesFilePlugin.init(applicationPropertiesFile);
//					propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
				}
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
	protected void builderConfig(){
		this.buildGeoipConfig();
		this.buildESConfig();
		this.buildDBConfig();
		this.buildStatusDBConfig();
		this.buildOtherDBConfigs();


	}
	/**
	 * 在数据导入过程可能需要使用的其他数据名称，需要在配置文件中定义相关名称的db配置
	 */
	protected void buildOtherDBConfigs(){

//			PropertiesContainer propertiesContainer = new PropertiesContainer();

//			if(this.applicationPropertiesFile == null) {
//				propertiesContainer.addConfigPropertiesFile("application.properties");
//			}
//			else{
//				propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
//			}
		if (this.applicationPropertiesFile == null) {
//					propertiesContainer.addConfigPropertiesFile("application.properties");

		} else {
			ElasticSearchPropertiesFilePlugin.init(applicationPropertiesFile);
//					propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
		}
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

	protected void _setJdbcFetchSize(Integer jdbcFetchSize) {
		this.customDBConfigs.put(DBConfig.db_jdbcFetchSize_key,1);
		checkDBConfig();
		dbConfig.setJdbcFetchSize(jdbcFetchSize);

	}
	protected void checkDBConfig(){
		if(this.dbConfig == null){
			this.dbConfig = new DBConfig();
		}
	}
	public void _setDbPassword(String dbPassword) {
		this.customDBConfigs.put(DBConfig.db_password_key,1);
		checkDBConfig();
		dbConfig.setDbPassword(dbPassword);

	}

	public BaseImportBuilder setDbName(String dbName) {
		_setDbName(  dbName);
		return this;
	}

	public BaseImportBuilder setColumnLableUpperCase(boolean columnLableUpperCase) {
		_setColumnLableUpperCase(columnLableUpperCase);
		return this;
	}

	public BaseImportBuilder setDbInitSize(int dbInitSize) {
		_setDbInitSize( dbInitSize);
		return this;
	}
	public BaseImportBuilder setDbMaxSize(int dbMaxSize) {
		_setDbMaxSize(  dbMaxSize);
		return this;
	}
	public BaseImportBuilder setDbMinIdleSize(int dbMinIdleSize) {
		_setDbMinIdleSize(  dbMinIdleSize);
		return this;
	}



	public BaseImportBuilder setDbDriver(String dbDriver) {
		_setDbDriver(  dbDriver);
		return this;
	}
	public BaseImportBuilder setEnableDBTransaction(boolean enableDBTransaction) {
		_setEnableDBTransaction(  enableDBTransaction);
		return this;
	}


	public BaseImportBuilder setDbUrl(String dbUrl) {
		_setDbUrl( dbUrl);
		return this;
	}

	public BaseImportBuilder setDbAdaptor(String dbAdaptor) {
		_setDbAdaptor(  dbAdaptor);
		return this;

	}

	public BaseImportBuilder setDbtype(String dbtype) {
		_setDbtype(  dbtype);
		return this;
	}

	public BaseImportBuilder setDbUser(String dbUser) {
		_setDbUser(  dbUser);
		return this;
	}

	public BaseImportBuilder setDbPassword(String dbPassword) {
		_setDbPassword(  dbPassword);
		return this;
	}

	public BaseImportBuilder setValidateSQL(String validateSQL) {
		_setValidateSQL(  validateSQL);
		return this;
	}

	public BaseImportBuilder setUsePool(boolean usePool) {
		_setUsePool(  usePool);
		return this;
	}

	public BaseImportBuilder setDbInfoEncryptClass(String dbInfoEncryptClass){
		_setDbInfoEncryptClass(dbInfoEncryptClass);
		return this;
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

	public BaseImportBuilder setParallel(boolean parallel) {
		this.parallel = parallel;
		return this;
	}

	public int getThreadCount() {
		return threadCount;
	}

	public BaseImportBuilder setThreadCount(int threadCount) {
		this.threadCount = threadCount;
		return this;
	}

	public int getQueue() {
		return queue;
	}

	public BaseImportBuilder setQueue(int queue) {
		this.queue = queue;
		return this;
	}

	public boolean isAsyn() {
		return asyn;
	}

	public BaseImportBuilder setAsyn(boolean asyn) {
		this.asyn = asyn;
		return this;
	}

	public BaseImportBuilder setContinueOnError(boolean continueOnError) {
		this.continueOnError = continueOnError;
		return this;
	}



	public BaseImportBuilder setEsParentIdValue(String esParentIdValue) {
		checkclientOptions();
		clientOptions.setEsParentIdValue(esParentIdValue);
//		this.esParentIdValue = esParentIdValue;
		return this;
	}



	public BaseImportBuilder setEsVersionValue(Object esVersionValue) {
		checkclientOptions();
		clientOptions.setVersion(esVersionValue);
//		this.esVersionValue = esVersionValue;
		return this;
	}

	public boolean isDiscardBulkResponse() {
		return discardBulkResponse;
	}

	public BaseImportBuilder setDiscardBulkResponse(boolean discardBulkResponse) {
		this.discardBulkResponse = discardBulkResponse;
		return this;
	}

	public boolean isDebugResponse() {
		return debugResponse;
	}

	public BaseImportBuilder setDebugResponse(boolean debugResponse) {
		this.debugResponse = debugResponse;
		return this;
	}


	public BaseImportBuilder setPeriod(Long period) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setPeriod(period);
		return this;
	}


	public BaseImportBuilder setDeyLay(Long deyLay) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setDeyLay(deyLay);
		return this;
	}



	public BaseImportBuilder setScheduleDate(Date scheduleDate) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setScheduleDate(scheduleDate);
		return this;
	}

	public BaseImportBuilder setFixedRate(Boolean fixedRate) {
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setFixedRate(fixedRate);
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
	public BaseImportBuilder setDateLastValueColumn(String dateLastValueColumn) {
		return setLastValueColumn(dateLastValueColumn);
	}


	public BaseImportBuilder setLastValueColumn(String numberLastValueColumn) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueColumn(numberLastValueColumn);
		return this;
	}

	/**
	 * @See use setLastValueColumn(String numberLastValueColumn)
	 * @param numberLastValueColumn
	 * @return
	 */
	@Deprecated
	public BaseImportBuilder setNumberLastValueColumn(String numberLastValueColumn) {
		return setLastValueColumn(numberLastValueColumn);
	}


	public BaseImportBuilder setLastValueStorePath(String lastValueStorePath) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueStorePath(lastValueStorePath);
		return this;
	}



	public BaseImportBuilder setLastValueStoreTableName(String lastValueStoreTableName) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueStoreTableName(lastValueStoreTableName);
		return this;
	}

	public BaseImportBuilder setFromFirst(boolean fromFirst) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setFromFirst(fromFirst);
		return this;
	}

	public BaseImportBuilder setLastValue(Object lastValue) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValue(lastValue);
		return this;
	}

	public BaseImportBuilder setLastValueType(int lastValueType) {
		if(importIncreamentConfig == null){
			importIncreamentConfig = new ImportIncreamentConfig();
		}
		this.importIncreamentConfig.setLastValueType(lastValueType);
		return this;
	}

	public BaseImportBuilder setJdbcFetchSize(Integer jdbcFetchSize) {
		_setJdbcFetchSize(  jdbcFetchSize);
		return  this;
	}

	public Integer getScheduleBatchSize() {
		return scheduleBatchSize;
	}

	public BaseImportBuilder setScheduleBatchSize(Integer scheduleBatchSize) {
		this.scheduleBatchSize = scheduleBatchSize;
		return this;
	}
	public BaseImportBuilder addCallInterceptor(CallInterceptor interceptor){
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

	public BaseImportBuilder setPrintTaskLog(boolean printTaskLog) {
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
			if(esIdGenerator != null)
				ret.append(",esIdGenerator="+esIdGenerator.getClass().getCanonicalName());
			else
				ret.append(",esIdGenerator=null");
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

	public void _setShowSql(boolean showSql) {
		this.customDBConfigs.put(DBConfig.db_showsql_key,1);
		checkDBConfig();
		dbConfig.setShowSql(showSql);


	}

	public void _setDbName(String dbName) {
		this.customDBConfigs.put(DBConfig.db_name_key,1);
		checkDBConfig();

		dbConfig.setDbName(dbName);

	}
	public void _setColumnLableUpperCase(boolean columnLableUpperCase) {
		this.customDBConfigs.put(DBConfig.db_columnLableUpperCase_key,1);
		checkDBConfig();

		dbConfig.setColumnLableUpperCase(columnLableUpperCase);
	}
	public void _setDbInitSize(int dbInitSize) {
		this.customDBConfigs.put(DBConfig.db_initSize_key,1);
		checkDBConfig();

		dbConfig.setInitSize(dbInitSize);
	}
	public void _setDbMaxSize(int dbMaxSize) {
		this.customDBConfigs.put(DBConfig.db_maxSize_key,1);
		checkDBConfig();

		dbConfig.setMaxSize(dbMaxSize);
	}
	public void _setDbMinIdleSize(int dbMinIdleSize) {
		this.customDBConfigs.put(DBConfig.db_minIdleSize_key,1);
		checkDBConfig();

		dbConfig.setMinIdleSize(dbMinIdleSize);
	}

	public void _setDbDriver(String dbDriver) {
		this.customDBConfigs.put(DBConfig.db_driver_key,1);
		checkDBConfig();
		this.dbConfig.setDbDriver(dbDriver);
	}
	public void _setEnableDBTransaction(boolean enableDBTransaction) {
		this.customDBConfigs.put(DBConfig.db_enableDBTransaction_key,1);
		checkDBConfig();
		dbConfig.setEnableDBTransaction(enableDBTransaction);
	}

	public void _setDbAdaptor(String dbAdaptor) {
		this.customDBConfigs.put(DBConfig.db_dbAdaptor_key,1);
		checkDBConfig();
		this.dbConfig.setDbAdaptor(dbAdaptor);
	}

	public void _setDbtype(String dbtype) {
		this.customDBConfigs.put(DBConfig.db_dbtype_key,1);
		checkDBConfig();
		this.dbConfig.setDbtype(dbtype);
	}

	public void _setDbUrl(String dbUrl) {
		this.customDBConfigs.put(DBConfig.db_url_key,1);
		checkDBConfig();
		dbConfig.setDbUrl(dbUrl);
	}

	public void _setDbUser(String dbUser) {
		this.customDBConfigs.put(DBConfig.db_user_key,1);
		checkDBConfig();
		this.dbConfig.setDbUser(dbUser);
	}

	public void _setValidateSQL(String validateSQL) {
		this.customDBConfigs.put(DBConfig.db_validateSQL_key,1);
		checkDBConfig();
		dbConfig.setValidateSQL(validateSQL);
	}

	public void _setUsePool(boolean usePool) {
		this.customDBConfigs.put(DBConfig.db_usePool_key,1);
		checkDBConfig();
		dbConfig.setUsePool(usePool);
	}

	public void _setDbInfoEncryptClass(String dbInfoEncryptClass) {
		this.customDBConfigs.put(DBConfig.db_dbInfoEncryptClass_key,1);
		checkDBConfig();
		dbConfig.setDbInfoEncryptClass(dbInfoEncryptClass);
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
	public static void addFieldValue(List<FieldMeta> fieldValues,String fieldName,Object value){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setTargetFieldName(fieldName);
		fieldMeta.setValue(value);
		fieldValues.add(fieldMeta);
	}


	public static void addFieldValue(List<FieldMeta> fieldValues,String fieldName,String dateFormat,Object value,String locale,String timeZone){
		FieldMeta fieldMeta = new FieldMeta();
		fieldMeta.setTargetFieldName(fieldName);
		fieldMeta.setValue(value);
		fieldMeta.setDateFormateMeta(buildDateFormateMeta( dateFormat,  locale,  timeZone));
		fieldValues.add(fieldMeta);

	}


	public BaseImportBuilder setTimeZone(String timeZone) {
		this.timeZone = timeZone;
		return this;
	}

	public BaseImportBuilder setLocale(String locale) {
		this.locale = locale;
		return this;
	}

	public BaseImportBuilder setDateFormat(String dateFormat) {
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
	private transient EsIdGenerator esIdGenerator = BaseImportConfig.DEFAULT_EsIdGenerator;
	private final Map<String,FieldMeta> fieldMetaMap = new HashMap<String,FieldMeta>();
	private String esIdGeneratorClass = "org.frameworkset.tran.DefaultEsIdGenerator";
	private final List<FieldMeta> fieldValues = new ArrayList<FieldMeta>();
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
	public BaseImportBuilder addFieldMapping(String sourceFieldName, String targetFieldName){
		this.fieldMetaMap.put(sourceFieldName,buildFieldMeta(  sourceFieldName,  targetFieldName,null ));
		return this;
	}

	public BaseImportBuilder addIgnoreFieldMapping(String sourceFieldName){
		addIgnoreFieldMapping(fieldMetaMap, sourceFieldName);
		return this;
	}

	public static void addIgnoreFieldMapping(Map<String,FieldMeta> fieldMetaMap, String sourceColumnName){
		fieldMetaMap.put(sourceColumnName,buildIgnoreFieldMeta(  sourceColumnName));
	}

	public BaseImportBuilder addFieldMapping(String sourceColumnName, String targetFieldName, String dateFormat){
		this.fieldMetaMap.put(sourceColumnName,buildFieldMeta(  sourceColumnName,  targetFieldName ,dateFormat));
		return this;
	}

	public BaseImportBuilder addFieldMapping(String sourceColumnName, String targetFieldName, String dateFormat, String locale, String timeZone){
		this.fieldMetaMap.put(sourceColumnName,buildFieldMeta(  sourceColumnName,  targetFieldName ,dateFormat,  locale,  timeZone));
		return this;
	}


	/**
	 * 补充额外的字段和值
	 * @param fieldName
	 * @param value
	 * @return
	 */
	public BaseImportBuilder addFieldValue(String fieldName, Object value){
		addFieldValue(  fieldValues,  fieldName,  value);
		return this;
	}

	/**
	 * 补充额外的字段和值
	 * @param fieldName
	 * @param dateFormat
	 * @param value
	 * @return
	 */
	public BaseImportBuilder addFieldValue(String fieldName, String dateFormat, Object value){
		addFieldValue(  fieldValues,  fieldName,  dateFormat,  value,  locale,  timeZone);
		return this;
	}
	public BaseImportBuilder addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone){
		addFieldValue(  fieldValues,  fieldName,  dateFormat,  value,  locale,  timeZone);
		return this;
	}



	public String getDataRefactorClass() {
		return dataRefactorClass;
	}

	private String dataRefactorClass;
	public BaseImportBuilder setDataRefactor(DataRefactor dataRefactor) {
		this.dataRefactor = dataRefactor;
		dataRefactorClass = dataRefactor.getClass().getName();
		return this;
	}

	public BaseImportBuilder setExternalTimer(boolean externalTimer) {
		this.externalTimer = externalTimer;
		if(scheduleConfig == null){
			scheduleConfig = new ScheduleConfig();
		}
		this.scheduleConfig.setExternalTimer(externalTimer);
		return this;
	}



	public boolean isPagine() {
		return pagine;
	}

	public BaseImportBuilder setPagine(boolean pagine) {
		this.pagine = pagine;
		return this;
	}
	//是否采用分页抽取数据
	private boolean pagine ;

	public DBConfig getDbConfig() {
		return dbConfig;
	}
	@JsonIgnore
	public String getDbName(){
		if(dbConfig != null)
			return dbConfig.getDbName();
		throw new ESDataImportException("dbconfig is null.");
	}

	public static void main(String[] args){
		System.out.println("meta:_id".substring(5));//""
	}
	protected void buildImportConfig(BaseImportConfig baseImportConfig){
		if(getTargetElasticsearch() != null && !getTargetElasticsearch().equals(""))
			baseImportConfig.setTargetElasticsearch(this.getTargetElasticsearch());
		if(getSourceElasticsearch() != null && !getSourceElasticsearch().equals(""))
			baseImportConfig.setSourceElasticsearch(this.getSourceElasticsearch());
		if(this.esConfig != null){
			baseImportConfig.setEsConfig(esConfig);
		}
		if(geoipConfig != null && geoipConfig.size() > 0){
			baseImportConfig.setGeoipConfig(geoipConfig);
		}
		baseImportConfig.setDateFormat(dateFormat);
		baseImportConfig.setLocale(locale);
		baseImportConfig.setTimeZone(this.timeZone);

		baseImportConfig.setFetchSize(this.fetchSize);

		baseImportConfig.setClientOptions(clientOptions);
//		baseImportConfig.setRoutingField(this.routingField);
		baseImportConfig.setUseJavaName(this.useJavaName);
		baseImportConfig.setFieldMetaMap(this.fieldMetaMap);
		baseImportConfig.setFieldValues(fieldValues);
		baseImportConfig.setDataRefactor(this.dataRefactor);
		baseImportConfig.setSortLastValue(this.sortLastValue);
		baseImportConfig.setDbConfig(dbConfig);
		baseImportConfig.setStatusDbConfig(statusDbConfig);

		baseImportConfig.setConfigs(this.configs);
		baseImportConfig.setBatchSize(this.batchSize);
		if(index != null) {
			ESIndexWrapper esIndexWrapper = new ESIndexWrapper(index, indexType);
//			esIndexWrapper.setUseBatchContextIndexName(this.useBatchContextIndexName);
			baseImportConfig.setEsIndexWrapper(esIndexWrapper);
		}


		baseImportConfig.setApplicationPropertiesFile(this.applicationPropertiesFile);
		baseImportConfig.setParallel(this.parallel);
		baseImportConfig.setThreadCount(this.threadCount);
		baseImportConfig.setQueue(this.queue);
		baseImportConfig.setAsyn(this.asyn);
		baseImportConfig.setContinueOnError(this.continueOnError);
		baseImportConfig.setAsynResultPollTimeOut(this.asynResultPollTimeOut);
		/**
		 * 是否不需要返回响应，不需要的情况下，可以设置为true，
		 * 提升性能，如果debugResponse设置为true，那么强制返回并打印响应到日志文件中
		 */
		baseImportConfig.setDiscardBulkResponse(this.discardBulkResponse);
		/**是否调试bulk响应日志，true启用，false 不启用，*/
		baseImportConfig.setDebugResponse(this.debugResponse);
		baseImportConfig.setScheduleConfig(this.scheduleConfig);//定时任务配置
		baseImportConfig.setImportIncreamentConfig(this.importIncreamentConfig);//增量数据配置

		if(this.scheduleBatchSize != null)
			baseImportConfig.setScheduleBatchSize(this.scheduleBatchSize);
		else
			baseImportConfig.setScheduleBatchSize(this.batchSize);
		baseImportConfig.setCallInterceptors(this.callInterceptors);
		baseImportConfig.setUseLowcase(this.useLowcase);
		baseImportConfig.setPrintTaskLog(this.printTaskLog);
		baseImportConfig.setEsIdGenerator(esIdGenerator);
		if(this.exportResultHandler != null){

			baseImportConfig.setExportResultHandler(buildExportResultHandler( exportResultHandler));
		}
		baseImportConfig.setPagine(this.pagine);
		baseImportConfig.setTranDataBufferQueue(this.tranDataBufferQueue);
		baseImportConfig.setFlushInterval(this.flushInterval);
		baseImportConfig.setIgnoreNullValueField(this.ignoreNullValueField);
		baseImportConfig.setIncreamentEndOffset(this.increamentEndOffset);
		baseImportConfig.setAsynFlushStatus(this.asynFlushStatus);
		baseImportConfig.setAsynFlushStatusInterval(this.asynFlushStatusInterval);
		baseImportConfig.setSplitHandler(this.getSplitHandler());
		baseImportConfig.setSplitFieldName(getSplitFieldName());
		baseImportConfig.setCustomOutPut(customOutPut);
//		baseImportConfig.setEsDetectNoop(this.esDetectNoop);

	}


	public BaseImportBuilder setTimeout(String timeout) {
		checkclientOptions();
		clientOptions.setTimeout(timeout);
		return this;
	}



	public BaseImportBuilder setMasterTimeout(String masterTimeout) {
		checkclientOptions();
		clientOptions.setMasterTimeout(masterTimeout);
		return this;
	}



	public BaseImportBuilder setWaitForActiveShards(Integer waitForActiveShards) {
		checkclientOptions();
		clientOptions.setWaitForActiveShards(waitForActiveShards);
		return this;
	}


	public BaseImportBuilder setSourceUpdateExcludes(List<String> sourceUpdateExcludes) {
		checkclientOptions();
		clientOptions.setSourceUpdateExcludes(sourceUpdateExcludes);
		return this;
	}

	public BaseImportBuilder setSourceUpdateIncludes(List<String> sourceUpdateIncludes) {
		checkclientOptions();
		clientOptions.setSourceUpdateIncludes(sourceUpdateIncludes);
		return this;
	}
	protected abstract WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler);
	public BaseImportBuilder setIndexType(String indexType) {
		this.indexType = indexType;
		return this;
	}

	public BaseImportBuilder setIndex(String index) {
		this.index = index;
		return this;
	}

	public BaseImportBuilder setBatchSize(int batchSize) {
		this.batchSize = batchSize;
		return this;
	}

	public BaseImportBuilder setRefreshOption(String refreshOption) {
		this.checkclientOptions();
		this.clientOptions.setRefreshOption(refreshOption);
		return this;
	}






	public int getBatchSize() {
		return batchSize;
	}

	public String getIndex() {
		return index;
	}

	public String getIndexType() {
		return indexType;
	}



	public Boolean getUseJavaName() {
		return useJavaName;
	}






	public BaseImportBuilder setUseJavaName(Boolean useJavaName) {
		this.useJavaName = useJavaName;
		return this;
	}

	public BaseImportBuilder setEsVersionType(String esVersionType) {
		checkclientOptions();
		clientOptions.setVersionType(esVersionType);
		return this;
	}

	public BaseImportBuilder setEsVersionField(String esVersionField) {
		checkclientOptions();
		if (!esVersionField.startsWith("meta:"))
			clientOptions.setVersionField(new ESField(false,esVersionField));
		else{
			clientOptions.setVersionField(new ESField(true,esVersionField.substring(5)));

		}
		return this;
	}

	public BaseImportBuilder setEsReturnSource(Boolean esReturnSource) {
		checkclientOptions();
		clientOptions.setReturnSource(esReturnSource);
		return this;
	}

	public BaseImportBuilder setEsRetryOnConflict(Integer esRetryOnConflict) {
		checkclientOptions();
		clientOptions.setEsRetryOnConflict(esRetryOnConflict);
		return this;
	}

	public BaseImportBuilder setEsDocAsUpsert(Boolean esDocAsUpsert) {
		checkclientOptions();
		clientOptions.setDocasupsert(esDocAsUpsert);
		return this;
	}

	public BaseImportBuilder setRoutingValue(String routingValue) {
		checkclientOptions();
		clientOptions.setRouting(routingValue);
		return this;
	}

	/**
	 * 设置Elasticsearch批处理入库的filter_path参数
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	 * @param filterPath
	 * @return
	 */
	public BaseImportBuilder setFilterPath(String filterPath) {
		checkclientOptions();
		clientOptions.setFilterPath(filterPath);
		return this;
	}

	public BaseImportBuilder setRoutingField(String routingField) {
		checkclientOptions();
		if (!routingField.startsWith("meta:"))
			clientOptions.setRoutingField(new ESField(false,routingField));
		else{
			clientOptions.setRoutingField(new ESField(true,routingField.substring(5)));

		}
		return this;
	}

	public BaseImportBuilder setEsParentIdField(String esParentIdField) {
		checkclientOptions();
		if (!esParentIdField.startsWith("meta:"))
			clientOptions.setParentIdField(new ESField(false,esParentIdField));
		else{
			clientOptions.setParentIdField(new ESField(true,esParentIdField.substring(5)));

		}
		return this;
	}

	public BaseImportBuilder setEsIdField(String esIdField) {
		checkclientOptions();
		if (!esIdField.startsWith("meta:"))
			clientOptions.setIdField(new ESField(false,esIdField));
		else{
			clientOptions.setIdField(new ESField(true,esIdField.substring(5)));

		}

		return this;
	}


	public boolean isContinueOnError() {
		return continueOnError;
	}


	public Integer getFetchSize() {
		return fetchSize;
	}

	public BaseImportBuilder setFetchSize(Integer fetchSize) {
		this.fetchSize = fetchSize;
		return this;
	}
	public abstract DataStream builder();
	public BaseImportBuilder setTranDataBufferQueue(int tranDataBufferQueue) {
		this.tranDataBufferQueue = tranDataBufferQueue;
		return this;
	}

	/**
	 * 源数据批量预加载队列大小，需要用到的最大缓冲内存为：
	 *  tranDataBufferQueue * fetchSize * 单条记录mem大小
	 */
	private int tranDataBufferQueue = 10;

	public String getEsIdGeneratorClass() {
		return esIdGeneratorClass;
	}

	public long getFlushInterval() {
		return flushInterval;
	}

	/**
	 * 设置强制刷新检测空闲时间间隔，单位：毫秒，在空闲flushInterval后，还没有数据到来，强制将已经入列的数据进行存储操作，默认8秒,为0时关闭本机制
	 * @param flushInterval
	 * @return
	 */
	public BaseImportBuilder setFlushInterval(long flushInterval) {
		this.flushInterval = flushInterval;
		return this;
	}

	public boolean isIgnoreNullValueField() {
		return ignoreNullValueField;
	}

	public BaseImportBuilder setIgnoreNullValueField(boolean ignoreNullValueField) {
		this.ignoreNullValueField = ignoreNullValueField;
		return this;
	}




	public BaseImportBuilder setEsDetectNoop(Object esDetectNoop) {
		checkclientOptions();
		clientOptions.setDetectNoop(esDetectNoop);
		return this;
	}
	public boolean isSortLastValue() {
		return sortLastValue;
	}
	private void checkclientOptions(){
		if(clientOptions == null){
			clientOptions = new ClientOptions();
		}
	}
	public BaseImportBuilder setSortLastValue(boolean sortLastValue) {
		this.sortLastValue = sortLastValue;
		return this;
	}

	public ClientOptions getClientOptions() {
		return clientOptions;
	}
	private void copy(ClientOptions oldClientOptions,ClientOptions newClientOptions){
		if(oldClientOptions.getIdField() !=null )
			newClientOptions.setIdField(oldClientOptions.getIdField());
		if(oldClientOptions.getRefreshOption() != null)
			newClientOptions.setRefreshOption(oldClientOptions.getRefreshOption());
	}
	public BaseImportBuilder setClientOptions(ClientOptions clientOptions) {
		if(this.clientOptions != null){
			copy(this.clientOptions,clientOptions);
		}
		this.clientOptions = clientOptions;

		return this;
	}

	public String getTargetElasticsearch() {
		return targetElasticsearch;
	}

	public BaseImportBuilder setTargetElasticsearch(String targetElasticsearch) {
		this.targetElasticsearch = targetElasticsearch;
		return this;
	}

	public String getSourceElasticsearch() {
		return sourceElasticsearch;
	}

	public BaseImportBuilder setSourceElasticsearch(String sourceElasticsearch) {
		this.sourceElasticsearch = sourceElasticsearch;
		return this;
	}

	/**
	 * 添加es客户端配置属性，具体的配置项参考文档：
	 * https://esdoc.bbossgroups.com/#/development?id=_2-elasticsearch%e9%85%8d%e7%bd%ae
	 *
	 * 如果在代码中指定配置项，就不会去加载application.properties中指定的数据源配置，如果没有配置则去加载applciation.properties中的对应数据源配置
	 * @param name
	 * @param value
	 * @return
	 */
	public BaseImportBuilder addElasticsearchProperty(String name,String value){
		if(this.esConfig == null){
			esConfig = new ESConfig();
		}
		esConfig.addElasticsearchProperty(name,value);
		return this;
	}


	public boolean isUseBatchContextIndexName() {
		return useBatchContextIndexName;
	}

	public BaseImportBuilder setUseBatchContextIndexName(boolean useBatchContextIndexName) {
		this.useBatchContextIndexName = useBatchContextIndexName;
		return this;
	}

	public BaseImportBuilder setGeoipDatabase(String geoipDatabase) {
		this.geoipDatabase = geoipDatabase;
		return this;
	}

	public BaseImportBuilder setGeoipAsnDatabase(String geoipAsnDatabase) {
		this.geoipAsnDatabase = geoipAsnDatabase;
		return this;
	}

	public BaseImportBuilder setGeoip2regionDatabase(String geoip2regionDatabase) {
		this.geoip2regionDatabase = geoip2regionDatabase;
		return this;
	}

	public BaseImportBuilder setGeoipCachesize(int geoipCachesize) {
		this.geoipCachesize = geoipCachesize;
		return this;
	}

	public BaseImportBuilder setGeoipTaobaoServiceURL(String geoipTaobaoServiceURL) {
		this.geoipTaobaoServiceURL = geoipTaobaoServiceURL;
		return this;
	}

	public String getStatusDbname() {
		return statusDbname;
	}

	public BaseImportBuilder setStatusDbname(String statusDbname) {
		this.statusDbname = statusDbname;
		return this;
	}

	public String getStatusTableDML() {
		return statusTableDML;
	}

	public BaseImportBuilder setStatusTableDML(String statusTableDML) {
		this.statusTableDML = statusTableDML;
		return this;
	}

	public BaseImportBuilder setGeoipIspConverter(Object geoipIspConverter) {
		this.geoipIspConverter = geoipIspConverter;
		return this;
	}
	protected abstract ImportContext buildImportContext(BaseImportConfig importConfig);
	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig) {
		return null;
	}

	public long getAsynFlushStatusInterval() {
		return asynFlushStatusInterval;
	}

	public BaseImportBuilder setAsynFlushStatusInterval(long asynFlushStatusInterval) {
		this.asynFlushStatusInterval = asynFlushStatusInterval;
		return this;
	}
	@JsonIgnore
	public SplitHandler getSplitHandler() {
		return splitHandler;
	}

	public BaseImportBuilder setSplitHandler(SplitHandler splitHandler) {
		this.splitHandler = splitHandler;
		return this;
	}
	@JsonIgnore
	public CustomOutPut getCustomOutPut() {
		return customOutPut;
	}

	public BaseImportBuilder setCustomOutPut(CustomOutPut customOutPut) {
		this.customOutPut = customOutPut;
		return this;
	}
}
