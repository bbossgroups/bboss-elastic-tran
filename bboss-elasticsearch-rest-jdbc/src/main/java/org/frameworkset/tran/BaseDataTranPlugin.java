package org.frameworkset.tran;
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

import com.frameworkset.common.poolman.SQLExecutor;
import com.frameworkset.common.poolman.util.DBConf;
import com.frameworkset.common.poolman.util.JDBCPool;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.DefaultStatusManager;
import org.frameworkset.tran.status.SingleStatusManager;
import org.frameworkset.tran.status.StatusManager;
import org.frameworkset.tran.util.TranConstant;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.Thread.sleep;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 16:55
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseDataTranPlugin implements DataTranPlugin {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	private boolean increamentImport = true;
	private ExportCount exportCount;
	protected StatusManager statusManager;
	public ExportCount getExportCount() {
		return exportCount;
	}

	/**
	 * 识别任务是否已经完成
	 * @param status
	 * @return
	 */
	public boolean isComplete(Status status){
		return status.getStatus() == ImportIncreamentConfig.STATUS_COMPLETE;
	}
	public Context buildContext(TaskContext taskContext,TranResultSet jdbcResultSet, BatchContext batchContext){
		return new ContextImpl(  taskContext,importContext,targetImportContext, jdbcResultSet, batchContext);
	}
	@Override
	public String getLastValueVarName() {
		return importContext.getLastValueColumn();
	}
	public Long getTimeRangeLastValue(){
		return null;
	}
	public BaseDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
//		init(importContext,targetImportContext);
		importContext.setDataTranPlugin(this);
		targetImportContext.setDataTranPlugin(this);
	}
	public void init(ImportContext importContext,ImportContext targetImportContext){

	}
	@Override
	public ImportContext getImportContext() {
		return importContext;
	}

	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

	protected ImportContext importContext;
	protected ImportContext targetImportContext;
	public ImportContext getTargetImportContext() {
		return targetImportContext;
	}

	public void setTargetImportContext(ImportContext targetImportContext) {
		this.targetImportContext = targetImportContext;
	}

	protected volatile Status currentStatus;
	protected volatile Status firstStatus;
	protected String updateSQL ;
	protected String updateStatusSQL;
	protected String insertSQL;
	protected String insertHistorySQL;
	protected String createStatusTableSQL;
	protected String createHistoryStatusTableSQL;
	protected String selectSQL;
	protected String deleteSQL;
	protected String selectAllSQL;
	protected String existSQL;
	protected int lastValueType = ImportIncreamentConfig.NUMBER_TYPE;

	protected Date initLastDate = null;
	protected String statusDbname;
	protected String statusTableName;
	protected String historyStatusTableName;
	protected String statusStorePath;
	protected String lastValueClumnName;
	protected ScheduleService scheduleService;
	protected boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}

	public void preCall(TaskContext taskContext){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		for(CallInterceptor callInterceptor: callInterceptors){
			try{
				callInterceptor.preCall(taskContext);
			}
			catch (Exception e){
				logger.error("preCall failed:",e);
			}
		}

	}
	public void afterCall(TaskContext taskContext){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		CallInterceptor callInterceptor = null;
		for(int j = callInterceptors.size() - 1; j >= 0; j --){
			callInterceptor = callInterceptors.get(j);
			try{
				callInterceptor.afterCall(taskContext);
			}
			catch (Exception e){
				logger.error("afterCall failed:",e);
			}
		}
	}

	public void throwException(TaskContext taskContext,Exception e){
		List<CallInterceptor> callInterceptors = importContext.getCallInterceptors();
		if(callInterceptors == null || callInterceptors.size() == 0)
			return;
		CallInterceptor callInterceptor = null;
		for(int j = callInterceptors.size() - 1; j >= 0; j --){
			callInterceptor = callInterceptors.get(j);
			try{
				callInterceptor.throwException(taskContext,e);
			}
			catch (Exception e1){
				logger.error("afterCall failed:",e1);
			}
		}

	}
	@Override
	public void importData() throws ESDataImportException {

		if(this.scheduleService == null) {//一次性执行数据导入操作

			long importStartTime = System.currentTimeMillis();

			TaskContext taskContext = new TaskContext(importContext,targetImportContext);
			try {

				preCall(taskContext);
				this.doImportData(taskContext);
				afterCall(taskContext);
				long importEndTime = System.currentTimeMillis();
				if( isPrintTaskLog())
					logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
			}
			catch (Exception e){
				throwException(taskContext,e);
				logger.error("scheduleImportData failed:",e);
			}

		}
		else{//定时增量导入数据操作
			try {
				if (!this.importContext.isExternalTimer()) {//内部定时任务引擎
					scheduleService.timeSchedule( );
				} else { //外部定时任务引擎执行的方法，比如quartz之类的
					scheduleService.externalTimeSchedule( );
				}
			}
			catch (ESDataImportException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				throw new ESDataImportException(e);
			}
		}

	}





	public abstract void beforeInit();
	public abstract void afterInit();
	public abstract void initStatusTableId();
	protected void initStatusManager(){
		statusManager = new SingleStatusManager(statusDbname, updateSQL, lastValueType,this);
		statusManager.init();
	}

	protected void _initStatusManager(){
		if(this.importContext.isAsynFlushStatus()) {
			initStatusManager();
		}
		else{
			statusManager = new DefaultStatusManager(statusDbname, updateSQL, lastValueType,this);
		}
	}
	@Override
	public void init() {
		exportCount = new ExportCount();
		beforeInit();
		this.initSchedule();
		initLastValueClumnName();
		initStatusStore();
		initDatasource();
		if(this.isIncreamentImport() && this.importContext.getStatusTableId() == null) {
			this.initStatusTableId();
		}
		initTableAndStatus();
		afterInit();

	}
	public boolean isMultiTran(){
		return false;
	}
	public String getLastValueClumnName(){
		return this.lastValueClumnName;
	}
	public boolean isContinueOnError(){
		return this.importContext.isContinueOnError();
	}

	/**
	 * 插件运行状态
	 */
	protected volatile int status = TranConstant.PLUGIN_START;
	protected volatile boolean hasTran = false;
	private ReentrantLock lock = new ReentrantLock();
	/**
	 *
	 */
	private AtomicInteger tranCounts = new AtomicInteger(0);
	public void setHasTran(){
		try {
			lock.lock();
			tranCounts.incrementAndGet();
			this.hasTran = true;
			status = TranConstant.PLUGIN_START;
		}
		finally {
			lock.unlock();
		}

	}
	public void setNoTran(){

		try {
			lock.lock();
			int count = tranCounts.decrementAndGet();
			if(count <= 0) {
				this.hasTran = false;
				this.status = TranConstant.PLUGIN_STOPREADY;
			}
		}
		finally {
			lock.unlock();
		}
	}
	public boolean isPluginStopAppending(){
		try {
			lock.lock();
			return status == TranConstant.PLUGIN_STOPAPPENDING;
		}
		finally {
			lock.unlock();
		}
	}
	public boolean isPluginStopREADY(){
		try {
			lock.lock();
			return status == TranConstant.PLUGIN_STOPREADY;
		}
		finally {
			lock.unlock();
		}
	}
	public boolean checkTranToStop(){
		try {
			lock.lock();
			return status == TranConstant.PLUGIN_STOPAPPENDING
				|| status == TranConstant.PLUGIN_STOPREADY || hasTran == false;
		}
		finally {
			lock.unlock();
		}
	}


	@Override
	public void destroy(boolean waitTranStop) {
//
//		this.status = TranConstant.PLUGIN_STOPAPPENDING;
//		do{
//			if(status == TranConstant.PLUGIN_STOPREADY){
//				break;
//			}
//			try {
//				sleep(1000l);
//			} catch (InterruptedException e) {
//
//			}
//		}while(true);

		this.status = TranConstant.PLUGIN_STOPAPPENDING;
		if(waitTranStop) {
			do {
				if (status == TranConstant.PLUGIN_STOPREADY || !hasTran) {
					break;
				}
				try {
					sleep(1000l);
				} catch (InterruptedException e) {

				}
			} while (true);
		}
		if(scheduleService != null){
			scheduleService.stop();
		}
		if(statusManager != null)
			statusManager.stop();
		try {
			if(statusDbname != null && !statusDbname.equals(""))
				SQLUtil.stopPool(this.statusDbname);
		}
		catch (Exception e){
			logger.error("Stop status db pool["+statusDbname+"] failed:",e);
		}
		this.stopDS(importContext.getDbConfig());
		this.stopOtherDSES(importContext.getConfigs());

	}


	public Object putLastParamValue(Map params){
		Object lastValue = this.currentStatus.getLastValue();
		if(this.lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			params.put(getLastValueVarName(), lastValue);


		}
		else{
			if(lastValue instanceof Date) {
				params.put(getLastValueVarName(), lastValue);

			}
			else {
				if(lastValue instanceof Long) {
					params.put(getLastValueVarName(), new Date((Long)lastValue));
				}
				else if(lastValue instanceof Integer){
					params.put(getLastValueVarName(), new Date(((Integer) lastValue).longValue()));
				}
				else if(lastValue instanceof Short){
					params.put(getLastValueVarName(), new Date(((Short) lastValue).longValue()));
				}
				else{
					params.put(getLastValueVarName(), new Date(((Number) lastValue).longValue()));
				}
			}

			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				params.put(getLastValueVarName()+"__endTime", lastOffsetValue);
			}
		}
		if(isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(params).toString());
		}
		return lastValue;
	}



	public Map getParamValue(){
		Object lastValue = this.currentStatus.getLastValue();
		Map params = new HashMap();
		if(this.lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			params.put(getLastValueVarName(), lastValue);
		}
		else{
			if(lastValue instanceof Date)
				params.put(getLastValueVarName(), lastValue);
			else {
				if(lastValue instanceof Long) {
					params.put(getLastValueVarName(), new Date((Long)lastValue));
				}
				else if(lastValue instanceof Integer){
					params.put(getLastValueVarName(), new Date(((Integer) lastValue).longValue()));
				}
				else if(lastValue instanceof Short){
					params.put(getLastValueVarName(), new Date(((Short) lastValue).longValue()));
				}
				else{
					params.put(getLastValueVarName(), new Date(((Number) lastValue).longValue()));
				}
			}
			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				params.put(getLastValueVarName()+"__endTime", lastOffsetValue);
			}
		}
		if(isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(params).toString());
		}
		return params;
	}
	public void initLastValueClumnName(){
		if(lastValueClumnName != null){
			return ;
		}

		if (importContext.getLastValueColumn() != null) {
			lastValueClumnName = importContext.getLastValueColumn();
		}
//		else if (importContext.getNumberLastValueColumn() != null) {
//			lastValueClumnName = importContext.getNumberLastValueColumn();
//		}
		else if (this.getLastValueVarName() != null) {
			lastValueClumnName =  getLastValueVarName();
		}

		if (lastValueClumnName == null){
			setIncreamentImport(false);
		}


	}

	private void initLastValueStatus(boolean update) throws Exception {
		Status currentStatus = new Status();
		currentStatus.setId(importContext.getStatusTableId());
		currentStatus.setTime(new Date().getTime());
		if(lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			if(importContext.getConfigLastValue() != null){

				if(importContext.getConfigLastValue() instanceof Date) {
					currentStatus.setLastValue(importContext.getConfigLastValue());
				}
				else if(importContext.getConfigLastValue() instanceof Long){
					currentStatus.setLastValue(new Date((Long)importContext.getConfigLastValue()));
				}
				else if(importContext.getConfigLastValue() instanceof Integer){
					currentStatus.setLastValue(new Date((Integer)importContext.getConfigLastValue()));
				}
				else{
					if(logger.isInfoEnabled()) {
						logger.info("TIMESTAMP TYPE Last Value Illegal:{}", importContext.getConfigLastValue());
					}
					throw new ESDataImportException("TIMESTAMP TYPE Last Value Illegal:"+importContext.getConfigLastValue() );
				}
			}
			else {
				currentStatus.setLastValue(initLastDate);
			}
		}
		else if(importContext.getConfigLastValue() != null){

			currentStatus.setLastValue(importContext.getConfigLastValue());
		}
		else{
			currentStatus.setLastValue(0l);
		}


		currentStatus.setLastValueType(lastValueType);
		if(!update)
			addStatus(currentStatus);
		else
			updateStatus(currentStatus);
		this.currentStatus = currentStatus;
		this.firstStatus = (Status) currentStatus.clone();
		if(logger.isInfoEnabled())
			logger.info("Init LastValue Status: {}",currentStatus.toString());
	}

	protected  void handleCompletedTasks(List<Status> completed ,boolean needSyn,long registLiveTime){


		try {
			long now = System.currentTimeMillis();
			long deletedTime = now - registLiveTime;
			for (Status status : completed) {
				File file = new File(status.getFilePath());
				if(!file.exists()) {
					long lastTime = status.getTime();
					if (lastTime <= deletedTime) {
						SQLExecutor.insertWithDBName(statusDbname, insertHistorySQL, SimpleStringUtil.getUUID(), status.getTime(),
								status.getLastValue(), status.getLastValueType(), status.getFilePath(), status.getFileId(), status.getStatus());
						SQLExecutor.deleteWithDBName(statusDbname, deleteSQL, status.getId());
					}
				}

			}
		}
		catch (Exception e){
			logger.error("handleCompletedTasks failed:"+SimpleStringUtil.object2json(completed),e);
		}


	}

	public  void handleOldedTasks(List<Status> olded ){
		for (Status status : olded) {
			handleOldedTask(status );
		}
	}
	public  void handleOldedTask(Status olded ){

//		String updateStatusSQL = new StringBuilder().append("update ")
//				.append(statusTableName).append(" set status = ?, lasttime= ?").append(" where id=?").toString();

		try {
			olded.setTime(System.currentTimeMillis());
			olded.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
			SQLExecutor.updateWithDBName(statusDbname, updateStatusSQL, olded.getStatus(), olded.getTime(),olded.getId());
		}
		catch (Exception e){
			logger.error("handleCompletedTasks failed:"+SimpleStringUtil.object2json(olded),e);
		}


	}
	
	protected void loadCurrentStatus(){
		try {
			/**
			 * 初始化数据检索起始状态信息
			 */
			currentStatus = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectSQL, importContext.getStatusTableId());
			if (currentStatus == null) {
				initLastValueStatus(false);
			} else {
				if (importContext.isFromFirst()) {
					initLastValueStatus(true);
				}
				else if(currentStatus.getLastValueType() != this.lastValueType){ //如果当前lastValueType和作业配置的类型不一致，按照配置了类型重置当前类型
					if(logger.isWarnEnabled()){
						logger.warn("The config lastValueType is {} but from currentStatus lastValueType is {},and use the config lastValueType to releace currentStatus lastValueType.",lastValueType,currentStatus.getLastValueType());
					}
					initLastValueStatus(true);
				}
				else {
					if(currentStatus.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE){
						Object lastValue = currentStatus.getLastValue();
						if(lastValue instanceof Long){
							currentStatus.setLastValue(new Date((Long)lastValue));
						}
						else if(lastValue instanceof Integer){
							currentStatus.setLastValue(new Date(((Integer) lastValue).longValue()));
						}
						else{
							if(logger.isWarnEnabled())
								logger.warn("initTableAndStatus：增量字段类型为日期类型, But the LastValue from status table is not a long value:{},value type is {}",lastValue,lastValue.getClass().getName());
							throw new ESDataImportException("InitTableAndStatus：增量字段类型为日期类型, But the LastValue from status table is not a long value:"+lastValue+",value type is "+lastValue.getClass().getName());
						}
					}
					this.firstStatus = (Status) currentStatus.clone();
				}
			}
		} catch (Exception e) {
			throw new ESDataImportException(e);
		}
	}

	protected void initTableAndStatus(){
		if(this.isIncreamentImport()) {
			try {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				initLastDate = dateFormat.parse("1970-01-01 00:00:00");
				SQLExecutor.queryObjectWithDBName(int.class, statusDbname, existSQL);

			} catch (Exception e) {
				String tsql = createStatusTableSQL;
				if(logger.isInfoEnabled())
					logger.info( "{} table not exist，{}：{}.",statusTableName,statusTableName,tsql);
				try {
					SQLExecutor.updateWithDBName(statusDbname, tsql);
					if(logger.isInfoEnabled())
						logger.info("table " + statusTableName + " create success：" + tsql + ".");

				} catch (Exception e1) {
					if(logger.isInfoEnabled())
						logger.info("table " + statusTableName + " create success：" + tsql + ".", e1);
					throw new ESDataImportException(e1);

				}
				try {
					SQLExecutor.updateWithDBName(statusDbname, createHistoryStatusTableSQL);
					if(logger.isInfoEnabled())
						logger.info("table " + historyStatusTableName + " create success：" + createHistoryStatusTableSQL + ".");

				} catch (Exception e1) {
					if(logger.isInfoEnabled())
						logger.info("table " + historyStatusTableName + " create success：" + createHistoryStatusTableSQL + ".", e1);
					throw new ESDataImportException(e1);

				}
			}
			_initStatusManager();
			this.loadCurrentStatus();
		}
		else{

			try {
				Status currentStatus = new Status();
				currentStatus.setId(importContext.getStatusTableId());
				currentStatus.setTime(new Date().getTime());
				this.firstStatus = (Status) currentStatus.clone();
				this.currentStatus = currentStatus;
			}
			catch (Exception e){
				throw new ESDataImportException(e);
			}


		}
	}

	protected void initStatusStore(){
		if(this.isIncreamentImport()) {
			statusTableName = importContext.getLastValueStoreTableName();
			if (statusTableName == null) {
				statusTableName = "increament_tab";
			}
			if (importContext.getLastValueStorePath() == null || importContext.getLastValueStorePath().equals("")) {
				statusStorePath = "StatusStoreDB";
			} else {
				statusStorePath = importContext.getLastValueStorePath();
			}
			historyStatusTableName = statusTableName + "_his";
		}



//		if(this.esjdbc.getImportIncreamentConfig().getDateLastValueColumn() == null
//				&& this.esjdbc.getImportIncreamentConfig().getNumberLastValueColumn() == null
//				)
//			throw new ESDataImportException("Must set dateLastValueColumn or numberLastValueColumn by ImportBuilder.");

	}

	/**
	 * 初始化增量采集数据状态保存数据源
	 */
	protected void initDatasource()  {
		if(this.isIncreamentImport()) {

			if(importContext.getStatusDbConfig() == null) {
				statusDbname =  "_status_datasource";
				String dbJNDIName ="_status_datasource_jndi";
				try {
					createStatusTableSQL = new StringBuilder().append("create table " ).append( statusTableName)
							.append( " (ID number(10),")  //记录标识
							.append( "lasttime number(10),") //最后更新时间
							.append( "lastvalue number(10),")  //增量字段值，值可能是日期类型，也可能是数字类型
							.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500) ,")  //日志文件路径
							.append( "fileId varchar(500) ,")  //日志文件indoe标识
							.append( "PRIMARY KEY (ID))").toString();
					createHistoryStatusTableSQL = new StringBuilder().append("create table " ).append( historyStatusTableName)
							.append( " (ID varchar(100),")  //记录标识
							.append( "lasttime number(10),") //最后更新时间
							.append( "lastvalue number(10),")  //增量字段值，值可能是日期类型，也可能是数字类型
							.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
							.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
							.append( "filePath varchar(500) ,")  //日志文件路径
							.append( "fileId varchar(500) ,")  //日志文件indoe标识
							.append( "statusId number(10) ,")  //状态表中使用的主键标识
							.append( "PRIMARY KEY (ID))").toString();
					File dbpath = new File(statusStorePath);
					logger.info("initDatasource dbpath:" + dbpath.getCanonicalPath());
					DBConf tempConf = new DBConf();
					tempConf.setPoolname(statusDbname);
					tempConf.setDriver("org.sqlite.JDBC");
					tempConf.setJdbcurl("jdbc:sqlite://" + dbpath.getCanonicalPath());
					tempConf.setUsername("root");
					tempConf.setPassword("root");
					tempConf.setReadOnly((String)null);
					tempConf.setTxIsolationLevel((String)null);
					tempConf.setValidationQuery("select 1");
					tempConf.setJndiName(dbJNDIName);
					tempConf.setInitialConnections(1);
					tempConf.setMinimumSize(1);
					tempConf.setMaximumSize(1);
					tempConf.setUsepool(true);
					tempConf.setExternal(false);
					tempConf.setExternaljndiName((String)null);
					tempConf.setShowsql(false);
					tempConf.setEncryptdbinfo(false);
					tempConf.setQueryfetchsize(null);
					SQLUtil.startPoolWithDBConf(tempConf);
					JDBCPool jdbcPool = SQLUtil.getSQLManager().getPool(tempConf.getPoolname(),false);
					if(jdbcPool == null){
						throw new ESDataImportException("status_datasource["+statusDbname+"] not started.");
					}
				} catch (Exception e) {
					throw new ESDataImportException(e);
				}

			}
			else{
				DBConfig statusDBConfig = importContext.getStatusDbConfig();

				statusDbname = importContext.getStatusDbConfig().getDbName();
				if(statusDbname == null || statusDbname.trim().equals(""))
					statusDbname =  "_status_datasource";

				if(statusDBConfig.getDbDriver() != null && !statusDBConfig.getDbDriver().trim().equals("")){
					String dbJNDIName = statusDbname+"_jndi";
					try {

//						SQLUtil.startPool(statusDbname,
//								statusDBConfig.getDbDriver(),
//								statusDBConfig.getDbUrl(),
//								statusDBConfig.getDbUser(), statusDBConfig.getDbPassword(),
//								null,//"false",
//								null,// "READ_UNCOMMITTED",
//								statusDBConfig.getValidateSQL(),
//								dbJNDIName,
//								10,
//								10,
//								20,
//								true,
//								false,
//								null, false, false
//						);

						DBConf tempConf = new DBConf();
						tempConf.setPoolname(statusDbname);
						tempConf.setDriver(statusDBConfig.getDbDriver());
						tempConf.setJdbcurl(statusDBConfig.getDbUrl());
						tempConf.setUsername(statusDBConfig.getDbUser());
						tempConf.setPassword(statusDBConfig.getDbPassword());
						tempConf.setReadOnly((String)null);
						tempConf.setTxIsolationLevel((String)null);
						tempConf.setValidationQuery(statusDBConfig.getValidateSQL());
						tempConf.setJndiName(dbJNDIName);
						tempConf.setInitialConnections(10);
						tempConf.setMinimumSize(10);
						tempConf.setMaximumSize(20);
						tempConf.setUsepool(true);
						tempConf.setExternal(false);
						tempConf.setExternaljndiName((String)null);
						tempConf.setShowsql(false);
						tempConf.setEncryptdbinfo(false);
						tempConf.setQueryfetchsize(null);
						tempConf.setDbInfoEncryptClass(statusDBConfig.getDbInfoEncryptClass());
						SQLUtil.startPoolWithDBConf(tempConf);
						JDBCPool jdbcPool = SQLUtil.getSQLManager().getPool(tempConf.getPoolname(),false);
						if(jdbcPool == null){
							throw new ESDataImportException("status_datasource["+statusDbname+"] not started.");
						}
					} catch (Exception e) {
						throw new ESDataImportException(e);
					}

				}

				createStatusTableSQL = statusDBConfig.getStatusTableDML();
				if(createStatusTableSQL == null){
					createStatusTableSQL = statusDBConfig.getCreateStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
				}
				createHistoryStatusTableSQL = statusDBConfig.getCreateHistoryStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
				createStatusTableSQL = createStatusTableSQL.replace("$statusTableName",statusTableName);
				createHistoryStatusTableSQL = createHistoryStatusTableSQL.replace("$historyStatusTableName",historyStatusTableName);
			}
			if (importContext.getLastValueType() != null) {
				this.lastValueType = importContext.getLastValueType();
			}
//			else if (importContext.getDateLastValueColumn() != null) {
//				this.lastValueType = ImportIncreamentConfig.TIMESTAMP_TYPE;
//			} else if (importContext.getNumberLastValueColumn() != null) {
//				this.lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
//
//			}
			else {
				this.lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
			}
			/**
			 * 回填值类型
			 */
			importContext.setLastValueType(this.lastValueType);


			existSQL = new StringBuilder().append("select 1 from ").append(statusTableName).toString();
			selectSQL = new StringBuilder().append("select id,lasttime,lastvalue,lastvaluetype,filePath,fileId,status from ")
					.append(statusTableName).append(" where id=?").toString();


			selectAllSQL =  new StringBuilder().append("select id,lasttime,lastvalue,lastvaluetype,filePath,fileId,status from ")
					.append(statusTableName).toString();
			updateSQL = new StringBuilder().append("update ").append(statusTableName)
					.append(" set lasttime = ?,lastvalue = ? ,lastvaluetype= ? , filePath = ?,fileId = ? ,status = ? where id=?").toString();
			updateStatusSQL = new StringBuilder().append("update ")
					.append(statusTableName).append(" set status = ?, lasttime= ?").append(" where id=?").toString();
			insertSQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append(" (id,lasttime,lastvalue,lastvaluetype,filePath,fileId,status) values(?,?,?,?,?,?,?)").toString();
			deleteSQL = new StringBuilder().append("delete from ")
					.append(statusTableName).append(" where id=?").toString();
			insertHistorySQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append(" (id,lasttime,lastvalue,lastvaluetype,filePath,fileId,status) values(?,?,?,?,?,?,?)").toString();
		}
	}

	public void setIncreamentImport(boolean increamentImport) {
		this.increamentImport = increamentImport;
	}
	public boolean isIncreamentImport() {
		return increamentImport;
	}
	public Status getCurrentStatus(){
		return this.currentStatus;
	}
	@Override
	public void flushLastValue(Object lastValue,Status currentStatus,boolean reachEOFClosed) {
		if(lastValue != null) {
			synchronized (currentStatus) {
				Object oldLastValue = currentStatus.getLastValue();
				if (!importContext.needUpdate(oldLastValue, lastValue))
					return;
				long time = System.currentTimeMillis();
				currentStatus.setTime(time);

				currentStatus.setLastValue(lastValue);
				if(reachEOFClosed){
					currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
				}


				if (this.isIncreamentImport()) {
					Status status = currentStatus.copy();
//					Status temp = new Status();
//					temp.setTime(time);
//					temp.setId(this.currentStatus.getId());
//					temp.setLastValueType(this.currentStatus.getLastValueType());
//					temp.setLastValue(lastValue);
					this.storeStatus(status);
				}
			}
		}
	}

	@Override
	public void forceflushLastValue(Status currentStatus) {
		synchronized (currentStatus) {
			currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
			currentStatus.setTime(System.currentTimeMillis());
			this.storeStatus(currentStatus);
		}

	}
	public void storeStatus(Status currentStatus)  {

		try {
			updateStatus(currentStatus);
		}
		catch (ESDataImportException e) {
			throw e;
		}
		catch (Exception e) {
			throw new ESDataImportException(e);
		}

	}
	public void addStatus(Status currentStatus) throws ESDataImportException {
//		Object lastValue = !importContext.isLastValueDateType()?currentStatus.getLastValue():((Date)currentStatus.getLastValue()).getTime();
		Object lastValue = currentStatus.getLastValue();
		if(logger.isInfoEnabled()){
			logger.info("AddStatus: 增量字段值 LastValue is Date Type:{},real data type is {},real last value is {}",importContext.isLastValueDateType(),
					lastValue.getClass().getName(),lastValue);
		}

		if(importContext.isLastValueDateType()){
			if(lastValue instanceof Date) {
				lastValue = ((Date) lastValue).getTime();

			}
			else{
				throw new ESDataImportException("AddStatus: 增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
			}
		}
		if(logger.isInfoEnabled()){
			logger.info("AddStatus: 增量字段值 LastValue is Date Type:{},real data type is {},and real last value to sqlite is {}",importContext.isLastValueDateType(),
					lastValue.getClass().getName(),lastValue);
		}

		try {
			SQLExecutor.insertWithDBName(statusDbname,insertSQL,currentStatus.getId(),currentStatus.getTime(),lastValue,lastValueType,currentStatus.getFilePath(),currentStatus.getFileId(),currentStatus.getStatus());
		} catch (SQLException throwables) {
			throw new ESDataImportException("Add Status failed:"+currentStatus.toString(),throwables);
		}
	}
	public void updateStatus(Status currentStatus) throws Exception {
		Object lastValue = currentStatus.getLastValue();
		if(logger.isDebugEnabled()){
			logger.debug("UpdateStatus：增量字段值 LastValue is Date Type:{},real data type is {},real last value is {}",importContext.isLastValueDateType(),
					lastValue.getClass().getName(),lastValue);
		}

		if(importContext.isLastValueDateType()){
			if(lastValue instanceof Date) {
				lastValue = ((Date) lastValue).getTime();
			}
			else{
				throw new ESDataImportException("UpdateStatus：增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
			}
		}
		if(logger.isDebugEnabled()){
			logger.debug("UpdateStatus：增量字段值 LastValue is Date Type:{},real data type is {},and real last value to sqlite is {}",importContext.isLastValueDateType(),
					lastValue.getClass().getName(),lastValue);
		}
//		SQLExecutor.updateWithDBName(statusDbname,updateSQL, currentStatus.getTime(), lastValue,
//									lastValueType,currentStatus.getFilePath(),currentStatus.getFileId(),
//									currentStatus.getStatus(),currentStatus.getId());
		if(!statusManager.isStoped()) {
			statusManager.putStatus(currentStatus);
		}
	}





	public ScheduleService getScheduleService(){
		return this.scheduleService;
	}


//	public Object getLastValue() throws ESDataImportException {
//
//
//			if(getLastValueClumnName() == null){
//				return null;
//			}
//
////			if (this.importIncreamentConfig.getDateLastValueColumn() != null) {
////				return this.getValue(this.importIncreamentConfig.getDateLastValueColumn());
////			} else if (this.importIncreamentConfig.getNumberLastValueColumn() != null) {
////				return this.getValue(this.importIncreamentConfig.getNumberLastValueColumn());
////			}
////			else if (this.dataTranPlugin.getSqlInfo().getLastValueVarName() != null) {
////				return this.getValue(this.dataTranPlugin.getSqlInfo().getLastValueVarName());
////			}
//			if(this.getLastValueType() == null || this.getLastValueType().intValue() ==  ImportIncreamentConfig.NUMBER_TYPE)
//				return this.getValue(getLastValueClumnName());
//			else if(this.getLastValueType().intValue() ==  ImportIncreamentConfig.TIMESTAMP_TYPE){
//				return this.getDateTimeValue(getLastValueClumnName());
//			}
//			return null;
//
//
//	}



	//	private String indexType;
	private TranErrorWrapper errorWrapper;
	public TranErrorWrapper getErrorWrapper() {
		return errorWrapper;
	}

	public void setErrorWrapper(TranErrorWrapper errorWrapper) {
		this.errorWrapper = errorWrapper;
	}


	private volatile boolean forceStop = false;
	public void setForceStop(){
		this.forceStop = true;
	}
	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	public boolean assertCondition(){
		if(forceStop)
			return false;
		if(errorWrapper != null)
			return errorWrapper.assertCondition();
		return true;
	}

	/**
	 * 判断执行条件是否成立，成立返回true，否则返回false
	 * @return
	 */
	public boolean assertCondition(Exception e){
		if(errorWrapper != null)
			return errorWrapper.assertCondition(e);
		return true;
	}
	protected void initES(String applicationPropertiesFile){
		if(SimpleStringUtil.isNotEmpty(applicationPropertiesFile ))
			ElasticSearchBoot.boot(applicationPropertiesFile);
		if(this.importContext.getESConfig() != null){
			ElasticSearchBoot.boot(importContext.getESConfig().getConfigs());
		}
	}

	public void initSchedule(){
		if(importContext.getScheduleConfig() != null) {
			this.scheduleService = new ScheduleService();
			this.scheduleService.init(importContext,targetImportContext);
		}
	}

	protected void initDS(DBConfig dbConfig){
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbDriver()) && SimpleStringUtil.isNotEmpty(dbConfig.getDbUrl())) {
			DBConf temConf = new DBConf();
			temConf.setPoolname(dbConfig.getDbName());
			temConf.setDriver(dbConfig.getDbDriver());
			temConf.setJdbcurl(dbConfig.getDbUrl());
			temConf.setUsername(dbConfig.getDbUser());
			temConf.setPassword(dbConfig.getDbPassword());
			temConf.setReadOnly(null);
			temConf.setTxIsolationLevel(null);
			temConf.setValidationQuery(dbConfig.getValidateSQL());
			temConf.setJndiName(dbConfig.getDbName()+"_jndi");
			temConf.setInitialConnections(dbConfig.getInitSize());
			temConf.setMinimumSize(dbConfig.getMinIdleSize());
			temConf.setMaximumSize(dbConfig.getMaxSize());
			temConf.setUsepool(dbConfig.isUsePool());
			temConf.setExternal(false);
			temConf.setExternaljndiName(null);
			temConf.setShowsql(dbConfig.isShowSql());
			temConf.setEncryptdbinfo(false);
			temConf.setQueryfetchsize(dbConfig.getJdbcFetchSize() == null?0:dbConfig.getJdbcFetchSize());
			temConf.setDbAdaptor(dbConfig.getDbAdaptor());
			temConf.setDbtype(dbConfig.getDbtype());
			temConf.setColumnLableUpperCase(dbConfig.isColumnLableUpperCase());
			temConf.setDbInfoEncryptClass(dbConfig.getDbInfoEncryptClass());
			SQLManager.startPool(temConf);
			/**
			SQLUtil.startPool(dbConfig.getDbName(),//数据源名称
					dbConfig.getDbDriver(),//oracle驱动
					dbConfig.getDbUrl(),//mysql链接串
					dbConfig.getDbUser(), dbConfig.getDbPassword(),//数据库账号和口令
					null,//"false",
					null,// "READ_UNCOMMITTED",
					dbConfig.getValidateSQL(),//数据库连接校验sql
					dbConfig.getDbName()+"_jndi",
					dbConfig.getInitSize(),
					dbConfig.getMinIdleSize(),
					dbConfig.getMaxSize(),
					dbConfig.isUsePool(),
					false,
					null, dbConfig.isShowSql(), false,dbConfig.getJdbcFetchSize() == null?0:dbConfig.getJdbcFetchSize(),dbConfig.getDbtype(),dbConfig.getDbAdaptor()
			);*/
		}
	}
	protected void initOtherDSes(List<DBConfig> dbConfigs){
		if(dbConfigs != null && dbConfigs.size() > 0){
			for (DBConfig dbConfig:dbConfigs){
				initDS( dbConfig);
			}
		}
	}

	protected void stopDS(DBConfig dbConfig){
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbDriver()) && SimpleStringUtil.isNotEmpty(dbConfig.getDbUrl())){
			try {
				SQLUtil.stopPool(dbConfig.getDbName());
			} catch (Exception e) {
				if(logger.isErrorEnabled())
					logger.error("SQLUtil.stopPool("+dbConfig.getDbName()+") failed:",e);
			}
		}
	}

	protected void stopOtherDSES(List<DBConfig> dbConfigs){

		if(dbConfigs != null && dbConfigs.size() > 0){
			for(DBConfig dbConfig:dbConfigs){
				stopDS(dbConfig);
			}
		}
	}

}
