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
import com.frameworkset.common.poolman.util.DBStartResult;
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.*;
import org.frameworkset.tran.status.DefaultStatusManager;
import org.frameworkset.tran.status.SingleStatusManager;
import org.frameworkset.tran.status.StatusManager;
import org.frameworkset.tran.util.TranConstant;
import org.frameworkset.tran.util.TranUtil;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
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
public class DataTranPluginImpl implements DataTranPlugin {
	protected static Logger logger = LoggerFactory.getLogger(DataTranPluginImpl.class);
	private boolean increamentImport = true;
	protected InputPlugin inputPlugin ;
	protected OutputPlugin outputPlugin;
	private ExportCount exportCount;
	protected StatusManager statusManager;
	protected ScheduleAssert scheduleAssert;
	/**
	 * 包含所有启动成功的db数据源
	 */
	protected DBStartResult dbStartResult = new DBStartResult();
	public ExportCount getExportCount() {
		return exportCount;
	}
	@Override
	public boolean useFilePointer(){
		return false;
	}
	public InputPlugin getInputPlugin() {
		return inputPlugin;
	}

	public OutputPlugin getOutputPlugin() {
		return outputPlugin;
	}

	@Override
	public ScheduleAssert getScheduleAssert() {
		return scheduleAssert;
	}

	public void setScheduleAssert(ScheduleAssert scheduleAssert){
		this.scheduleAssert = scheduleAssert;
	}
	public Map getJobParams() {
		Map _params = importContext.getParams();
		Map params = new HashMap();
		if (_params != null && _params.size() > 0) {
			params.putAll(_params);
		}
		return params;
	}
	public boolean isSchedulePaussed(boolean autoPause){
		if(this.scheduleAssert != null)
			return !this.scheduleAssert.assertSchedule(  autoPause);
		return false;
	}
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet,CountDownLatch countDownLatch,Status currentStatus){
		return this.outputPlugin.createBaseDataTran(taskContext,tranResultSet,countDownLatch,currentStatus);
	}

	@Override
	public void doImportData(TaskContext taskContext) {
		this.inputPlugin.doImportData(taskContext);
	}


	/**
	 * 识别任务是否已经完成
	 * @param status
	 * @return
	 */
	public boolean isComplete(Status status){
		return status.getStatus() == ImportIncreamentConfig.STATUS_COMPLETE;
	}
	@Override
	public Context buildContext(TaskContext taskContext,TranResultSet tranResultSet, BatchContext batchContext){
//		return new ContextImpl(  taskContext,importContext, tranResultSet, batchContext);
		return inputPlugin.buildContext(taskContext,tranResultSet,batchContext);
	}
	@Override
	public String getLastValueVarName() {
//		return importContext.getLastValueColumn();
		return inputPlugin.getLastValueVarName();
	}

	public Long getTimeRangeLastValue(){
		return inputPlugin.getTimeRangeLastValue();
	}
	public DataTranPluginImpl(ImportContext importContext){
		this.importContext = importContext;
		importContext.setDataTranPlugin(this);



//		init(importContext,targetImportContext);


	}

	@Override
	public ImportContext getImportContext() {
		return importContext;
	}

	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}

	protected ImportContext importContext;


	protected volatile Status currentStatus;
	protected volatile Status firstStatus;
	protected String updateSQL ;
	protected String updateStatusSQL;
	protected String insertSQL;
	protected String insertHistorySQL;
	protected String createStatusTableSQL;
	protected String createHistoryStatusTableSQL;
	protected String selectSQL;
	protected String checkFieldSQL ;
	protected String checkHisFieldSQL ;
	protected String deleteSQL;
	protected String selectAllSQL;
	protected String existSQL;
	protected String existHisSQL;
	protected int lastValueType = ImportIncreamentConfig.NUMBER_TYPE;

	protected Date initLastDate = null;
	protected String statusDbname;
	protected boolean useOuterStatusDb = false;
	protected String statusTableName;
	protected String historyStatusTableName;
	protected String statusStorePath;
	protected String lastValueClumnName;
	protected ScheduleService scheduleService;
	protected boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}
//	protected boolean enablePluginTaskIntercept = true;

//	public void setEnablePluginTaskIntercept(boolean enablePluginTaskIntercept) {
//		this.enablePluginTaskIntercept = enablePluginTaskIntercept;
//	}
//
//	public boolean isEnablePluginTaskIntercept() {
//		return enablePluginTaskIntercept;
//	}

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
		TranUtil.initTaskContextSQLInfo(taskContext, importContext);

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
	public boolean isEnableAutoPauseScheduled(){
		return true;
	}
	@Override
	public void startAction(){
		if(this.importContext.getImportStartAction() != null){
			try {
				this.importContext.getImportStartAction().startAction(importContext);
			}
			catch (Exception e){
				logger.warn("",e);
			}
		}
	}
	@Override
	public void importData() throws DataImportException {

		if(this.scheduleService == null) {//一次性执行数据导入操作

			long importStartTime = System.currentTimeMillis();

			TaskContext taskContext = inputPlugin.isEnablePluginTaskIntercept()?new TaskContext(importContext):null;
			try {
				if(inputPlugin.isEnablePluginTaskIntercept())
					preCall(taskContext);
//				this.doImportData(taskContext);
				this.inputPlugin.doImportData(taskContext);
				if(inputPlugin.isEnablePluginTaskIntercept())
					afterCall(taskContext);
				long importEndTime = System.currentTimeMillis();
				if( isPrintTaskLog())
					logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
			}
			catch (Exception e){
				if(inputPlugin.isEnablePluginTaskIntercept())
					throwException(taskContext,e);
				logger.error("scheduleImportData failed:",e);
			}

		}
		else{//定时增量导入数据操作
			try {
				if (!this.importContext.isExternalTimer()) {//内部定时任务引擎
					scheduleService.timeSchedule( );
				} else { //外部定时任务引擎执行的方法，比如quartz之类的
					if(scheduleService.isSchedulePaused(isEnableAutoPauseScheduled())){
						if(logger.isInfoEnabled()){
							logger.info("Ignore  Paussed Schedule Task,waiting for next resume schedule sign to continue.");
						}
						return;
					}
					scheduleService.externalTimeSchedule();

				}
			}
			catch (DataImportException e)
			{
				throw e;
			}
			catch (Exception e)
			{
				throw new DataImportException(e);
			}
		}

	}




	public  void beforeInit(){
//		initOtherDSes(importContext.getConfigs());
		this.inputPlugin.beforeInit();
		this.outputPlugin.beforeInit();
	}
//	public abstract void afterInit();
//	public abstract void initStatusTableId();
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
			statusManager.init();
		}
	}
	protected boolean initOtherDSes ;
	protected boolean initDefaultDS;
	public void initDefaultDS(){
		if(initDefaultDS )
			return;
		try {
			DBConfig dbConfig = importContext.getDefaultDBConfig();
			if (dbConfig != null ) {
				initDS(dbStartResult,dbConfig);
			}
		}
		finally {
			initDefaultDS = true;
		}
	}
	public void initOtherDSes(){
		if(initOtherDSes )
			return;
		try {
			List<DBConfig> dbConfigs = importContext.getOhterDBConfigs();
			if (dbConfigs != null && dbConfigs.size() > 0) {
				for (DBConfig dbConfig : dbConfigs) {
					initDS(dbStartResult,dbConfig);
				}
			}
		}
		finally {
			initOtherDSes = true;
		}
	}

	public static void initDS(DBStartResult dbStartResult,DBConfig dbConfig){
		if(dbConfig != null && SimpleStringUtil.isNotEmpty(dbConfig.getDbName())
				&& SimpleStringUtil.isNotEmpty(dbConfig.getDbDriver())
				&& SimpleStringUtil.isNotEmpty(dbConfig.getDbUrl()) && !dbStartResult.contain(dbConfig.getDbName())) {
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
			temConf.setQueryfetchsize(dbConfig.getJdbcFetchSize() == null?null:dbConfig.getJdbcFetchSize());
			temConf.setDbAdaptor(dbConfig.getDbAdaptor());
			temConf.setDbtype(dbConfig.getDbtype());
			temConf.setColumnLableUpperCase(dbConfig.isColumnLableUpperCase());
			temConf.setDbInfoEncryptClass(dbConfig.getDbInfoEncryptClass());
			boolean ret = SQLManager.startPool(temConf);
			if(ret){
				dbStartResult.addDBStartResult(temConf.getPoolname());
			}

		}
	}

	@Override
	public void init(ImportContext importContext) {

		this.importContext = importContext;

		exportCount = new ExportCount();
		this.inputPlugin = importContext.getInputPlugin();
		this.outputPlugin = importContext.getOutputPlugin();
		inputPlugin.setDataTranPlugin(this);
		outputPlugin.setDataTranPlugin(this);
		initDefaultDS();
		initOtherDSes();
		beforeInit();
		this.inputPlugin.init();
		this.outputPlugin.init();

		this.initSchedule();
		initLastValueClumnName();
		initStatusStore();
		initDatasource();
		if(this.isIncreamentImport() && this.importContext.getStatusTableId() == null) {
			inputPlugin.initStatusTableId();
		}
		initTableAndStatus();
		inputPlugin.afterInit();
		outputPlugin.afterInit();
	}
	public boolean isMultiTran(){
		return inputPlugin.isMultiTran();
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

	public static void stopDatasources(DBStartResult dbStartResult){
		if(dbStartResult != null ){
			Map<String,Object> dbs = dbStartResult.getDbstartResult();
			if(dbs != null && dbs.size() > 0){
				Iterator<Map.Entry<String, Object>> iterator = dbs.entrySet().iterator();
				while(iterator.hasNext()){
					Map.Entry<String, Object> entry = iterator.next();

					String db = entry.getKey();
					try {
						SQLUtil.stopPool(db);
					} catch (Exception e) {
						if(logger.isErrorEnabled())
							logger.error("SQLUtil.stopPool("+db+") failed:",e);
					}
				}
			}
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
			if(statusDbname != null && !statusDbname.equals("")) {
				//如果使用的不是外部数据源，那么就需要停止数据源
				if(!useOuterStatusDb) {
					SQLUtil.stopPool(this.statusDbname);
				}
			}
		}
		catch (Exception e){
			logger.error("Stop status db pool["+statusDbname+"] failed:",e);
		}
//		this.stopDS(importContext.getDbConfig());
//		this.stopOtherDSES(importContext.getConfigs());
		stopDatasources(dbStartResult);
		inputPlugin.destroy(waitTranStop);
		outputPlugin.destroy(waitTranStop);

	}


	protected Object formatLastDateValue(Date date){

		return date;


	}
	@Override
	public Object[] putLastParamValue(Map params){
		Object[] ret = new Object[2];
		Object lastValue = this.currentStatus.getLastValue();
		if(this.lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			params.put(getLastValueVarName(), lastValue);


		}
		else{
			Date ldate = null;
			if(lastValue instanceof Date) {
				ldate = (Date)lastValue;


			}
			else {
				if(lastValue instanceof Long) {
					ldate = new Date((Long)lastValue);
				}
				else if(lastValue instanceof Integer){
					ldate = new Date(((Integer) lastValue).longValue());
				}
				else if(lastValue instanceof Short){
					ldate = new Date(((Short) lastValue).longValue());
				}
				else{
					ldate = new Date(((Number) lastValue).longValue());
				}
			}
			params.put(getLastValueVarName(), formatLastDateValue(ldate));

			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				ret[1] = lastOffsetValue;
				params.put(getLastValueVarName()+"__endTime", formatLastDateValue(lastOffsetValue));
			}
		}
		if(isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(params).toString());
		}
		ret[0] = lastValue;
		return ret;
	}



	public Map getParamValue(Map params){
		Object lastValue = this.currentStatus.getLastValue();
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
					throw new DataImportException("TIMESTAMP TYPE Last Value Illegal:"+importContext.getConfigLastValue() );
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
								status.getLastValue(), status.getLastValueType(), status.getFilePath(), status.getRelativeParentDir(),status.getFileId(), status.getStatus());
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

	/**
	 * 创建字段
	 * @param field
	 * @param tableName
	 * @param defaultValue
	 * @param length
	 * @param type
	 */
	private void addField(String field,String tableName,String defaultValue,String length,String type){
		String addFiledSQL = defaultValue !=null ?
				"ALTER TABLE "+tableName+" ADD "+field+" "+type+"("+length+") DEFAULT "+ defaultValue:
				"ALTER TABLE "+tableName+" ADD "+field+" "+type+"("+length+") ";

		try {
			SQLExecutor.updateWithDBName(statusDbname, addFiledSQL);
			if(logger.isInfoEnabled())
				logger.info("add field to table success：" + addFiledSQL + ".");

		} catch (Exception e1) {
			if(logger.isWarnEnabled())
				logger.warn("add field to table failed：" + addFiledSQL + ".", e1);
//			throw new ESDataImportException("add field to table failed：" + addFiledSQL + ".",e1);

		}
	}
	/**
	 * 检查状态表字段是否存在，不存在则创建
	 */
	private void checkStatusFieldExist()  {
		String defaultValue = DBConfig.getStatusTableDefaultValue(SQLUtil.getPool(statusDbname).getDBType());
		String type = DBConfig.getStatusTableType(SQLUtil.getPool(statusDbname).getDBType());
		try {
			SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, checkFieldSQL);

		}
		catch (SQLException e){
			logger.warn("filePath,status and fileId not exit in table {"+statusTableName+"}",e);

			addField("filePath",statusTableName,defaultValue,"500",type);
			addField("relativeParentDir",statusTableName,defaultValue,"500",type);
			addField("fileId",statusTableName,defaultValue,"500",type);
			addField("status",statusTableName,null,"1",DBConfig.getStatusTableTypeNumber(SQLUtil.getPool(statusDbname).getDBType()));

		}



	}

	/**
	 * 检查历史状态表字段是否存在，不存在则创建
	 */
	private void checkHisStatusFieldExist()  {
		String defaultValue = DBConfig.getStatusTableDefaultValue(SQLUtil.getPool(statusDbname).getDBType());
		String type = DBConfig.getStatusTableType(SQLUtil.getPool(statusDbname).getDBType());


		try {
			SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, checkHisFieldSQL);

		}
		catch (SQLException e){
			logger.warn("filePath,status,statusId and fileId not exit in table {"+historyStatusTableName+"}",e);

			addField("filePath",historyStatusTableName,defaultValue,"500",type);
			addField("relativeParentDir",historyStatusTableName,defaultValue,"500",type);
			addField("fileId",historyStatusTableName,defaultValue,"500",type);
			addField("status",historyStatusTableName,null,"1",DBConfig.getStatusTableTypeNumber(SQLUtil.getPool(statusDbname).getDBType()));
			addField("statusId",historyStatusTableName,null,"10",DBConfig.getStatusTableTypeBigNumber(SQLUtil.getPool(statusDbname).getDBType()));
		}


	}

	public int getLastValueType() {
		return lastValueType;
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
							throw new DataImportException("InitTableAndStatus：增量字段类型为日期类型, But the LastValue from status table is not a long value:"+lastValue+",value type is "+lastValue.getClass().getName());
						}
					}
					this.firstStatus = (Status) currentStatus.clone();
				}
			}
		} catch (Exception e) {
			throw new DataImportException(e);
		}
	}

	private void createTable(String tableName,String sql){
		try {
			SQLExecutor.updateWithDBName(statusDbname, sql);
			if(logger.isInfoEnabled())
				logger.info("table " + tableName + " create success：" + sql + ".");

		} catch (Exception e1) {
			if(logger.isInfoEnabled())
				logger.info("table " + tableName + " create failed：" + sql + ".", e1);
			throw new DataImportException(e1);

		}
	}
	protected void initTableAndStatus(){
		if(this.isIncreamentImport()) {
			try {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				initLastDate = dateFormat.parse("1970-01-01 00:00:00");
				SQLExecutor.queryObjectWithDBName(int.class, statusDbname, existSQL);
				/**
				 * 检查状态表字段是否存在，不存在则创建
				 */
				checkStatusFieldExist();
			} catch (Exception e) {

				if(logger.isInfoEnabled())
					logger.info( "{} table not exist，{}：{}.",statusTableName,statusTableName,createStatusTableSQL);
				createTable(statusTableName,createStatusTableSQL);


			}

			try {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				initLastDate = dateFormat.parse("1970-01-01 00:00:00");
				SQLExecutor.queryObjectWithDBName(int.class, statusDbname, existHisSQL);
				/**
				 * 检查历史状态表字段是否存在，不存在则创建
				 */
				checkHisStatusFieldExist();
			} catch (Exception e) {
				if(logger.isInfoEnabled())
					logger.info( "{} table not exist，{}：{}.",historyStatusTableName,statusTableName,createHistoryStatusTableSQL);
				createTable(historyStatusTableName,createHistoryStatusTableSQL);
				/**
				 try {
				 SQLExecutor.updateWithDBName(statusDbname, createHistoryStatusTableSQL);
				 if(logger.isInfoEnabled())
				 logger.info("table " + historyStatusTableName + " create success：" + createHistoryStatusTableSQL + ".");

				 } catch (Exception e1) {
				 if(logger.isInfoEnabled())
				 logger.info("table " + historyStatusTableName + " create failed：" + createHistoryStatusTableSQL + ".", e1);
				 throw new ESDataImportException(e1);

				 }*/
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
				throw new DataImportException(e);
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
				DBConfig statusDBConfig = importContext.getStatusDbConfig();
				if(statusDBConfig == null){
					statusStorePath = "StatusStoreDB";
				}
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



	private void initSQLiteStatusDB(String statusDbname,String dbJNDIName){

		try {
			createStatusTableSQL = new StringBuilder().append("create table " ).append( statusTableName)
					.append( " (ID number(10),")  //记录标识
					.append( "lasttime number(10),") //最后更新时间
					.append( "lastvalue number(10),")  //增量字段值，值可能是日期类型，也可能是数字类型
					.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
					.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
					.append( "filePath varchar(500) ,")  //日志文件路径
					.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径

					.append( "fileId varchar(500) ,")  //日志文件indoe标识
					.append( "PRIMARY KEY (ID))").toString();
			createHistoryStatusTableSQL = new StringBuilder().append("create table " ).append( historyStatusTableName)
					.append( " (ID varchar(100),")  //记录标识
					.append( "lasttime number(10),") //最后更新时间
					.append( "lastvalue number(10),")  //增量字段值，值可能是日期类型，也可能是数字类型
					.append( "lastvaluetype number(1),") //值类型 0-数字 1-日期
					.append( "status number(1) ,")  //数据采集完成状态：0-采集中  1-完成  适用于文件日志采集 默认值 0
					.append( "filePath varchar(500) ,")  //日志文件路径
					.append( "relativeParentDir varchar(500) ,")  //日志文件子目录相对路径
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
			boolean ret = SQLUtil.startPoolWithDBConf(tempConf);
//			JDBCPool jdbcPool = SQLUtil.getSQLManager().getPool(tempConf.getPoolname(),false);
			if(!ret ){
				throw new DataImportException("status_datasource["+statusDbname+"] not started.");
			}
		} catch (Exception e) {
			throw new DataImportException(e);
		}
	}
	private void initStatusSQL(DBConfig statusDBConfig ){
		createStatusTableSQL = statusDBConfig.getStatusTableDML();
		if(createStatusTableSQL == null){
			createStatusTableSQL = statusDBConfig.getCreateStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
		}
		createHistoryStatusTableSQL = statusDBConfig.getCreateHistoryStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
		createStatusTableSQL = createStatusTableSQL.replace("$statusTableName",statusTableName);
		createHistoryStatusTableSQL = createHistoryStatusTableSQL.replace("$historyStatusTableName",historyStatusTableName);
	}
	/**
	 * 初始化增量采集数据状态保存数据源
	 */
	protected void initDatasource()  {
		if(this.isIncreamentImport()) {

			if(importContext.getStatusDbConfig() == null) {
				statusDbname =  "_status_datasource";
				String dbJNDIName ="_status_datasource_jndi";
				initSQLiteStatusDB(statusDbname,dbJNDIName);

			}
			else{
				DBConfig statusDBConfig = importContext.getStatusDbConfig();

				statusDbname = statusDBConfig.getDbName();

				if(statusDBConfig.getDbDriver() != null && !statusDBConfig.getDbDriver().trim().equals("")){
					if(statusDbname == null || statusDbname.trim().equals(""))
						statusDbname =  "_status_datasource";

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
						boolean ret = SQLUtil.startPoolWithDBConf(tempConf);
//						JDBCPool jdbcPool = SQLUtil.getSQLManager().getPool(tempConf.getPoolname(),false);
						if(!ret){
							throw new DataImportException("status_datasource["+statusDbname+"] not started.");
						}
//						else{
//							dbStartResult.addDBStartResult(tempConf.getPoolname());
//						}
					} catch (Exception e) {
						throw new DataImportException(e);
					}
					initStatusSQL( statusDBConfig );
				}
				else{
					if(statusStorePath != null && !statusStorePath.equals("")){
						if(statusDbname == null || statusDbname.trim().equals(""))
							statusDbname =  "_status_datasource";
						String dbJNDIName =statusDbname+"_jndi";
						initSQLiteStatusDB(statusDbname,dbJNDIName);
					}
					else {
						useOuterStatusDb = true;
						initStatusSQL( statusDBConfig );
					}
				}


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
			existHisSQL = new StringBuilder().append("select 1 from ").append(historyStatusTableName).toString();
			selectSQL = new StringBuilder().append("select id,lasttime,lastvalue,lastvaluetype,filePath,relativeParentDir,fileId,status from ")
					.append(statusTableName).append(" where id=?").toString();
			checkFieldSQL = "select filePath,fileId,relativeParentDir,status from " + statusTableName;
			checkHisFieldSQL = "select filePath,fileId,relativeParentDir,status,statusId from " + historyStatusTableName;
			selectAllSQL =  new StringBuilder().append("select id,lasttime,lastvalue,lastvaluetype,filePath,relativeParentDir,fileId,status from ")
					.append(statusTableName).toString();
			updateSQL = new StringBuilder().append("update ").append(statusTableName)
					.append(" set lasttime = ?,lastvalue = ? ,lastvaluetype= ? , filePath = ?,relativeParentDir = ?,fileId = ? ,status = ? where id=?").toString();
			updateStatusSQL = new StringBuilder().append("update ")
					.append(statusTableName).append(" set status = ?, lasttime= ?").append(" where id=?").toString();
			insertSQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append(" (id,lasttime,lastvalue,lastvaluetype,filePath,relativeParentDir,fileId,status) values(?,?,?,?,?,?,?,?)").toString();
			deleteSQL = new StringBuilder().append("delete from ")
					.append(statusTableName).append(" where id=?").toString();
			insertHistorySQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append(" (id,lasttime,lastvalue,lastvaluetype,filePath,relativeParentDir,fileId,status) values(?,?,?,?,?,?,?,?)").toString();
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
		catch (DataImportException e) {
			throw e;
		}
		catch (Exception e) {
			throw new DataImportException(e);
		}

	}
	public void addStatus(Status currentStatus) throws DataImportException {
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
				throw new DataImportException("AddStatus: 增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
			}
		}
		if(logger.isInfoEnabled()){
			logger.info("AddStatus: 增量字段值 LastValue is Date Type:{},real data type is {},and real last value to sqlite is {}",importContext.isLastValueDateType(),
					lastValue.getClass().getName(),lastValue);
		}

		try {
			SQLExecutor.insertWithDBName(statusDbname,insertSQL,currentStatus.getId(),currentStatus.getTime(),lastValue,lastValueType,
																currentStatus.getFilePath(),currentStatus.getRelativeParentDir(),
																currentStatus.getFileId(),currentStatus.getStatus());
		} catch (SQLException throwables) {
			throw new DataImportException("Add Status failed:"+currentStatus.toString(),throwables);
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
				throw new DataImportException("UpdateStatus：增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
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


	public void initSchedule(){
		if(importContext.getScheduleConfig() != null) {
			this.scheduleService = new ScheduleService();
			scheduleService.setEnablePluginTaskIntercept(inputPlugin.isEnablePluginTaskIntercept());
			this.scheduleService.init(importContext);
		}
	}





}
