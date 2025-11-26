package org.frameworkset.tran.status;
/**
 * Copyright 2020 bboss
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
import com.frameworkset.common.poolman.util.SQLManager;
import com.frameworkset.common.poolman.util.SQLUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.frameworkset.tran.task.BaseTranJob;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/26 9:16
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseStatusManager implements StatusManager {
	private DataTranPlugin dataTranPlugin;
	private boolean increamentImport = true;
	protected ImportContext importContext;
    protected LastValueWraperSerial lastValueWraperSerial;


	protected volatile Status currentStatus;
	protected volatile Status firstStatus;
	protected String updateSQL ;
	protected String updateByJobIdSQL ;
	protected String updateStatusSQL;
	protected String updateByJobIdStatusSQL;
	protected String insertSQL;
	protected String insertHistorySQL;
	protected String createStatusTableSQL;
	protected String createHistoryStatusTableSQL;
	protected String selectSQL;
	protected String selectByJobIdSQL;
	protected String checkFieldSQL ;
	protected String checkHisFieldSQL ;
	protected String deleteSQL;
	protected String deleteByJobIdSQL;
    /**
	 * 获取采集状态表中正在采集和文件丢失的记录
	 */
	protected String selectAllSQL;

    /**
     * 获取采集状态表中fileId对应的采集已完成状态记录
     */
    protected String selectCompleteStatusByFileId;

    /**
     * 获取采集状态表中采集已完成状态记录
     */
    protected String selectCompleteStatuses;

    /**
     * 获取采集历史状态表中fileId对应的采集状态记录
     */
    protected String selectHistoryStatusByFileId;
    
    /**
     * 获取采集状态表中jobId对应作业正在采集和文件丢失的记录
     */
	protected String selectAllByJobIdSQL;

    /**
     * 获取采集状态表中jobId对应作业的fileId对应的已完成记录信息
     */
    protected String selectCompleteStatusByJobIdAndFileIdSQL;
    /**
     * 获取采集状态表中jobId对应作业的fileId对应的已完成记录信息
     */
    protected String selectCompleteStatusByJobIdSQL;

    /**
     * 获取采集历史状态表中jobId对应作业的fileId对应的记录信息
     */
    protected String selectHistoryStatusByJobIdAndFileIdSQL;
	protected String existSQL;
	protected String existHisSQL;

	protected Date initLastDate = null;
    protected LocalDateTime initLastLocalDateTime = null;
	protected String statusDbname;
	protected boolean useOuterStatusDb = false;
	protected String statusTableName;
	protected String historyStatusTableName;
	protected String statusStorePath;
    protected String statusStorePassword = "Root_123456#";
	protected String lastValueClumnName;
	private static Logger logger = LoggerFactory.getLogger(BaseStatusManager.class);
	protected int lastValueType;
	private StatusFlushThread flushThread ;
	private boolean stoped;
	public BaseStatusManager(
							 DataTranPlugin dataTranPlugin){

		this.dataTranPlugin = dataTranPlugin;
		this.importContext = dataTranPlugin.getImportContext();
        setLastValueWraperSerial();
	}
    protected void setLastValueWraperSerial(){
        this.lastValueWraperSerial = new DefaultLastValueWraperSerial();
    }

	public DataTranPlugin getDataTranPlugin() {
		return dataTranPlugin;
	}

	public void init(){

		flushThread = new StatusFlushThread(this,
				dataTranPlugin.getImportContext().getAsynFlushStatusInterval());
		flushThread.start();
		ShutdownUtil.addShutdownHook(new Runnable() {
			@Override
			public void run() {
				if(isStoped())
					return;
				synchronized(BaseStatusManager.this) {
					if(isStoped())
						return;
					flushStatus();
				}
			}
		});
	}

//	private ReadWriteLock putStatusLock = new ReentrantReadWriteLock();
//	private Lock read = putStatusLock.readLock();
//	private Lock write = putStatusLock.writeLock();
    private ReentrantLock putStatusLock = new ReentrantLock();
	protected abstract void _putStatus(Status currentStatus);

	public void putStatus(Status currentStatus) throws Exception{
//        if(this.lastValueWraperSerial != null){
//            lastValueWraperSerial.serial(currentStatus);
//        }
        putStatusLock.lock();
		try{
			_putStatus( currentStatus);
		}
		finally {
            putStatusLock.unlock();
		}
	}
	protected abstract void _flushStatus() throws Exception;
	public void flushStatus(){
        putStatusLock.lock();
		try {

			_flushStatus();
		} catch (Exception throwables) {
			logger.error("flushStatus failed:statusDbname["+statusDbname+"],updateSQL["+updateSQL+"]",throwables);
		}
		finally {
            putStatusLock.unlock();
		}
	}

	@Override
	public synchronized void stop(){
		if(stoped )
			return;
		stoped = true;
		if(flushThread != null) {
            flushThread.interrupt();
            try {
                flushThread.join();
            } catch (InterruptedException e) {

            }
        }
	}

	@Override
	public synchronized boolean isStoped() {
		return stoped;
	}

	protected Object convertLastValue(Object lastValue){
		if(lastValue == null){
			return null;
		}
		if(lastValue instanceof Date){
			lastValue = ((Date) lastValue).getTime();
		}
		return lastValue;
	}

    protected Object convertStrLastValue(Object lastValue){
        if(lastValue == null){
            return null;
        }
//        if(lastValue instanceof LocalDateTime){
//            lastValue = new Long(((Date) lastValue).getTime());
//        }
        return lastValue;
    }




	public static boolean needUpdate(Integer lastValueType, Object oldValue,Object newValue){
        return compare(lastValueType, oldValue,newValue);
	}
    public static boolean max(Integer lastValueType, Object oldValue,Object newValue){
        return compare(lastValueType, oldValue,newValue);
    }


    private static boolean compare(Integer lastValueType, Object oldValue,Object newValue){
		if(newValue == null)
			return false;

		if(oldValue == null)
			return true;
//		this.getLastValueType()
        if(lastValueType == null){
            if(oldValue instanceof Date)    {
                lastValueType = ImportIncreamentConfig.TIMESTAMP_TYPE;
            }
            else if(oldValue instanceof LocalDateTime)    {
                lastValueType = ImportIncreamentConfig.LOCALDATETIME_TYPE;
            }
			else if(oldValue instanceof String) {
				lastValueType = ImportIncreamentConfig.STRING_TYPE;
			}
            else{
                lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
            }
        }
		if(lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			Date oldValueDate = (Date)oldValue;
			Date newValueDate = (Date)newValue;
			if(newValueDate.after(oldValueDate))
				return true;
			else
				return false;
		}
        else if(lastValueType == ImportIncreamentConfig.LOCALDATETIME_TYPE) {
            LocalDateTime oldValueDate = (LocalDateTime)oldValue;
            LocalDateTime newValueDate = (LocalDateTime)newValue;
            if(newValueDate.isAfter(oldValueDate))
                return true;
            else
                return false;
        }
		else{
//			Method compareTo = oldValue.getClass().getMethod("compareTo");
            Number ts = (Number)oldValue;
            Number nts = (Number)newValue;
            if(nts.longValue() > ts.longValue())
                return true;
            else
                return false;
            /**
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
             */

		}
	}



	private void initStatusSQL(String statusDbname ){
		createStatusTableSQL = DBConfig.getCreateStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());

		createHistoryStatusTableSQL = DBConfig.getCreateHistoryStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
		createStatusTableSQL = createStatusTableSQL.replace("$statusTableName",statusTableName);
		createHistoryStatusTableSQL = createHistoryStatusTableSQL.replace("$historyStatusTableName",historyStatusTableName);
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
				logger.warn("add field to table failed：" + addFiledSQL + ", field "+ field +" alread exist.");
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
			logger.warn("filePath,status and fileId not exit in table {"+statusTableName+"}");

			addField("filePath",statusTableName,defaultValue,"500",type);
			addField("relativeParentDir",statusTableName,defaultValue,"500",type);
			addField("fileId",statusTableName,defaultValue,"500",type);
            addField("strLastValue",statusTableName,defaultValue,"100",type);
			addField("status",statusTableName,null,"1",DBConfig.getStatusTableTypeNumber(SQLUtil.getPool(statusDbname).getDBType()));
			addField("jobId",statusTableName,defaultValue,"500",type);
			addField("jobType",statusTableName,defaultValue,"500",type);

		}



	}

	@Override
	public void initLastValueType(){
		if (importContext.getLastValueType() != null) {
			this.lastValueType = importContext.getLastValueType();
		}

		else {
			this.lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
		}
		/**
		 * 回填值类型
		 */
		importContext.setLastValueType(this.lastValueType);
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
                        tempConf.setConnectionTimeout(statusDBConfig.getConnectionTimeout());
                        tempConf.setMaxIdleTime(statusDBConfig.getMaxIdleTime());
                        tempConf.setMaxWait(statusDBConfig.getMaxWait());
						tempConf.setMinimumSize(10);
						tempConf.setMaximumSize(20);
						tempConf.setUsepool(true);
						tempConf.setExternal(false);
						tempConf.setExternaljndiName((String)null);
						tempConf.setShowsql(statusDBConfig.isShowSql());
						tempConf.setEncryptdbinfo(false);
						tempConf.setQueryfetchsize(null);
						tempConf.setDbInfoEncryptClass(statusDBConfig.getDbInfoEncryptClass());
                        tempConf.setConnectionProperties(statusDBConfig.getConnectionProperties());
                        tempConf.setEnableBalance(statusDBConfig.isEnableBalance());
                        tempConf.setDatasource(statusDBConfig.getDataSource());
                        tempConf.setBalance(statusDBConfig.getBalance());
						boolean ret = SQLManager.startPool(tempConf);
						if(!ret){
							logger.warn("Ignore start started Status_datasource["+statusDbname+"].");
							this.useOuterStatusDb = true;
							initStatusSQL(statusDbname );
						}
					} catch (Exception e) {
						throw ImportExceptionUtil.buildDataImportException(importContext,e);
					}
					initStatusSQL( statusDBConfig );
				}
                else if(statusDBConfig.getDataSource() != null){
                    if(statusDbname == null || statusDbname.trim().equals(""))
                        statusDbname =  "_status_datasource";

                    String dbJNDIName = statusDbname+"_jndi";
                    try {



                        DBConf tempConf = new DBConf();
                        tempConf.setPoolname(statusDbname);
                        tempConf.setDriver(statusDBConfig.getDbDriver());
                       
                        tempConf.setJndiName(dbJNDIName);
                       
                        tempConf.setShowsql(statusDBConfig.isShowSql());
                        tempConf.setDbAdaptor(statusDBConfig.getDbAdaptor());
                        tempConf.setDbtype(statusDBConfig.getDbtype());
                        tempConf.setColumnLableUpperCase(statusDBConfig.isColumnLableUpperCase());
                        tempConf.setDatasource(statusDBConfig.getDataSource());
                        boolean ret = SQLManager.startPool(tempConf);
                        if(!ret){
                            logger.warn("Ignore start started Status_datasource["+statusDbname+"].");
                            this.useOuterStatusDb = true;
                            initStatusSQL(statusDbname );
                        }
                    } catch (Exception e) {
                        throw ImportExceptionUtil.buildDataImportException(importContext,e);
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
//			if (importContext.getLastValueType() != null) {
//				this.lastValueType = importContext.getLastValueType();
//			}
////			else if (importContext.getDateLastValueColumn() != null) {
////				this.lastValueType = ImportIncreamentConfig.TIMESTAMP_TYPE;
////			} else if (importContext.getNumberLastValueColumn() != null) {
////				this.lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
////
////			}
//			else {
//				this.lastValueType = ImportIncreamentConfig.NUMBER_TYPE;
//			}
			/**
			 * 回填值类型
			 */
//			importContext.setLastValueType(this.lastValueType);
			dataTranPlugin.getSetLastValueType().set();


			existSQL = new StringBuilder().append("select 1 from ").append(statusTableName).toString();
			existHisSQL = new StringBuilder().append("select 1 from ").append(historyStatusTableName).toString();
            String selectfields = "select id,lasttime,lastvalue,strLastValue,lastvaluetype,filePath,relativeParentDir,fileId,status,jobId,jobType from ";
			selectSQL = new StringBuilder().append(selectfields)
					.append(statusTableName).append(" where id=? and jobType=?").toString();
			selectByJobIdSQL = new StringBuilder().append(selectfields)
					.append(statusTableName).append(" where id=? and ").append(" jobId=? and jobType=?").toString();
			checkFieldSQL = "select filePath,fileId,relativeParentDir,status,jobId,jobType,strLastValue from " + statusTableName + " where 1 <> 1";
			checkHisFieldSQL = "select filePath,fileId,relativeParentDir,status,statusId,jobId,jobType,strLastValue from " + historyStatusTableName + " where 1 <> 1";
			selectAllSQL =  new StringBuilder().append(selectfields)
					.append(statusTableName).append(" where jobType=? and status in (0,2)").toString();
            selectCompleteStatusByFileId = new StringBuilder().append(selectfields)
					.append(statusTableName).append(" where fileId = ? and jobType=? and status = 1").toString();

            selectCompleteStatuses = new StringBuilder().append(selectfields)
                    .append(statusTableName).append(" where jobType=? and status = 1 and lasttime <= ?").toString();
			selectAllByJobIdSQL =  new StringBuilder().append(selectfields)
					.append(statusTableName).append(" where jobId = ? and jobType=? and status in (0,2)").toString();

            selectCompleteStatusByJobIdAndFileIdSQL = new StringBuilder().append(selectfields)
                    .append(statusTableName).append(" where jobId = ? and fileId = ? and jobType=? and status = 1").toString();

            selectCompleteStatusByJobIdSQL = new StringBuilder().append(selectfields)
                    .append(statusTableName).append(" where jobId = ? and jobType=? and status = 1 and lasttime <= ?").toString();
            
			updateSQL = new StringBuilder().append("update ").append(statusTableName)
					.append(" set lasttime = ?,lastvalue = ? ,strLastValue = ?,lastvaluetype= ? , filePath = ?,relativeParentDir = ?,fileId = ? ,status = ? where id=? and jobType=?").toString();
			updateByJobIdSQL = new StringBuilder().append("update ").append(statusTableName)
					.append(" set lasttime = ?,lastvalue = ? ,strLastValue = ?,lastvaluetype= ? , filePath = ?,relativeParentDir = ?,fileId = ? ,status = ? where id=? and ").append(" jobId=? and jobType=?").toString();
			updateStatusSQL = new StringBuilder().append("update ")
					.append(statusTableName).append(" set status = ?, lasttime= ?").append(" where id=? and jobType=?").toString();
			updateByJobIdStatusSQL = new StringBuilder().append("update ")
					.append(statusTableName).append(" set status = ?, lasttime= ?").append(" where id=? and ").append(" jobId=? and jobType=?").toString();
			insertSQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append(" (id,lasttime,lastvalue,strLastValue,lastvaluetype,filePath,relativeParentDir,fileId,status,jobId,jobType) values(?,?,?,?,?,?,?,?,?,?,?)").toString();
			deleteSQL = new StringBuilder().append("delete from ")
					.append(statusTableName).append(" where id=? and jobType=?").toString();
			deleteByJobIdSQL = new StringBuilder().append("delete from ")
					.append(statusTableName).append(" where id=? and ").append(" jobId=? and jobType=?").toString();
            String selectHistoryFields = "select id,lasttime,lastvalue,strLastValue,lastvaluetype,filePath,relativeParentDir,fileId,status,jobId,jobType,statusId from ";
            selectHistoryStatusByFileId = new StringBuilder().append(selectHistoryFields).append(statusTableName)
                    .append("_his  where fileId = ? and jobType=?").toString();
            this.selectHistoryStatusByJobIdAndFileIdSQL = new StringBuilder().append(selectHistoryFields).append(statusTableName)
                    .append("_his  where jobId = ? and fileId = ? and jobType=?").toString();
			insertHistorySQL = new StringBuilder().append("insert into ").append(statusTableName)
					.append("_his (id,lasttime,lastvalue,strLastValue,lastvaluetype,filePath,relativeParentDir,fileId,status,jobId,jobType,statusId) values(?,?,?,?,?,?,?,?,?,?,?,?)").toString();
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
			throw ImportExceptionUtil.buildDataImportException(importContext,e1);

		}
	}
	public void initTableAndStatus(InitLastValueClumnName initLastValueClumnName){
		initLastValueClumnName.initLastValueClumnName();
		initStatusStore();
		initDatasource();
		if(this.isIncreamentImport() && this.importContext.getStatusTableId() == null) {
			dataTranPlugin.initStatusTableId();
		}
		if(this.isIncreamentImport()) {
			try {
				DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				initLastDate = dateFormat.parse("1970-01-01 00:00:00");
                initLastLocalDateTime = TimeUtil.localDateTime("1970-01-01 00:00:00");
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
                initLastLocalDateTime = TimeUtil.localDateTime("1970-01-01 00:00:00");
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

			init();
			dataTranPlugin.getLoadCurrentStatus().load();//this.loadCurrentStatus();
		}
		else{

			try {
				Status currentStatus = new Status();
				currentStatus.setId(importContext.getStatusTableId());
				currentStatus.setTime(new Date().getTime());
				currentStatus.setJobId(importContext.getJobId());
                LastValueWrapper lastValueWrapper = new LastValueWrapper();
                currentStatus.setCurrentLastValueWrapper(lastValueWrapper);
                lastValueWrapper.setTimeStamp(currentStatus.getTime());
//                lastValueWrapper.setLastValue(currentStatus.getLastValue());
				this.firstStatus = (Status) currentStatus.copy();
				this.currentStatus = currentStatus;
			}
			catch (Exception e){
				throw ImportExceptionUtil.buildDataImportException(importContext,e);
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
                    if(SimpleStringUtil.isNotEmpty(importContext.getLastValueStorePassword()) )
                        statusStorePassword = importContext.getLastValueStorePassword() ;
				}
			} else {
				statusStorePath = importContext.getLastValueStorePath();
                if(SimpleStringUtil.isNotEmpty(importContext.getLastValueStorePassword()) )
                    statusStorePassword = importContext.getLastValueStorePassword() ;
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
            createStatusTableSQL = DBConfig.sqlitex_createStatusTableSQL.replace("$statusTableName",statusTableName);;
            createHistoryStatusTableSQL = DBConfig.sqlitex_createHistoryStatusTableSQL.replace("$historyStatusTableName",historyStatusTableName);

			File dbpath = new File(statusStorePath);
			logger.info("initDatasource dbpath:" + dbpath.getCanonicalPath());
			DBConf tempConf = new DBConf();
			tempConf.setPoolname(statusDbname);
			tempConf.setDriver("org.sqlite.JDBC");
			tempConf.setJdbcurl("jdbc:sqlite://" + dbpath.getCanonicalPath());
			tempConf.setUsername("root");
			tempConf.setPassword(statusStorePassword);
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
			boolean ret = SQLManager.startPool(tempConf);
//			JDBCPool jdbcPool = SQLUtil.getSQLManager().getPool(tempConf.getPoolname(),false);
			if(!ret ){
//				throw new DataImportException("status_datasource["+statusDbname+"] not started.");
				logger.warn("Ignore start started Status_datasource["+statusDbname+"].");
				this.useOuterStatusDb = true;
				initStatusSQL(statusDbname );
			}
			else{
				logger.warn("Start Status_datasource["+statusDbname+"] complete.");
			}
		} catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
	}
	private void initStatusSQL(DBConfig statusDBConfig ){
		createStatusTableSQL = statusDBConfig.getStatusTableDML();
		if(createStatusTableSQL == null){
			createStatusTableSQL = statusDBConfig.getCreateStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
		}
        createHistoryStatusTableSQL = statusDBConfig.getStatusHistoryTableDML();
        if(createHistoryStatusTableSQL == null)
		    createHistoryStatusTableSQL = statusDBConfig.getCreateHistoryStatusTableSQL(SQLUtil.getPool(statusDbname).getDBType());
		createStatusTableSQL = createStatusTableSQL.replace("$statusTableName",statusTableName);
		createHistoryStatusTableSQL = createHistoryStatusTableSQL.replace("$historyStatusTableName",historyStatusTableName);
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
			logger.warn("filePath,status,statusId and fileId not exit in table {"+historyStatusTableName+"},and will be added.");

			addField("filePath",historyStatusTableName,defaultValue,"500",type);
			addField("relativeParentDir",historyStatusTableName,defaultValue,"500",type);
			addField("fileId",historyStatusTableName,defaultValue,"500",type);
			addField("jobId",historyStatusTableName,defaultValue,"500",type);
			addField("jobType",historyStatusTableName,defaultValue,"500",type);
            addField("strLastValue",historyStatusTableName,defaultValue,"100",type);
			addField("status",historyStatusTableName,null,"1",DBConfig.getStatusTableTypeNumber(SQLUtil.getPool(statusDbname).getDBType()));
			addField("statusId",historyStatusTableName,null,"10",DBConfig.getStatusTableTypeBigNumber(SQLUtil.getPool(statusDbname).getDBType()));
		}


	}
	public void initLastValueClumnName(){
        if(importContext.isConfigIncreamentImport() != null
                && importContext.isConfigIncreamentImport() == false){
            setIncreamentImport(false);
            return;
        }
		if(lastValueClumnName != null){
			return ;
		}

		if (importContext.getLastValueColumn() != null) {
			lastValueClumnName = importContext.getLastValueColumn();
		}
//		else if (importContext.getNumberLastValueColumn() != null) {
//			lastValueClumnName = importContext.getNumberLastValueColumn();
//		}
		else if (dataTranPlugin.getLastValueVarName() != null) {
			lastValueClumnName =  dataTranPlugin.getLastValueVarName();
		}

        
		if (lastValueClumnName == null){
            if(!importContext.validateIncreamentConfig()) {
                throw ImportExceptionUtil.buildDataImportException(importContext,"配置校验失败：增量导入情况下，未指定增量导入状态字段!");
            }
			setIncreamentImport(false);
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
			if(olded.getJobId() == null)
				SQLExecutor.updateWithDBName(statusDbname, updateStatusSQL, olded.getStatus(), olded.getTime(),olded.getId());
			else
				SQLExecutor.updateWithDBName(statusDbname, updateByJobIdStatusSQL, olded.getStatus(), olded.getTime(),olded.getId(),olded.getJobId());
		}
		catch (Exception e){
			logger.error("handleCompletedTasks failed:"+ SimpleStringUtil.object2json(olded),e);
		}


	}

    private void handleStatus(Status currentStatus , boolean fromUpdate){
        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();
        currentStatus.setOriginCurrentLastValueWrapper(lastValueWrapper.copy());
        Object lastValue = lastValueWrapper.getLastValue();
        String strLastValue = null;

        if(importContext.isLastValueDateType()){
            if(logger.isDebugEnabled()){
                logger.debug("AddStatus: 增量字段值 LastValue is Date Type:{},real data type is {},real last value is {}",importContext.isLastValueDateType(),
                        lastValue.getClass().getName(),lastValue);
            }
            if(lastValue instanceof Date) {
                lastValue = ((Date) lastValue).getTime();
                lastValueWrapper.setLastValue(lastValue);
            }
            else{
                throw ImportExceptionUtil.buildDataImportException(importContext,"AddStatus: 增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
            }
//            lastValueWrapper.setStrLastValue(null);
//            strLastValue = null;
        }
        else if(importContext.isLastValueLocalDateTimeType()){
            if(logger.isDebugEnabled()){
                logger.debug("AddStatus: 增量字段值 LastValue isLastValueLocalDateTimeType:{},real data type is {},real last value is {}",importContext.isLastValueLocalDateTimeType(),
                        lastValue.getClass().getName(),lastValue);
            }

            if(lastValue instanceof LocalDateTime) {
                strLastValue = TimeUtil.changeLocalDateTime2String((LocalDateTime)lastValue,importContext.getLastValueDateformat());
                lastValueWrapper.setStrLastValue(strLastValue);
                lastValueWrapper.setLastValue(-1);
                lastValue = null;
            }
            else{
                throw ImportExceptionUtil.buildDataImportException(importContext,"AddStatus: 增量字段为LocalDateTime类型，But the LastValue is not a LocalDateTime value:"+lastValue+",value type is "+lastValue.getClass().getName());
            }
        }
        else{
            if(logger.isDebugEnabled()){
                logger.debug("handleStatus: 增量字段值 LastValue is Number Type:{},real data type is {},real last value is {}",importContext.isLastValueNumberType(),
                            lastValue.getClass().getName(),lastValue);
            }
//            lastValueWrapper.setStrLastValue(null);
//            strLastValue = null;
        }


        if (this.lastValueWraperSerial != null) {
            lastValueWraperSerial.serial(currentStatus);
        }
    }
	public void addStatus(Status currentStatus) throws DataImportException {
//		Object lastValue = !importContext.isLastValueDateType()?currentStatus.getLastValue():((Date)currentStatus.getLastValue()).getTime();

//        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();
//        Object lastValue = lastValueWrapper.getLastValue();
//        String strLastValue = lastValueWrapper.getStrLastValue();
//
//		if(importContext.isLastValueDateType()){
//            if(logger.isInfoEnabled()){
//                logger.info("AddStatus: 增量字段值 LastValue is Date Type:{},real data type is {},real last value is {}",importContext.isLastValueDateType(),
//                        lastValue.getClass().getName(),lastValue);
//            }
//			if(lastValue instanceof Date) {
//				lastValue = ((Date) lastValue).getTime();
//                lastValueWrapper.setLastValue(lastValue);
//			}
//			else{
//				throw new DataImportException("AddStatus: 增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
//			}
//            lastValueWrapper.setStrLastValue(null);
//            strLastValue = null;
//		}
//        else if(importContext.isLastValueLocalDateTimeType()){
//            if(logger.isInfoEnabled()){
//                logger.info("AddStatus: 增量字段值 LastValue isLastValueLocalDateTimeType:{},real data type is {},real last value is {}",importContext.isLastValueLocalDateTimeType(),
//                        lastValue.getClass().getName(),lastValue);
//            }
//
//            if(lastValue instanceof LocalDateTime) {
//                strLastValue = TimeUtil.changeLocalDateTime2String((LocalDateTime)lastValue,importContext.getLastValueDateformat());
//                lastValueWrapper.setStrLastValue(strLastValue);
//                lastValueWrapper.setLastValue(null);
//                lastValue = null;
//            }
//            else{
//                throw new DataImportException("AddStatus: 增量字段为LocalDateTime类型，But the LastValue is not a LocalDateTime value:"+lastValue+",value type is "+lastValue.getClass().getName());
//            }
//        }
//        else{
//            if(logger.isInfoEnabled()){
//                logger.info("AddStatus: 增量字段值 LastValue is Number Type:{},real data type is {},real last value is {}",importContext.isLastValueNumberType(),
//                        lastValue.getClass().getName(),lastValue);
//            }
//            lastValueWrapper.setStrLastValue(null);
//            strLastValue = null;
//        }


		try {
//            if(this.lastValueWraperSerial != null){
//                lastValueWraperSerial.serial(currentStatus);
//            }
            handleStatus(currentStatus,false);
			SQLExecutor.insertWithDBName(statusDbname,insertSQL,currentStatus.getId(),currentStatus.getTime(),currentStatus.getCurrentLastValueWrapper().getLastValue(),currentStatus.getStrLastValue(),lastValueType,
					currentStatus.getFilePath(),currentStatus.getRelativeParentDir(),
					currentStatus.getFileId(),currentStatus.getStatus(),currentStatus.getJobId(),currentStatus.getJobType());
		} catch (SQLException throwables) {
			throw new DataImportException("Add Status failed:"+currentStatus.toString(),throwables);
		}
	}
	public void updateStatus(Status currentStatus) throws Exception {
//        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();
//        Object lastValue = lastValueWrapper.getLastValue();
//        String strLastValue = lastValueWrapper.getStrLastValue();
//
//		if(importContext.isLastValueDateType()){
//            if (logger.isDebugEnabled()) {
//                logger.debug("UpdateStatus：增量字段值 LastValue is Date Type:{},real data type is {},and real last value to sqlite is {}", importContext.isLastValueDateType(),
//                        lastValue.getClass().getName(), lastValue);
//            }
//			if(lastValue instanceof Date) {
//				lastValue = ((Date) lastValue).getTime();
//			}
//			else{
//				throw new DataImportException("UpdateStatus：增量字段为日期类型，But the LastValue is not a Date value:"+lastValue+",value type is "+lastValue.getClass().getName());
//			}
//		}
//        else if(importContext.isLastValueLocalDateTimeType()){
//            if(lastValue instanceof LocalDateTime) {
//                String strLastValue = TimeUtil.changeLocalDateTime2String((LocalDateTime)lastValue,importContext.getLastValueDateformat());
//                currentStatus.setLastValue(null);
//                currentStatus.setStrLastValue(strLastValue);
//            }
//            else{
//                throw new DataImportException("AddStatus: 增量字段为LocalDateTime类型，But the LastValue is not a LocalDateTime value:"+lastValue+",value type is "+lastValue.getClass().getName());
//            }
//        }
//        else if(importContext.isLastValueNumberType()) {
//            if (logger.isDebugEnabled()) {
//                logger.debug("UpdateStatus：增量字段值 LastValue is Number Type:{},real data type is {},and real last value to sqlite is {}", importContext.isLastValueDateType(),
//                        lastValue.getClass().getName(), lastValue);
//            }
//        }
////		SQLExecutor.updateWithDBName(statusDbname,updateSQL, currentStatus.getTime(), lastValue,
////									lastValueType,currentStatus.getFilePath(),currentStatus.getFileId(),
////									currentStatus.getStatus(),currentStatus.getId());
		if(!isStoped()) {
            handleStatus(currentStatus,true);
			putStatus(currentStatus);
		}
	}


	private void initLastValueStatus(boolean update) throws Exception {
		Status currentStatus = new Status();
		currentStatus.setId(importContext.getStatusTableId());
		currentStatus.setTime(new Date().getTime());
        LastValueWrapper lastValueWrapper = new LastValueWrapper();
        currentStatus.setCurrentLastValueWrapper(lastValueWrapper);
        lastValueWrapper.setTimeStamp(currentStatus.getTime());
        dataTranPlugin.initLastValueStatus(currentStatus,this);

		if(importContext.getJobId() != null) {
			currentStatus.setJobId(importContext.getJobId());
		}
		currentStatus.setJobType(importContext.getJobType());

		currentStatus.setLastValueType(lastValueType);
        Status tmp = currentStatus.copy();
		if(!update) {
            addStatus(tmp);
        }
		else {
            updateStatus(tmp);
        }

		this.currentStatus = currentStatus;
		this.firstStatus = (Status) currentStatus.copy();
		if(logger.isInfoEnabled())
			logger.info("Init Job {} LastValue Status: {}",importContext.getJobName(),currentStatus.toString());
	}

	public  void handleLostedTasks(List<Status> losteds , boolean needSyn){
		try {
			for (Status losted : losteds) {
                Status status = losted.copy();
                handleStatus(status,true);
				putStatus(status);
			}
		}
		catch (Exception e){
			logger.error("handleCompletedTasks failed:"+SimpleStringUtil.object2json(losteds),e);
		}
	}

    /**
     * 迁移过期的已完成状态记录
     * @param completed
     */
	public  void handleOldedRegistedRecordTasks(List<Status> completed){


        handleOldedRegistedRecordTasks( completed,true);


	}

    /**
     * 迁移过期的已完成状态记录
     * @param completed
     */
    public  void handleOldedRegistedRecordTasks(List<Status> completed,boolean needCopy){


        try {
            for (Status status_ : completed) {

                Status status = needCopy?status_.copy():status_;
                handleStatus(status,true);
                SQLExecutor.insertWithDBName(statusDbname, insertHistorySQL, SimpleStringUtil.getUUID(), status.getTime(),
                        status.getCurrentLastValueWrapper().getLastValue(),status.getStrLastValue(), status.getLastValueType(), status.getFilePath(), status.getRelativeParentDir(),status.getFileId(), status.getStatus(),status.getJobId(),status.getJobType(),status.getId());
                if(status.getJobId() == null) {
                    SQLExecutor.deleteWithDBName(statusDbname, deleteSQL, status.getId(),status.getJobType());
                }
                else{
                    SQLExecutor.deleteWithDBName(statusDbname, deleteByJobIdSQL, status.getId(),status.getJobId(),status.getJobType());
                }
                if(logger.isInfoEnabled())
                    logger.info("Move Olded Task Registed Records to history complete {}:",status.toString());



            }
        }
        catch (Exception e){
            logger.error("handleOldedRegistedRecordTasks failed:",e);
        }


    }

    /**
     * 迁移过期的已完成状态记录:指定过期时间
     */
    public  void handleOldedRegistedRecordTasks(long deleteTime){


        try {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");           
            
            List<Status> completed = null;
            if(importContext.getJobId() != null) {
                completed = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectCompleteStatusByJobIdSQL, importContext.getJobId(), importContext.getJobType(),deleteTime);
            }
            else{
                completed = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectCompleteStatuses,  importContext.getJobType(),deleteTime);
            }
            
            if(logger.isInfoEnabled()){
                StringBuilder sb = new StringBuilder();
                BaseTranJob.builderJobInfo(sb,importContext );
                logger.info("获取作业{}已过期历史记录数据,过期时间：{},过期记录数：{} ",sb.toString(), dateFormat.format(new Date(deleteTime)),completed != null?completed.size():0);
            }
            if(completed != null && completed.size() >  0){
                for(Status status:completed){
                    lastValueWraperSerial.deserial(status);
                }
                handleOldedRegistedRecordTasks( completed,false);
            }
             
            
        }
        catch (Exception e){
            logger.error("handleOldedRegistedRecordTasks failed:",e);
        }


    }
    public boolean isOldRegistRecordWithDeleteTime(Status completed ,long deleteTime){
        long lastTime = completed.getTime();
        if (lastTime <= deleteTime) {
            return true;
        }
//        }
        return false;
    }
    public  boolean isOldRegistRecord(Status completed ,long registLiveTime){

        long now = System.currentTimeMillis();
        long deleteTime = now - registLiveTime;
        return isOldRegistRecordWithDeleteTime(  completed ,  deleteTime);
    }

	@Override
	public List<Status> getPluginStatuses(){
		try {
			if(importContext.getJobId() == null) {
				List<Status> statuses = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectAllSQL, importContext.getJobType());
                if(statuses != null && statuses.size() >  0){
                    for(Status status:statuses){
                        lastValueWraperSerial.deserial(status);
                    }
                }
				return statuses;
			}
			else{
				List<Status> statuses = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectAllByJobIdSQL,importContext.getJobId(), importContext.getJobType());
                if(statuses != null && statuses.size() >  0){
                    for(Status status:statuses){
                        lastValueWraperSerial.deserial(status);
                    }
                }
				return statuses;
			}
		} catch (SQLException throwables) {
			throw new DataImportException(throwables);
		}
	}
	@Override
	public LoadCurrentStatus getLoadCurrentStatus(){
		return new LoadCurrentStatus() {
			@Override
			public void load() {
				loadCurrentStatus();
			}
		};
	}
	public Status getStatus(String jobId, String jobType, String statusId) {

		try {
            Status status = null;
			if (jobId == null) {
				status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectSQL, statusId, jobType);
			} else {
				status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectByJobIdSQL, statusId, jobId, jobType);
			}
            this.lastValueWraperSerial.deserial(status);
			return status;
		}
		catch (Exception e){
			throw new DataImportException(e);
		}
	}

    /**
     * 根据fileId获取已完成状态Status
     * @param fileId
     */
    @Override
    public Status getComplateStatusByFileId(String fileId) { 
        try {
            String jobId = importContext.getJobId();
            String jobType = importContext.getJobType();
            Status status = null;
            if (jobId == null) {
                status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectCompleteStatusByFileId, fileId, jobType);
                if(status == null){
                    status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectHistoryStatusByFileId, fileId, jobType);
                    if(status != null){
                        status.setHistoryStatus(true);
                    }
                }
            } else {
                status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectCompleteStatusByJobIdAndFileIdSQL,  jobId, fileId,jobType);
                if(status == null){
                    status = SQLExecutor.queryObjectWithDBName(Status.class, statusDbname, selectHistoryStatusByJobIdAndFileIdSQL,  jobId, fileId,jobType);
                    if(status != null){
                        status.setHistoryStatus(true);
                    }
                }
            }
            if(status != null) {
                this.lastValueWraperSerial.deserial(status);
            }
            return status;
        } catch (Exception e){
            throw new DataImportException(e);
            
        }
    }
    /**
     * 加载当前作业状态
     */
    protected void loadCurrentStatus(){
		try {

			/**
			 * 初始化数据检索起始状态信息
			 */
			currentStatus = getStatus(importContext.getJobId(),importContext.getJobType(),importContext.getStatusTableId());

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
                    LastValueWrapper lastValueWrapper =currentStatus.getCurrentLastValueWrapper();
					if(currentStatus.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE){
						Object lastValue = lastValueWrapper.getLastValue();
						if(lastValue instanceof Long){
                            lastValueWrapper.setLastValue(new Date((Long)lastValue));
						}
						else if(lastValue instanceof BigDecimal){
                            lastValueWrapper.setLastValue(new Date(((BigDecimal) lastValue).longValue()));
						}
						else if(lastValue instanceof Integer){
                            lastValueWrapper.setLastValue(new Date(((Integer) lastValue).longValue()));
						}
						else{
							if(logger.isWarnEnabled())
								logger.warn("initTableAndStatus：增量字段类型为日期类型, But the LastValue from status table is not a long value:{},value type is {}",lastValue,lastValue.getClass().getName());
							throw new DataImportException("InitTableAndStatus：增量字段类型为日期类型, But the LastValue from status table is not a long value:"+lastValue+",value type is "+lastValue.getClass().getName());
						}
					}
                    else if(currentStatus.getLastValueType() == ImportIncreamentConfig.LOCALDATETIME_TYPE){
                        String lastValue = lastValueWrapper.getStrLastValue();
                        lastValueWrapper.setLastValue(TimeUtil.localDateTime(lastValue));
                    }
					this.firstStatus = (Status) currentStatus.copy();
				}
			}
		}
		catch (DataImportException e) {
			throw e;
		}
		catch (Exception e) {
			throw new DataImportException(e);
		}
	}

	public Status getCurrentStatus(){
		return this.currentStatus;
	}
	public void setIncreamentImport(boolean increamentImport) {
		this.increamentImport = increamentImport;
	}
	public boolean isIncreamentImport() {
		return increamentImport;
	}
	public String getLastValueClumnName(){
		return this.lastValueClumnName;
	}
	public Object[] putLastParamValue(Map params){
		Object[] ret = new Object[2];
        LastValueWrapper lastValueWrapper = this.currentStatus.getCurrentLastValueWrapper();
        String lastValueVarName = dataTranPlugin.getLastValueVarName();
        Object lastValue = lastValueWrapper.getLastValue();
		if(this.lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			params.put(lastValueVarName, lastValue);
            if(importContext.increamentEndOffset() != null && importContext.isNumberTypeTimestamp()){
                Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
                ret[1] = lastOffsetValue;
                params.put(lastValueVarName+"__endTime", lastOffsetValue.getTime());
            }


		}
        else if(this.lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE)  {
			Date ldate = null;
			if(lastValue instanceof Date) {
				ldate = (Date)lastValue;


			}
			else {
				if(lastValue instanceof Long) {
					ldate = new Date((Long)lastValue);
				}
				else if(lastValue instanceof BigDecimal){
					ldate = new Date(((BigDecimal) lastValue).longValue());
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
			params.put(lastValueVarName, formatLastDateValue(ldate));

			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				ret[1] = lastOffsetValue;
				params.put(lastValueVarName+"__endTime", formatLastDateValue(lastOffsetValue));
			}
		}
        else if(this.lastValueType == ImportIncreamentConfig.LOCALDATETIME_TYPE) {
            params.put(lastValueVarName, formatLastLocalDateTimeValue((LocalDateTime)lastValue));
            if(importContext.increamentEndOffset() != null){
                Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
                LocalDateTime localDateTime = TimeUtil.date2LocalDateTime(lastOffsetValue);
                ret[1] = localDateTime;
                params.put(lastValueVarName+"__endTime", formatLastLocalDateTimeValue(localDateTime));
            }
        }
		if(isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(params).toString());
		}
		ret[0] = lastValue;
		return ret;
	}
	protected boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}
	protected Object formatLastDateValue(Date date){

		return importContext.getInputPlugin().formatLastDateValue(date);


	}
    protected Object formatLastLocalDateTimeValue(LocalDateTime date){

        return importContext.getInputPlugin().formatLastLocalDateTimeValue(date);


    }
	public void stopStatusDatasource(){
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
	}



	public Map getParamValue(Map params){
        LastValueWrapper lastValueWrapper = currentStatus.getCurrentLastValueWrapper();
		Object lastValue = lastValueWrapper.getLastValue();
        String lastValueVarName = dataTranPlugin.getLastValueVarName();
		if(this.lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
			params.put(lastValueVarName, lastValue);
            if(importContext.isNumberTypeTimestamp() && importContext.increamentEndOffset() != null){
                Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
                params.put(lastValueVarName+"__endTime", lastOffsetValue.getTime());
            }
		}
		else if(this.lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE){
			if(lastValue instanceof Date)
				params.put(lastValueVarName, lastValue);
			else {
				if(lastValue instanceof Long) {
					params.put(lastValueVarName, new Date((Long)lastValue));
				}
				else if(lastValue instanceof BigDecimal){
					params.put(lastValueVarName, new Date(((BigDecimal)lastValue).longValue()));
				}
				else if(lastValue instanceof Integer){
					params.put(lastValueVarName, new Date(((Integer) lastValue).longValue()));
				}
				else if(lastValue instanceof Short){
					params.put(lastValueVarName, new Date(((Short) lastValue).longValue()));
				}
				else{
					params.put(lastValueVarName, new Date(((Number) lastValue).longValue()));
				}
			}
			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				params.put(lastValueVarName+"__endTime", lastOffsetValue);
			}
		}
        else if(this.lastValueType == ImportIncreamentConfig.LOCALDATETIME_TYPE) {
            params.put(lastValueVarName, lastValue);
            if(importContext.increamentEndOffset() != null){
                Date lastOffsetValue = org.frameworkset.util.TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
                params.put(lastValueVarName+"__endTime", TimeUtil.date2LocalDateTime(lastOffsetValue));
            }
        }
		if(isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(params).toString());
		}
		return params;
	}


	@Override
	public void flushLastValue(LastValueWrapper lastValueWrapper, Status currentStatus, boolean reachEOFClosed) {
		if(lastValueWrapper != null) {
            Status status = null;
            synchronized (currentStatus) {
                LastValueWrapper oldLastValueWrapper = currentStatus.getCurrentLastValueWrapper();

                /**
                 * 在非结尾（及任务处理过程中处理的数据），才需要校验新旧数据需要更新，结尾一律更新状态值
                 */
				if (!reachEOFClosed && !importContext.needUpdateLastValueWrapper(oldLastValueWrapper, lastValueWrapper))
					return;

				if(lastValueWrapper.getTimeStamp() != null) {
					currentStatus.setTime(lastValueWrapper.getTimeStamp());
				}
				else{
					long time = System.currentTimeMillis();
					currentStatus.setTime(time);
				}
                currentStatus.setCurrentLastValueWrapper(lastValueWrapper);
				if(reachEOFClosed){
					currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
				}

				if (this.isIncreamentImport()) {
					status = currentStatus.copy();
				}
			}
            if(status != null)
                this.storeStatus(status);
		}
	}

//	@Override
//	public void flushLastValue(LastValueWrapper lastValue,Status currentStatus) {
//		flushLastValue(lastValue, currentStatus,false);
//	}
//    @Override
//    public void flushLastValueWrapper(LastValueWrapper lastValueWrapper, Status currentStatus){
//        flushLastValueWrapper( lastValueWrapper,  currentStatus,false);
//    }

//	@Override
//	public void forceflushLastValue(Status currentStatus) {
//		synchronized (currentStatus) {
//
//			currentStatus.setStatus(ImportIncreamentConfig.STATUS_COMPLETE);
//			currentStatus.setTime(System.currentTimeMillis());
//            Status tmp = currentStatus.copy();
//			this.storeStatus(tmp);
//		}
//
//	}

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
	@Override
	public int getLastValueType() {
		return lastValueType;
	}

    public Date getInitLastDate() {
        return initLastDate;
    }

    public LocalDateTime getInitLastLocalDateTime() {
        return initLastLocalDateTime;
    }
}
