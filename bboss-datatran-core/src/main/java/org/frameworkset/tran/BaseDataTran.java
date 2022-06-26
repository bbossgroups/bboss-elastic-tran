package org.frameworkset.tran;

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.AsynSplitTranResultSet;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.SplitTranResultSet;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.ParrelTranCommand;
import org.frameworkset.tran.task.SerialTranCommand;
import org.frameworkset.tran.task.TranJob;
import org.frameworkset.tran.task.TranStopReadEOFCallback;
import org.frameworkset.util.annotations.DateFormateMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class BaseDataTran implements DataTran{
	protected  Logger logger = LoggerFactory.getLogger(this.getClass());
	protected static Object dummy = new Object();
	protected ImportContext importContext;
	protected TranResultSet tranResultSet;
	protected AsynTranResultSet asynTranResultSet;
	protected TaskContext taskContext;
	protected SerialTranCommand serialTranCommand;
	protected ParrelTranCommand parrelTranCommand ;
	protected String taskInfo ;
	protected TranJob tranJob;
	public void logTaskStart(Logger logger) {
//		StringBuilder builder = new StringBuilder().append("import data to db[").append(importContext.getDbConfig().getDbUrl())
//				.append("] dbuser[").append(importContext.getDbConfig().getDbUser())
//				.append("] insert sql[").append(es2DBContext.getTargetSqlInfo() == null ?"": es2DBContext.getTargetSqlInfo().getOriginSQL()).append("] \r\nupdate sql[")
//					.append(es2DBContext.getTargetUpdateSqlInfo() == null?"":es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("] \r\ndelete sql[")
//					.append(es2DBContext.getTargetDeleteSqlInfo() == null ?"":es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("] start.");
		logger.info(taskInfo + " start.");
	}

	public TaskContext getTaskContext() {
		return taskContext;
	}

	protected void appendSplitFieldValues(CommonRecord record,
										  String[] splitColumns,
										  Map<String, Object> addedFields, Context context) {
		if(splitColumns ==  null || splitColumns.length == 0){
			return;
		}

		String varName = null;
		for (String fieldName : splitColumns) {
			FieldMeta fieldMeta = context.getMappingName(fieldName);//获取字段映射或者忽略配置

			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)//忽略字段
					continue;
				varName = fieldMeta.getTargetFieldName();//获取映射字段
				if(varName == null || varName.equals(""))
					throw new DataImportException("fieldName["+fieldName+"]名称映射配置错误：varName="+varName);
			}
			else{
				varName = fieldName;
			}
			if (addedFields.containsKey(varName))
				continue;
			addRecordValue( record, varName, tranResultSet.getValue(fieldName),fieldMeta ,context);
//			record.addData(fieldName, jdbcResultSet.getValue(fieldName));
			addedFields.put(varName, dummy);

		}

	}

	private void addRecordValue(CommonRecord record,String fieldName,Object value,FieldMeta fieldMeta,Context context){
		RecordColumnInfo recordColumnInfo = null;
		if (value != null && value instanceof Date){
			DateFormat dateFormat = null;
			if(fieldMeta != null){
				DateFormateMeta dateFormateMeta = fieldMeta.getDateFormateMeta();
				if(dateFormateMeta != null){
					dateFormat = dateFormateMeta.toDateFormat();
				}
			}
			if(dateFormat == null)
				dateFormat = context.getDateFormat();
			recordColumnInfo = new RecordColumnInfo();
			recordColumnInfo.setDateTag(true);
			recordColumnInfo.setDateFormat(dateFormat);
		}
		record.addData(fieldName, value,recordColumnInfo);
	}
	protected void appendFieldValues(CommonRecord record,
									 String[] columns ,
									 List<FieldMeta> fieldValueMetas,
									 Map<String, Object> addedFields, boolean useResultKeys,Context context) {
		if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
			return;
		}

		if(columns != null && columns.length > 0) {
			for (FieldMeta fieldMeta : fieldValueMetas) {
				String fieldName = fieldMeta.getTargetFieldName();
				if (addedFields.containsKey(fieldName))
					continue;
				boolean matched = false;
				for (String name : columns) {
					if (name.equals(fieldName)) {
						addRecordValue( record,name, fieldMeta.getValue(), fieldMeta, context);
//						record.addData(name, fieldMeta.getValue());
						addedFields.put(name, dummy);
						matched = true;
						break;
					}
				}
				if (useResultKeys && !matched) {
					addRecordValue( record,fieldName, fieldMeta.getValue(), fieldMeta, context);
//					record.addData(fieldName, fieldMeta.getValue());
					addedFields.put(fieldName, dummy);
				}
			}
		}
		else{ //hbase之类的数据同步工具，数据都是在datarefactor接口中封装处理，columns信息不存在，直接用fieldValueMetas即可
			for (FieldMeta fieldMeta : fieldValueMetas) {
				String fieldName = fieldMeta.getTargetFieldName();
				if (addedFields.containsKey(fieldName))
					continue;
				addRecordValue( record,fieldName, fieldMeta.getValue(), fieldMeta, context);
//				record.addData(fieldName, fieldMeta.getValue());
				addedFields.put(fieldName, dummy);

			}
		}
	}
	/**
	 * 当前作业处理的增量状态信息
	 */
	protected Status currentStatus;
	protected volatile boolean tranFinished;
	public AsynTranResultSet getAsynTranResultSet(){
		return asynTranResultSet;
	}
	private TranStopReadEOFCallback tranStopReadEOFCallback;

	public void setTranStopReadEOFCallback(TranStopReadEOFCallback tranStopReadEOFCallback) {
		this.tranStopReadEOFCallback = tranStopReadEOFCallback;
	}

	public TranStopReadEOFCallback getTranStopReadEOFCallback() {
		return tranStopReadEOFCallback;
	}
	public void tranStopReadEOFCallback(){
		if(tranStopReadEOFCallback != null){
			tranStopReadEOFCallback.call();
		}
	}


	public void appendData(Data data) throws InterruptedException{

		if(asynTranResultSet != null)
			asynTranResultSet.appendData(data);
	}


	public Status getCurrentStatus() {
		return currentStatus;
	}

	//	private CountDownLatch countDownLatch;
	public BreakableScrollHandler getBreakableScrollHandler() {
		return breakableScrollHandler;
	}

	public void setBreakableScrollHandler(BreakableScrollHandler breakableScrollHandler) {
		this.breakableScrollHandler = breakableScrollHandler;
	}

	private BreakableScrollHandler breakableScrollHandler;
	public BaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, Status currentStatus) {
		this.currentStatus = currentStatus;
		this.taskContext = taskContext;

		if(importContext.getSplitHandler() != null){
			if(tranResultSet instanceof AsynTranResultSet) {
				AsynSplitTranResultSet asynSplitTranResultSet = new AsynSplitTranResultSet(importContext, (AsynTranResultSet) tranResultSet);
				this.asynTranResultSet = asynSplitTranResultSet;
				this.tranResultSet = asynSplitTranResultSet;
			}
			else {
				this.tranResultSet = new SplitTranResultSet(importContext, tranResultSet);
			}
		}
		else{
			this.tranResultSet = tranResultSet;

			if(tranResultSet instanceof AsynTranResultSet)
				this.asynTranResultSet = (AsynTranResultSet) tranResultSet;
		}
		this.importContext = importContext;
		tranResultSet.setBaseDataTran(this);
//		init();
	}

	protected abstract void initTranJob();
	protected abstract void initTranTaskCommand();
	public void init(){

	}
	public void afterInit(){
		initTranJob();
		initTranTaskCommand();
	}

	public void initTran(){
		init();
		afterInit();
	}


	/**
	 * 停止转换作业及释放作业相关资源，关闭插件资源
	 */
	public void stop(){
		if(breakableScrollHandler != null) {
			breakableScrollHandler.setBreaked(true);
		}
		importContext.destroy(false);

	}

	/**
	 * 只停止转换作业
	 */
	public void stopTranOnly(){
		if(breakableScrollHandler != null) {
			breakableScrollHandler.setBreaked(true);
		}
	}

//	public abstract void logTaskStart(Logger logger);
	public String tran() throws DataImportException {
		try {
			this.getDataTranPlugin().setHasTran();
			if (tranResultSet == null)
				return null;
			if (isPrintTaskLog()) {
				logTaskStart(logger);

			}
			if (importContext.getStoreBatchSize() <= 0) {
				return serialExecute();
			} else {
				if (importContext.getThreadCount() > 0 && importContext.isParallel()) {
					return this.parallelBatchExecute();
				} else {
					return this.batchExecute();
				}

			}
		}
		finally {
			tranFinished = true;
			this.getDataTranPlugin().setNoTran();

		}

	}

	protected void jobComplete(ExecutorService service,Exception exception,Object lastValue ,TranErrorWrapper tranErrorWrapper,Status currentStatus,boolean reachEOFClosed){
		if (importContext.getScheduleService() == null) {//一次性非定时调度作业调度执行的话，转换完成需要关闭线程池
//			service.shutdown();
			if(!importContext.getDataTranPlugin().isMultiTran()) {
				this.stop();
			} else{
				this.stopTranOnly();
			}
		}
		else{
			if(tranErrorWrapper.assertCondition(exception)){
				//importContext.flushLastValue( lastValue,  currentStatus ,reachEOFClosed);
				// donothing,because flushLastValue has been done by every task command.
			}
			else{//不继续执行作业关闭作业依赖的相关资源池
//				service.shutdown();
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
		}
	}
	public void endJob( boolean reachEOFClosed, ImportCount importCount){
		Date endTime = new Date();
		if(getTaskContext() != null)
			getTaskContext().setJobEndTime(endTime);
		importCount.setJobEndTime(endTime);
		if(reachEOFClosed){
			tranStopReadEOFCallback();
		}
	}
	public boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}
	public void waitTasksComplete(final List<Future> tasks,
								   final ExecutorService service,Exception exception,Object lastValue,
								  final ImportCount totalCount ,final TranErrorWrapper tranErrorWrapper ,final WaitTasksCompleteCallBack waitTasksCompleteCallBack,
								  final boolean reachEOFClosed){
		if(!importContext.isAsyn() || importContext.getScheduleService() != null) {
			int count = 0;
			for (Future future : tasks) {
				try {
					future.get();
					count ++;
				} catch (ExecutionException e) {
					if(exception == null)
						exception = e;
					if( logger.isErrorEnabled()) {
						if (e.getCause() != null)
							logger.error("", e.getCause());
						else
							logger.error("", e);
					}
				}catch (Exception e) {
					if(exception == null)
						exception = e;
					if( logger.isErrorEnabled()) logger.error("",e);
				}
			}
			if(waitTasksCompleteCallBack != null)
				waitTasksCompleteCallBack.call();

			if(isPrintTaskLog()) {

				logger.info(new StringBuilder().append("Parallel batch import Complete tasks:")
						.append(count).append(",Total success import ")
						.append(totalCount.getSuccessCount()).append(" records,Ignore Total ")
						.append(totalCount.getIgnoreTotalCount()).append(" records,failed total ")
						.append(totalCount.getFailedCount()).append(" records.").toString());
			}
			jobComplete(  service,exception,lastValue ,tranErrorWrapper,this.currentStatus,reachEOFClosed);
			endJob(  reachEOFClosed, totalCount);
		}
		else{
			Thread completeThread = new Thread(new Runnable() {
				@Override
				public void run() {
					int count = 0;
					for (Future future : tasks) {
						try {
							future.get();
							count ++;
						} catch (ExecutionException e) {
							if( logger.isErrorEnabled()) {
								if (e.getCause() != null)
									logger.error("", e.getCause());
								else
									logger.error("", e);
							}
						}catch (Exception e) {
							if( logger.isErrorEnabled()) logger.error("",e);
						}
					}
					if(waitTasksCompleteCallBack != null)
						waitTasksCompleteCallBack.call();
					if(isPrintTaskLog()) {
						logger.info(new StringBuilder().append("Parallel batch import Complete tasks:")
								.append(count).append(",Total success import ")
								.append(totalCount.getSuccessCount()).append(" records,Ignore Total ")
								.append(totalCount.getIgnoreTotalCount()).append(" records,failed total ")
								.append(totalCount.getFailedCount()).append(" records.").toString());
					}

					jobComplete(  service,null,null,tranErrorWrapper,currentStatus,reachEOFClosed);
					totalCount.setJobEndTime(new Date());
				}
			});
			completeThread.start();
		}
	}



	public static final Class[] basePrimaryTypes = new Class[]{Integer.TYPE, Long.TYPE,
								Boolean.TYPE, Float.TYPE, Short.TYPE, Double.TYPE,
								Character.TYPE, Byte.TYPE, BigInteger.class, BigDecimal.class};

	public static boolean isBasePrimaryType(Class type) {
		if (!type.isArray()) {
			if (type.isEnum()) {
				return true;
			} else {
				Class[] var1 = basePrimaryTypes;
				int var2 = var1.length;

				for(int var3 = 0; var3 < var2; ++var3) {
					Class primaryType = var1[var3];
					if (primaryType.isAssignableFrom(type)) {
						return true;
					}
				}

				return false;
			}
		} else {
			return false;
		}
	}



	public DataTranPlugin getDataTranPlugin(){
		return importContext.getDataTranPlugin();
	}
	public Object getLastValue() throws DataImportException {

		if(!importContext.useFilePointer()) {
			if (importContext.getLastValueColumnName() == null) {
				return null;
			}
			return tranResultSet.getLastValue(importContext.getLastValueColumnName());
		}
		else{
			return tranResultSet.getLastOffsetValue();
		}
//		try {
//			if (importContext.getLastValueType() == null || importContext.getLastValueType().intValue() == ImportIncreamentConfig.NUMBER_TYPE)
//				return jdbcResultSet.getValue(importContext.getLastValueClumnName());
//			else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
//				return jdbcResultSet.getDateTimeValue(importContext.getLastValueClumnName());
//			}
//		}
//		catch (DataImportException e){
//			throw (e);
//		}
//		catch (Exception e){
//			throw new DataImportException(e);
//		}
//		return null;


	}

	public boolean isTranFinished() {
		return tranFinished;
	}
}
