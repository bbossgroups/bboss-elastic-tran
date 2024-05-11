package org.frameworkset.tran;

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.AsynSplitTranResultSet;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.SplitTranResultSet;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;
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
import java.util.function.Consumer;

public abstract class BaseDataTran implements DataTran{
	protected  Logger logger = LoggerFactory.getLogger(this.getClass());
	protected static Object dummy = new Object();
	protected ImportContext importContext;
	protected TranResultSet tranResultSet;
    protected boolean dataTranStopped;

    protected AsynTranResultSet asynTranResultSet;
	protected TaskContext taskContext;
	protected SerialTranCommand serialTranCommand;
	protected ParrelTranCommand parrelTranCommand ;
	protected String taskInfo ;
	protected TranJob tranJob;
    protected JobCountDownLatch countDownLatch;
	public abstract CommonRecord buildRecord(Context context) throws Exception;
	@Override
	public void beforeOutputData(BBossStringWriter writer){

	}

    @Override
    public Object buildSerialDatas(Object data,CommonRecord record){
        return data;
    }
	@Override
	public ImportContext getImportContext(){
		return importContext;
	}
	public void logTaskStart(Logger logger) {
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
			addedFields.put(varName, dummy);

		}

	}

    protected RecordColumnInfo resolveRecordColumnInfo(Object value,FieldMeta fieldMeta,Context context){
        return null;
    }
	private void addRecordValue(CommonRecord record,String fieldName,Object value,FieldMeta fieldMeta,Context context){
		RecordColumnInfo recordColumnInfo = resolveRecordColumnInfo(  value,  fieldMeta,  context);		 
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



	public void appendData(Data data) throws InterruptedException{

		if(asynTranResultSet != null)
			asynTranResultSet.appendData(data);
	}


	public Status getCurrentStatus() {
		return currentStatus;
	}

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


    private Object stopLock = new Object();
	/**
	 * 停止转换作业及释放作业相关资源，关闭插件资源
	 */
	public void stop(boolean fromException){
        innerStop( fromException, false);
//        if(dataTranStopped)
//            return;
//        synchronized (stopLock){
//            if(dataTranStopped)
//                return;
//            dataTranStopped = true;
//        }
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stop(fromException);
////            asynTranResultSet.clearQueue();
//			asynTranResultSet = null;
//		}
//		if(breakableScrollHandler != null) {
//			breakableScrollHandler.setBreaked(true);
//		}
////		importContext.finishAndWaitTran();
//
//        if(logger.isInfoEnabled())
//            logger.info("DataTran load data completed.");

	}

    private void innerStop(boolean fromException,boolean cleanQueue){
        if(dataTranStopped)
            return;
        synchronized (stopLock){
            if(dataTranStopped)
                return;
            dataTranStopped = true;
        }
        if(asynTranResultSet != null) {
            asynTranResultSet.stop(fromException);
            if(cleanQueue)
                asynTranResultSet.clearQueue();
            asynTranResultSet = null;
        }
        if(breakableScrollHandler != null) {
            breakableScrollHandler.setBreaked(true);
        }
//		importContext.finishAndWaitTran();

        if(logger.isInfoEnabled())
            logger.info("DataTran load data completed fromException[{}] and ClearResultsetQueue[{}].",fromException,cleanQueue);
    }

    public void stop2ndClearResultsetQueue(boolean fromException){
//        if(dataTranStopped)
//            return;
//        synchronized (stopLock){
//            if(dataTranStopped)
//                return;
//            dataTranStopped = true;
//        }
//        if(asynTranResultSet != null) {
//            asynTranResultSet.stop(fromException);
//            asynTranResultSet.clearQueue();
//            asynTranResultSet = null;
//        }
//        if(breakableScrollHandler != null) {
//            breakableScrollHandler.setBreaked(true);
//        }
////		importContext.finishAndWaitTran();
//
//        if(logger.isInfoEnabled())
//            logger.info("DataTran load data completed.");

        innerStop( fromException, true);

    }

//	/**
//	 * 只停止转换作业
//	 */
//	public void stopTranOnly(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stopTranOnly();
//			asynTranResultSet = null;
//		}
//		if(breakableScrollHandler != null) {
//			breakableScrollHandler.setBreaked(true);
//		}
//
//        if(logger.isInfoEnabled())
//            logger.info("DataTran stopTranOnly completed.");
//	}
    public String tran() throws DataImportException {
        try {
            return this.commonTran();
        }
        catch (DataImportException dataImportException){
            if(this.countDownLatch != null)
                countDownLatch.attachException(dataImportException);
            throw dataImportException;
        }
        catch (Exception dataImportException){
            if(this.countDownLatch != null)
                countDownLatch.attachException(dataImportException);
            throw new DataImportException(dataImportException);
        }
        catch (Throwable dataImportException){
            if(this.countDownLatch != null)
                countDownLatch.attachException(dataImportException);
            throw new DataImportException(dataImportException);
        }
        finally {
            if(this.countDownLatch != null)
                countDownLatch.countDown();
        }
    }
    protected String commonTran() throws DataImportException {
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
    




    protected void jobComplete(ExecutorService service,Throwable exception,LastValueWrapper lastValue ,TranErrorWrapper tranErrorWrapper,Status currentStatus,boolean reachEOFClosed){
		if (importContext.getScheduleService() == null) {//一次性非定时调度作业调度执行的话，转换完成需要关闭线程池
			if(reachEOFClosed){
				if(tranErrorWrapper.assertCondition(exception)){
					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
				}
				else{//不继续执行作业关闭作业依赖的相关资源池
					if(exception == null){
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					}
				}
			}



		}
		else{

			if(tranErrorWrapper.assertCondition(exception)){
				if(reachEOFClosed ){
					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
				}
			}
			else{//不继续执行作业关闭作业依赖的相关资源池
				if(reachEOFClosed && exception == null){
					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
				}

			}
		}
        this.stop2ndClearResultsetQueue(exception != null);
	}
	public void endJob( boolean reachEOFClosed, ImportCount importCount,Throwable errorStop){
		Date endTime = new Date();
		if(getTaskContext() != null)
			getTaskContext().setJobEndTime(endTime);
//		importCount.setJobEndTime(endTime);

//		if(reachEOFClosed){
//			tranStopReadEOFCallback(  errorStop);
//		}
		if(tranStopReadEOFCallback != null){
			TranStopReadEOFCallbackContext tranStopReadEOFCallbackContext = new TranStopReadEOFCallbackContext(errorStop,reachEOFClosed);
			tranStopReadEOFCallback.call(    tranStopReadEOFCallbackContext);
		}
	}
	public boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}
	public void waitTasksComplete(final List<Future> tasks,
								   final ExecutorService service,final Throwable exception,LastValueWrapper lastValue,
								  final ImportCount totalCount ,final TranErrorWrapper tranErrorWrapper ,final WaitTasksCompleteCallBack waitTasksCompleteCallBack,
								  final boolean reachEOFClosed){
		Consumer function = new Consumer<Object>(){

			@Override
			public void accept(Object o) {
				int count = 0;
                Throwable _exception = exception;
				for (Future future : tasks) {
					try {
						future.get();
						count ++;
					} catch (ExecutionException e) {
						if(_exception == null)
							_exception = e;
						if( logger.isErrorEnabled()) {
							if (e.getCause() != null)
								logger.error("", e.getCause());
							else
								logger.error("", e);
						}
					}catch (Exception e) {
						if(_exception == null)
							_exception = e;
						if( logger.isErrorEnabled()) logger.error("",e);
					}
				}
				if(waitTasksCompleteCallBack != null) {
                    try {
                        waitTasksCompleteCallBack.call();
                    }
                    catch (Throwable throwable){
                        if(_exception == null)
                            _exception = throwable;
                        if( logger.isErrorEnabled()) logger.error("",throwable);
                    }
                }
				if(isPrintTaskLog()) {

                    StringBuilder stringBuilder = BaseTranJob.builderJobInfo(new StringBuilder(),  importContext);
                    logger.info(stringBuilder.append("Parallel batch import Complete tasks:")
							.append(count).append(",Total success import ")
							.append(totalCount.getSuccessCount()).append(" records,Ignore Total ")
							.append(totalCount.getIgnoreTotalCount()).append(" records,failed total ")
							.append(totalCount.getFailedCount()).append(" records.").toString());
				}
				jobComplete(  service,_exception,lastValue ,tranErrorWrapper,currentStatus,reachEOFClosed);
				endJob(  reachEOFClosed, totalCount,_exception);
			}
		};
        function.accept(null);
//		if(!importContext.isAsyn() || importContext.getScheduleService() != null) {
//			function.accept(null);
//		}
//		else{
//			Thread completeThread = new Thread(new Runnable() {
//				@Override
//				public void run() {
//					function.accept(null);
//				}
//			});
//			completeThread.start();
//		}
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

        /**
		if(!importContext.useFilePointer()) {
			if (importContext.getLastValueColumnName() == null) {
				return null;
			}
			return tranResultSet.getLastValue(importContext.getLastValueColumnName());
		}
		else{
			return tranResultSet.getLastOffsetValue();
		}*/
        return tranResultSet.getLastValue();


	}

 
 
	public boolean isTranFinished() {
		return tranFinished;
	}

    public boolean isRecordDirectIgnore(){
        return tranResultSet.getAction() == Record.RECORD_DIRECT_IGNORE;
    }

}
