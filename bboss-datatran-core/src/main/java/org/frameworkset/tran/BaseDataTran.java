package org.frameworkset.tran;

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.AsynSplitTranResultSet;
import org.frameworkset.tran.record.SplitTranResultSet;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

public abstract class BaseDataTran implements DataTran{
	protected  Logger logger = LoggerFactory.getLogger(this.getClass());
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
    
	@Override
	public void beforeOutputData(BBossStringWriter writer){

	}
    protected void initTaskCommandContext(TaskCommandContext taskCommandContext){
        taskCommandContext.setTaskContext(taskContext);
        taskCommandContext.setJobNo(taskContext.getJobNo());
        taskCommandContext.setCurrentStatus(currentStatus);
        taskCommandContext.setTaskInfo(taskInfo);
        taskCommandContext.evalDataSize();
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
            throw ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
        }
        catch (Throwable dataImportException){
            if(this.countDownLatch != null)
                countDownLatch.attachException(dataImportException);
            throw ImportExceptionUtil.buildDataImportException(importContext,dataImportException);
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
    



    private void flush(Throwable exception,LastValueWrapper lastValue ,TranErrorWrapper tranErrorWrapper,Status currentStatus,boolean reachEOFClosed){
        if(reachEOFClosed){
            if(tranErrorWrapper.assertCondition(exception)){
                if(tranErrorWrapper.exceptionOccur(exception)){
                    importContext.reportJobMetricErrorLog("Excetion occur but continue on error,so flushLastValue last status to job status registry table.",tranErrorWrapper.getError(exception));
                }
                importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
            }
            else{//不继续执行作业关闭作业依赖的相关资源池
                if(!tranErrorWrapper.exceptionOccur(exception)){
                    importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
                }
            }
        }
    }
    protected void jobComplete(ExecutorService service,Throwable exception,LastValueWrapper lastValue ,TranErrorWrapper tranErrorWrapper,Status currentStatus,boolean reachEOFClosed){
//		if (importContext.getScheduleService() == null) {//一次性非定时调度作业调度执行的话，转换完成需要关闭线程池
////			if(reachEOFClosed){
////				if(tranErrorWrapper.assertCondition(exception)){
////					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
////				}
////				else{//不继续执行作业关闭作业依赖的相关资源池
////					if(exception == null){
////						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
////					}
////				}
////			}
//            flush(  exception,  lastValue ,  tranErrorWrapper,  currentStatus,  reachEOFClosed);
//
//
//		}
//		else{
//
////			if(tranErrorWrapper.assertCondition(exception)){
////				if(reachEOFClosed ){
////					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
////				}
////			}
////			else{//不继续执行作业关闭作业依赖的相关资源池
////				if(reachEOFClosed && exception == null){
////					importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
////				}
////
////			}
//            flush(  exception,  lastValue ,  tranErrorWrapper,  currentStatus,  reachEOFClosed);
//		}
        flush(  exception,  lastValue ,  tranErrorWrapper,  currentStatus,  reachEOFClosed);
        this.stop2ndClearResultsetQueue(tranErrorWrapper.exceptionOccur(exception));
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
        totalCount.setSubmitTasksEndTime(System.currentTimeMillis());
        if(isPrintTaskLog()) {

            StringBuilder stringBuilder = BaseTranJob.builderJobInfo(new StringBuilder(),  importContext);
            logger.info(stringBuilder.append("Parallel batch import submit tasks:")
                    .append(tasks.size()).append(" and take times:").append(totalCount.getSubmitTasksElapsed()).append("ms.").toString());
        }
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
                totalCount.setEndTime(System.currentTimeMillis());
				if(isPrintTaskLog()) {

                    StringBuilder stringBuilder = BaseTranJob.builderJobInfo(new StringBuilder(),  importContext);
                    logger.info(stringBuilder.append("Parallel batch import Complete tasks:")
							.append(count).append(",Total success import ")
							.append(totalCount.getSuccessCount()).append(" records,Ignore Total ")
							.append(totalCount.getIgnoreTotalCount()).append(" records,failed total ")
							.append(totalCount.getFailedCount()).append(" records,Take times:").append(totalCount.getElapsed()).append("ms.").toString());
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

    protected void initTranJob(){
        tranJob = importContext.getInputPlugin().getTranJob();
    }

}
