package org.frameworkset.tran;

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.AsynSplitTranResultSet;
import org.frameworkset.tran.record.SplitTranResultSet;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
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
	protected ImportContext targetImportContext;
	protected TranResultSet jdbcResultSet;
	protected AsynTranResultSet esTranResultSet;
	protected TaskContext taskContext;

	public TaskContext getTaskContext() {
		return taskContext;
	}

	protected void appendSplitFieldValues(CommonRecord record,
										  String[] splitColumns,
										  Map<String, Object> addedFields) {
		if(splitColumns ==  null || splitColumns.length == 0){
			return;
		}


		for (String fieldName : splitColumns) {
//				String fieldName = fieldMeta.getEsFieldName();
			if (addedFields.containsKey(fieldName))
				continue;
			record.addData(fieldName, jdbcResultSet.getValue(fieldName));
			addedFields.put(fieldName, dummy);

		}

	}
	/**
	 * 当前作业处理的增量状态信息
	 */
	protected Status currentStatus;
	protected volatile boolean tranFinished;
	public AsynTranResultSet getAsynTranResultSet(){
		return esTranResultSet;
	}
	public void appendData(Data data){

		if(esTranResultSet != null)
			esTranResultSet.appendData(data);
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
	public BaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,Status currentStatus) {
		this.currentStatus = currentStatus;
		this.taskContext = taskContext;
		this.jdbcResultSet = jdbcResultSet;

		if(jdbcResultSet instanceof AsynTranResultSet)
			esTranResultSet = (AsynTranResultSet)jdbcResultSet;
		if(importContext.getSplitHandler() != null){
			if(jdbcResultSet instanceof AsynTranResultSet)
				esTranResultSet = new AsynSplitTranResultSet(importContext,(AsynTranResultSet)jdbcResultSet);
			else
				this.jdbcResultSet = new SplitTranResultSet(importContext,jdbcResultSet);
		}
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
		jdbcResultSet.setBaseDataTran(this);
//		init();
	}

	public void init(){

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

	public abstract void logTaskStart(Logger logger);
	public String tran() throws ESDataImportException {
		try {
			this.getDataTranPlugin().setHasTran();
			if (jdbcResultSet == null)
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
	protected boolean isPrintTaskLog(){
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
			totalCount.setJobEndTime(new Date());
			if(isPrintTaskLog()) {

				logger.info(new StringBuilder().append("Parallel batch import Complete tasks:")
						.append(count).append(",Total success import ")
						.append(totalCount.getSuccessCount()).append(" records,Ignore Total ")
						.append(totalCount.getIgnoreTotalCount()).append(" records,failed total ")
						.append(totalCount.getFailedCount()).append(" records.").toString());
			}
			jobComplete(  service,exception,lastValue ,tranErrorWrapper,this.currentStatus,reachEOFClosed);
			totalCount.setJobEndTime(new Date());
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
	public Object getLastValue() throws ESDataImportException {

		if(!importContext.useFilePointer()) {
			if (importContext.getLastValueColumnName() == null) {
				return null;
			}
			return jdbcResultSet.getLastValue(importContext.getLastValueColumnName());
		}
		else{
			return jdbcResultSet.getLastOffsetValue();
		}
//		try {
//			if (importContext.getLastValueType() == null || importContext.getLastValueType().intValue() == ImportIncreamentConfig.NUMBER_TYPE)
//				return jdbcResultSet.getValue(importContext.getLastValueClumnName());
//			else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
//				return jdbcResultSet.getDateTimeValue(importContext.getLastValueClumnName());
//			}
//		}
//		catch (ESDataImportException e){
//			throw (e);
//		}
//		catch (Exception e){
//			throw new ESDataImportException(e);
//		}
//		return null;


	}

	public boolean isTranFinished() {
		return tranFinished;
	}
}
