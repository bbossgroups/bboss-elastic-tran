package org.frameworkset.tran;

import org.frameworkset.elasticsearch.scroll.BreakableScrollHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.List;
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
	public AsynTranResultSet getAsynTranResultSet(){
		return esTranResultSet;
	}
	public void appendData(Data data){
		getAsynTranResultSet().appendData(data);
	}


//	private CountDownLatch countDownLatch;
	public BreakableScrollHandler getBreakableScrollHandler() {
		return breakableScrollHandler;
	}

	public void setBreakableScrollHandler(BreakableScrollHandler breakableScrollHandler) {
		this.breakableScrollHandler = breakableScrollHandler;
	}

	private BreakableScrollHandler breakableScrollHandler;
	public BaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext) {
		this.taskContext = taskContext;
		this.jdbcResultSet = jdbcResultSet;
		if(jdbcResultSet instanceof AsynTranResultSet)
			esTranResultSet = (AsynTranResultSet)jdbcResultSet;
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
//		init();
	}

	public void init(){

	}






	protected void stop(){
		if(breakableScrollHandler != null) {
			breakableScrollHandler.setBreaked(true);
		}
		importContext.stop();

	}
	public abstract void logTaskStart(Logger logger);
	public String tran() throws ESDataImportException {
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
	protected void jobComplete(ExecutorService service,Exception exception,Object lastValue ,TranErrorWrapper tranErrorWrapper){
		if (importContext.getScheduleService() == null) {//作业定时调度执行的话，需要关闭线程池
			service.shutdown();
		}
		else{
			if(tranErrorWrapper.assertCondition(exception)){
				importContext.flushLastValue( lastValue );
			}
			else{
				service.shutdown();
				this.stop();
			}
		}
	}
	protected boolean isPrintTaskLog(){
		return importContext.isPrintTaskLog() && logger.isInfoEnabled();
	}
	public void waitTasksComplete(final List<Future> tasks,
								   final ExecutorService service,Exception exception,Object lastValue,
								  final ImportCount totalCount ,final TranErrorWrapper tranErrorWrapper ,final WaitTasksCompleteCallBack waitTasksCompleteCallBack){
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
			jobComplete(  service,exception,lastValue ,tranErrorWrapper);
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

					jobComplete(  service,null,null,tranErrorWrapper);
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




	public Object getLastValue() throws ESDataImportException {


		if(importContext.getLastValueColumnName() == null){
			return null;
		}
		return jdbcResultSet.getLastValue(importContext.getLastValueColumnName());
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

}
