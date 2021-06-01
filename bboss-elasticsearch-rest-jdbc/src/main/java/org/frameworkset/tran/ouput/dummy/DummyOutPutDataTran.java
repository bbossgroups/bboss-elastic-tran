package org.frameworkset.tran.ouput.dummy;

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCall;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DummyOutPutDataTran extends BaseCommonRecordDataTran {
	protected DummyOupputContext dummyOupputContext ;
//	protected String fileName;
//	protected String remoteFileName;

	protected CountDownLatch countDownLatch;
	@Override
	public void logTaskStart(Logger logger) {
//		StringBuilder builder = new StringBuilder().append("import data to db[").append(importContext.getDbConfig().getDbUrl())
//				.append("] dbuser[").append(importContext.getDbConfig().getDbUser())
//				.append("] insert sql[").append(es2DBContext.getTargetSqlInfo() == null ?"": es2DBContext.getTargetSqlInfo().getOriginSQL()).append("] \r\nupdate sql[")
//					.append(es2DBContext.getTargetUpdateSqlInfo() == null?"":es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("] \r\ndelete sql[")
//					.append(es2DBContext.getTargetDeleteSqlInfo() == null ?"":es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("] start.");
		logger.info(taskInfo + " start.");
	}
	private String taskInfo ;

	@Override
	public void stop(){
		if(esTranResultSet != null) {
			esTranResultSet.stop();
			esTranResultSet = null;
		}
		super.stop();
	}
	/**
	 * 只停止转换作业
	 */
	@Override
	public void stopTranOnly(){
		if(esTranResultSet != null) {
			esTranResultSet.stopTranOnly();
			esTranResultSet = null;
		}
		super.stopTranOnly();
	}
	public void init(){
		super.init();

		dummyOupputContext = (DummyOupputContext)targetImportContext;
		taskInfo = new StringBuilder().append("import data to dummy").toString();


	}

	@Override
	public String tran() throws ESDataImportException {
		try {
			String ret = super.tran();

			return ret;
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}
	}
	public DummyOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
	}
	public DummyOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}


	public String serialExecute(){
		logger.info("serial import data Execute started.");
		StringBuilder builder = null;
		BBossStringWriter writer = null;
		if(dummyOupputContext.isPrintRecord()) {
			builder = new StringBuilder();
			writer = new BBossStringWriter(builder);
		}
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount();
		int taskNo = 0;
		long totalCount = 0;
		int count = 0;
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;

		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;
			Param param = null;
//			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					String ret = null;
					if(count > 0) {
						String _dd = null;
						if(dummyOupputContext.isPrintRecord()) {
							_dd = builder.toString();
							builder.setLength(0);
						}
						int _count = count;
						count = 0;
						DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(importCount, importContext,targetImportContext,
								_count, taskNo, importCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
						taskCommand.setDatas(_dd);
						ret = TaskCall.call(taskCommand);
						taskNo ++;
					}
					else{
						ret = "{\"took\":0,\"errors\":false}";
					}
//					importContext.flushLastValue(lastValue);
					if(isPrintTaskLog()) {

						long end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Force flush datas Take time:").append((end - start)).append("ms")
								.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
								.append(ignoreTotalCount).append(" records.").toString());

					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				try {
					if (lastValue == null)
						lastValue = importContext.max(currentValue, getLastValue());
					else {
						lastValue = importContext.max(lastValue, getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(taskContext,jdbcResultSet, null);
					if(!reachEOFClosed)
						reachEOFClosed = context.reachEOFClosed();
					if(context.removed()){
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					CommonRecord record = buildRecord(  context );
					dummyOupputContext.generateReocord(context, record, writer);
					if(dummyOupputContext.isPrintRecord()) {

						writer.write(TranUtil.lineSeparator);
					}
//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					count ++;

				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			if(count > 0) {

				DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(importCount, importContext,targetImportContext,
						count, taskNo, importCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
				taskNo ++;
				if(dummyOupputContext.isPrintRecord()) {
					taskCommand.setDatas(builder.toString());
					builder.setLength(0);
				}
				TaskCall.call(taskCommand);

			}

			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(importCount.getIgnoreTotalCount()).append(" records.").toString());

			}
		}
		catch (DataImportException e){
			exception = e;
			throw e;


		}
		catch (Exception e){
			exception = e;
			throw new DataImportException(e);


		} finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
			if(importContext.isCurrentStoped()){

					this.stopTranOnly();

			}
			importCount.setJobEndTime(new Date());
		}
		return null;

	}

	/**
	 * 并行批处理导入

	 * @return
	 */
	public String parallelBatchExecute( ){
		int count = 0;
		StringBuilder builder = null;
		BBossStringWriter writer = null;
		if(dummyOupputContext.isPrintRecord()) {
			builder = new StringBuilder();
			writer = new BBossStringWriter(builder);
		}
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount();
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {

			BatchContext batchContext = new BatchContext();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					jdbcResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){//强制flush操作
					if (count > 0) {
						String datas = null;
						if(dummyOupputContext.isPrintRecord()) {
							datas = builder.toString();
							builder.setLength(0);
						}


						int _count = count;
						count = 0;
						DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,targetImportContext,
								_count, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
						taskCommand.setDatas(datas);
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
						taskNo++;

					}
					continue;
				}
				else if(!hasNext.booleanValue())
					break;

				if(lastValue == null)
					lastValue = importContext.max(currentValue,getLastValue());
				else{
					lastValue = importContext.max(lastValue,getLastValue());
				}
				Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(context.removed()){
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
				CommonRecord record = buildRecord(  context );
				dummyOupputContext.generateReocord(context,record, writer);
				if(dummyOupputContext.isPrintRecord()) {
					writer.write(TranUtil.lineSeparator);
				}
				count++;
				if(count >= batchsize ){
					String datas = null;
					if(dummyOupputContext.isPrintRecord()) {
						datas = builder.toString();
						builder.setLength(0);
					}

					int _count = count;
					count = 0;
					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,targetImportContext,
							_count, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
					taskCommand.setDatas(datas);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
					taskNo++;

				}
			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
				String datas = null;
				if(dummyOupputContext.isPrintRecord()) {
					datas = builder.toString();
					builder.setLength(0);
				}
				DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,targetImportContext,
						count, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
				taskCommand.setDatas(datas);
				tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());

			}
			else{
				if(isPrintTaskLog())
					logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());
			}


		} catch (SQLException e) {
			exception = e;
			throw new ElasticSearchException(e);

		} catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {
			waitTasksComplete(tasks, service, exception, lastValue, totalCount, tranErrorWrapper, (WaitTasksCompleteCallBack)null,reachEOFClosed);
			try {
				writer.close();
			} catch (Exception e) {

			}
			totalCount.setJobEndTime(new Date());
		}

		return ret;
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	public String batchExecute(  ){
		int count = 0;
		StringBuilder builder = null;
		BBossStringWriter writer = null;
		if(dummyOupputContext.isPrintRecord()) {
			builder = new StringBuilder();
			writer = new BBossStringWriter(builder);
		}
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		long start = System.currentTimeMillis();
		long istart = 0;
		long end = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;

		ImportCount importCount = new SerialImportCount();
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {
			istart = start;
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {
						String _dd = null;
						if(dummyOupputContext.isPrintRecord()) {
							_dd = builder.toString();
							builder.setLength(0);
						}
						int _count = count;
						count = 0;
						DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(importCount, importContext,targetImportContext,
								_count, taskNo, importCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
						taskCommand.setDatas(_dd);
						ret = TaskCall.call(taskCommand);


						if (isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(_count).append(" records.").toString());
							istart = end;
						}
						taskNo ++;

					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				if(lastValue == null)
					lastValue = importContext.max(currentValue,getLastValue());
				else{
					lastValue = importContext.max(lastValue,getLastValue());
				}
				Context context = importContext.buildContext(taskContext,jdbcResultSet, batchContext);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(context.removed()){
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
				CommonRecord record = buildRecord(  context );

				dummyOupputContext.generateReocord(context,record, writer);
				if(dummyOupputContext.isPrintRecord()) {
					writer.write(TranUtil.lineSeparator);
				}
				count++;
				totalCount ++;
				if (count >= batchsize) {
					String datas = null;
					if(dummyOupputContext.isPrintRecord()) {
						writer.flush();
						datas = builder.toString();
						builder.setLength(0);
						writer.close();
						writer = new BBossStringWriter(builder);
					}
					int _count = count;
					count = 0;
					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(importCount, importContext,targetImportContext,
							_count, taskNo, importCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed);
					taskCommand.setDatas(datas);
					ret = TaskCall.call(taskCommand);



					if(isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}
					taskNo ++;

				}


			}
			if (count > 0) {
				String datas = null;
				if(dummyOupputContext.isPrintRecord()) {
					writer.flush();
					datas = builder.toString();
				}

				DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(importCount, importContext, targetImportContext,
						count, taskNo, importCount.getJobNo(),  lastValue,  currentStatus,reachEOFClosed);
				taskCommand.setDatas(datas);
				TaskCall.call(taskCommand);
				if(isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
			}

			if(isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
		} catch (SQLException e) {
			exception = e;
			throw new ElasticSearchException(e);

		} catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
			try {
				writer.close();
			} catch (Exception e) {

			}
			importCount.setJobEndTime(new Date());
		}

		return ret;
	}





}
