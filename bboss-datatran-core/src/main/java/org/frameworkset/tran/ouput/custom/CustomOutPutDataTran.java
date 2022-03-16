package org.frameworkset.tran.ouput.custom;

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class CustomOutPutDataTran extends BaseCommonRecordDataTran {

	protected CountDownLatch countDownLatch;

	protected String taskInfo ;
	@Override
	public void logTaskStart(Logger logger) {
//		StringBuilder builder = new StringBuilder().append("import data to db[").append(importContext.getDbConfig().getDbUrl())
//				.append("] dbuser[").append(importContext.getDbConfig().getDbUser())
//				.append("] insert sql[").append(es2DBContext.getTargetSqlInfo() == null ?"": es2DBContext.getTargetSqlInfo().getOriginSQL()).append("] \r\nupdate sql[")
//					.append(es2DBContext.getTargetUpdateSqlInfo() == null?"":es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("] \r\ndelete sql[")
//					.append(es2DBContext.getTargetDeleteSqlInfo() == null ?"":es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("] start.");
		logger.info(taskInfo + " start.");
	}

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

	public void init(){
		super.init();
		taskInfo = new StringBuilder().append("Import data to custom output.").toString();


	}



	public CustomOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
	}
	public CustomOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, targetImportContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(totalCount, importContext,targetImportContext,
							dataSize, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed){
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(totalCount, importContext,targetImportContext,
							dataSize, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//					TaskCall.call(taskCommand);
//					taskNo++;
//				}
				return action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
				return taskNo;

			}


		};
	}

	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}

}
