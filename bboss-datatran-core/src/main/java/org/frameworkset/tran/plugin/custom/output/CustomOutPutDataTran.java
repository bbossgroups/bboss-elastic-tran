package org.frameworkset.tran.plugin.custom.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;

public class CustomOutPutDataTran extends AbstraCommonRecordOutPutDataTran {

//	protected JobCountDownLatch countDownLatch;

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

//	@Override
//	public void stop(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stop();
//			asynTranResultSet = null;
//		}
//		super.stop();
//	}
//	/**
//	 * 只停止转换作业
//	 */
//	@Override
//	public void stopTranOnly(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stopTranOnly();
//			asynTranResultSet = null;
//		}
//		super.stopTranOnly();
//	}



	public void init(){
		super.init();
		taskInfo = new StringBuilder().append("Import data to custom output.").toString();


	}



	public CustomOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,  Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,   currentStatus);
	}
	public CustomOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,  JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}

    protected  TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(taskCommandContext);
        return taskCommand;
    }
//	@Override
//	protected void initTranTaskCommand(){
//		parrelTranCommand = new BaseParrelTranCommand(){
//
//			@Override
//			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas,
//										  ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					taskNo++;
//					CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), lastValue,  currentStatus,taskContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//
//				}
//				return taskNo;
//			}
//
//
//		};
//		serialTranCommand = new BaseSerialTranCommand() {
//			private int action(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas){
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					taskNo++;
//					CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), lastValue,  currentStatus,taskContext);
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
////						importContext.flushLastValue(lastValue);
//
//				}
//				return taskNo;
//			}
//			@Override
//			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
////				List<CommonRecord> records = convertDatas( datas);
////				if(records != null && records.size() > 0)  {
////					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
////							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
////					taskCommand.setDatas(records);
////					TaskCall.call(taskCommand);
////					taskNo++;
////				}
//				return action(totalCount, dataSize, taskNo, lastValue, datas);
//			}
//
//			@Override
//			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
//				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas);
//				return taskNo;
//
//			}
//
//
//		};
//	}

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}

}
