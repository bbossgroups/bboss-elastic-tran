package org.frameworkset.tran.plugin.http.output;

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.BaseCommonRecordDataTran;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
import org.slf4j.Logger;

import java.io.Writer;

public class HttpOutPutDataTran extends BaseCommonRecordDataTran {
	protected HttpOutputConfig httpOutputConfig;
//	protected String fileName;
//	protected String remoteFileName;

//	protected JobCountDownLatch countDownLatch;
	@Override
	public void logTaskStart(Logger logger) {
//		StringBuilder builder = new StringBuilder().append("import data to db[").append(importContext.getDbConfig().getDbUrl())
//				.append("] dbuser[").append(importContext.getDbConfig().getDbUser())
//				.append("] insert sql[").append(es2DBContext.getTargetSqlInfo() == null ?"": es2DBContext.getTargetSqlInfo().getOriginSQL()).append("] \r\nupdate sql[")
//					.append(es2DBContext.getTargetUpdateSqlInfo() == null?"":es2DBContext.getTargetUpdateSqlInfo().getOriginSQL()).append("] \r\ndelete sql[")
//					.append(es2DBContext.getTargetDeleteSqlInfo() == null ?"":es2DBContext.getTargetDeleteSqlInfo().getOriginSQL()).append("] start.");
		logger.info(taskInfo + " start.");
	}
	protected String taskInfo ;

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

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
                    HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(  taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    taskCommandContext.addTask(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(),  lastValue, currentStatus,  taskContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//
//				}
//				return taskNo;
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return HttpOutPutDataTran.this.buildStringRecord(context,writer);
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
                    HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(  taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(),  lastValue, currentStatus,  taskContext);
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
//
//				}
//				return taskNo;
			}

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
                    HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(  taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo ++;
//                    List<CommonRecord> records = convertDatas( datas);
//					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), lastValue,  currentStatus,taskContext);
//
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
//				}
//				return taskNo;
			}




			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return HttpOutPutDataTran.this.buildStringRecord(context,writer);
			}
		};
	}
	public void init(){
		super.init();

 		taskInfo = "Http output datatran job";

	}


	public HttpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,tranResultSet,importContext,   currentStatus);
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();
	}


	public HttpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,tranResultSet,importContext,   currentStatus);
		this.countDownLatch = countDownLatch;
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();
	}



	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//		CommonRecord record = buildRecord(  context );
//		if(record.getDatas() == null){
//			Record dataRecord = context.getCurrentRecord();
//			if(dataRecord instanceof CommonStringRecord) {
//				record.setOringeData(dataRecord.getData());
//			}
//		}
		CommonRecord record = context.getCommonRecord();
		if(writer == null){
			httpOutputConfig.generateReocord(taskContext, record, writer);
			writer.write(httpOutputConfig.getLineSeparator());

		}
		else {
			if (writer instanceof BBossStringWriter) {
				BBossStringWriter bBossStringWriter = (BBossStringWriter) writer;
				if (bBossStringWriter.getBuffer().length() == 0) {
					if(httpOutputConfig.isJson())
						writer.write("[");
					httpOutputConfig.generateReocord(taskContext, record, writer);

				} else {
					writer.write(httpOutputConfig.getLineSeparator());
					httpOutputConfig.generateReocord(taskContext, record, writer);
				}
			} else {
				httpOutputConfig.generateReocord(taskContext, record, writer);
				writer.write(httpOutputConfig.getLineSeparator());
			}
		}
		return record;

	}

	@Override
	public void beforeOutputData(BBossStringWriter writer){
		if(httpOutputConfig.isJson() && writer.getBuffer().length() >  0){
			writer.write("]");
		}

	}







}
