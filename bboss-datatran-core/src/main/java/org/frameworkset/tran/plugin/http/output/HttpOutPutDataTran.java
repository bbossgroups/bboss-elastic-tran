package org.frameworkset.tran.plugin.http.output;

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.StringTranJob;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class HttpOutPutDataTran extends BaseCommonRecordDataTran {
	protected HttpOutputConfig httpOutputConfig;
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
	protected String taskInfo ;

	@Override
	public void stop(){
		if(asynTranResultSet != null) {
			asynTranResultSet.stop();
			asynTranResultSet = null;
		}
		super.stop();
	}
	/**
	 * 只停止转换作业
	 */
	@Override
	public void stopTranOnly(){
		if(asynTranResultSet != null) {
			asynTranResultSet.stopTranOnly();
			asynTranResultSet = null;
		}
		super.stopTranOnly();
	}

	@Override
	protected void initTranJob(){
		tranJob = new StringTranJob();
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				if(datas != null) {
					taskNo++;
					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(),  lastValue, currentStatus, reachEOFClosed, taskContext);
					taskCommand.setDatas((String) datas);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return HttpOutPutDataTran.this.buildStringRecord(context,writer);
			}

			@Override
			public void parrelCompleteAction() {
			}
		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				if(datas != null) {
					taskNo++;
					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(),  lastValue, currentStatus, reachEOFClosed, taskContext);
					taskCommand.setDatas((String) datas);
					TaskCall.call(taskCommand);

				}
				return taskNo;
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				if(datas != null) {
					taskNo ++;
					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed,taskContext);

					taskCommand.setDatas((String)datas);
					TaskCall.call(taskCommand);
				}
				return taskNo;
			}

			@Override
			public int splitSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				if(datas != null) {
					taskNo++;
					HttpTaskCommandImpl taskCommand = new HttpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(),  lastValue, currentStatus, reachEOFClosed, taskContext);
					taskCommand.setDatas((String)datas);
					TaskCall.call(taskCommand);

				}
				return taskNo;
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

	@Override
	public String tran() throws DataImportException {
		try {
			String ret = super.tran();

			return ret;
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}
	}
	public HttpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,tranResultSet,importContext,   currentStatus);
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();
	}


	public HttpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, CountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,tranResultSet,importContext,   currentStatus);
		this.countDownLatch = countDownLatch;
		httpOutputConfig = (HttpOutputConfig) importContext.getOutputConfig();
	}



	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
		CommonRecord record = buildRecord(  context );
		if(writer == null){
			httpOutputConfig.generateReocord(context, record, writer);
			writer.write(httpOutputConfig.getLineSeparator());

		}
		else {
			if (writer instanceof BBossStringWriter) {
				BBossStringWriter bBossStringWriter = (BBossStringWriter) writer;
				if (bBossStringWriter.getBuffer().length() == 0) {
					if(httpOutputConfig.isJson())
						writer.write("[");
					httpOutputConfig.generateReocord(context, record, writer);

				} else {
					writer.write(httpOutputConfig.getLineSeparator());
					httpOutputConfig.generateReocord(context, record, writer);
				}
			} else {
				httpOutputConfig.generateReocord(context, record, writer);
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
