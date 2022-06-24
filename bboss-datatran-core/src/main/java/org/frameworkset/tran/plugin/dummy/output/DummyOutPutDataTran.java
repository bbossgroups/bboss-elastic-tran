package org.frameworkset.tran.plugin.dummy.output;

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.ouput.custom.CustomOutPutDataTran;
import org.frameworkset.tran.ouput.dummy.DummyTaskCommandImpl;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.StringTranJob;
import org.frameworkset.tran.task.TaskCall;
import org.frameworkset.tran.util.TranUtil;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DummyOutPutDataTran extends CustomOutPutDataTran {
	protected DummyOutputConfig dummyOupputConfig ;
//	protected String fileName;
//	protected String remoteFileName;

	public DummyOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext, currentStatus);
		dummyOupputConfig = (DummyOutputConfig) importContext.getOutputConfig();
	}
	public DummyOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, CountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext, countDownLatch, currentStatus);
	}


	public void init(){
		super.init();

//		dummyOupputContext = (DummyOupputContext)targetImportContext;
		taskInfo = new StringBuilder().append("import data to dummy").toString();


	}
	public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
		CommonRecord record = buildRecord(  context );
		dummyOupputConfig.generateReocord(context,record, writer);
		if(dummyOupputConfig.isPrintRecord()) {
			writer.write(TranUtil.lineSeparator);
		}
		return record;
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {

				if(datas != null )  {
					taskNo++;
					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas((String)datas);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));


				}
				return taskNo;
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return DummyOutPutDataTran.this.buildStringRecord(context,writer);
			}

		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed){
				if(datas != null )  {
					taskNo++;
					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, totalCount.getJobNo(), lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas((String)datas);
					TaskCall.call(taskCommand);

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


			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return DummyOutPutDataTran.this.buildStringRecord(context,writer);
			}
		};
	}

	@Override
	protected void initTranJob(){
		tranJob = new StringTranJob();
	}






}
