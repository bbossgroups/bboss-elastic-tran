package org.frameworkset.tran.plugin.dummy.output;

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.custom.output.CustomOutPutDataTran;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
import org.frameworkset.tran.util.TranUtil;

import java.io.Writer;

public class DummyOutPutDataTran extends CustomOutPutDataTran {
	protected DummyOutputConfig dummyOupputConfig ;
//	protected String fileName;
//	protected String remoteFileName;


	public DummyOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext, countDownLatch, currentStatus);
		dummyOupputConfig = (DummyOutputConfig) importContext.getOutputConfig();
	}

	@Override
	public void init(){
		super.init();

//		dummyOupputContext = (DummyOupputContext)targetImportContext;
		taskInfo = new StringBuilder().append("import data to dummy").toString();


	}

	public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//		CommonRecord record = buildRecord(  context );
//		if(record.getDatas() == null){
//			Record dataRecord = context.getCurrentRecord();
//			if(dataRecord instanceof CommonStringRecord) {
//				record.setOringeData(dataRecord.getData());
//			}
//		}
		CommonRecord record = context.getCommonRecord();
		dummyOupputConfig.generateReocord(taskContext,context.getTaskMetrics(),record, writer);
		if(dummyOupputConfig.isPrintRecord()) {
			writer.write(TranUtil.lineSeparator);
		}
		return record;
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {

				if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(  taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    taskCommandContext.addTask(taskCommand);


				}
				return taskCommandContext.getTaskNo();
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return DummyOutPutDataTran.this.buildStringRecord(context,writer);
			}

		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(TaskCommandContext taskCommandContext){
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
                    DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(  taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
//				if(datas != null )  {
//					taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//					DummyTaskCommandImpl taskCommand = new DummyTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), lastValue,  currentStatus,taskContext);
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
//
//				}
				return taskCommandContext.getTaskNo();
			}
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//					TaskCall.call(taskCommand);
//					taskNo++;
//				}
				return action(  taskCommandContext);
			}

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
				return action(  taskCommandContext);

			}


			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return DummyOutPutDataTran.this.buildStringRecord(context,writer);
			}
		};
	}

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}






}
