package org.frameworkset.tran.output.excelftp;

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.output.fileftp.FileFtpOutPutDataTran;
import org.frameworkset.tran.output.fileftp.FileTransfer;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TaskCall;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ExcelFileFtpOutPutDataTran extends FileFtpOutPutDataTran {


	@Override
	protected FileTransfer buildFileTransfer(FileOutputConfig fileOutputConfig)  {
		FileTransfer fileTransfer = new ExcelFileTransfer( (ExcelFileOutputConfig) fileOutputConfig,path,this);

		return fileTransfer;

	}

	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
	}
	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,countDownLatch,  currentStatus);
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, taskContext.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}
			@Override
			public boolean splitCheck(long totalCount) {
				return _splitCheck( totalCount);
			}


			@Override
			public void parrelCompleteAction() {
//				fileTransfer.sendFile();//传输文件
				sendFile();
			}
		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed){
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,
							dataSize, taskNo, taskContext.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
					taskCommand.setDatas(records);
					TaskCall.call(taskCommand);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
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
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
				sendFile();
				return taskNo;

			}

//			@Override
//			public int splitSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
//				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
//				sendFile(true);
//				return taskNo;
//			}
//
			@Override
			public boolean splitCheck(long totalCount) {
				return _splitCheck( totalCount);
			}

		};
	}
	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}







}
