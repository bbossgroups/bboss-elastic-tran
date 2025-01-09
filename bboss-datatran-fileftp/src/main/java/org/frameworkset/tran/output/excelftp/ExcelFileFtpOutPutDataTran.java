package org.frameworkset.tran.output.excelftp;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.output.fileftp.FileFtpOutPutDataTran;
import org.frameworkset.tran.output.fileftp.FileTransfer;
import org.frameworkset.tran.plugin.file.output.ExcelFileOutputConfig;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;

public class ExcelFileFtpOutPutDataTran extends FileFtpOutPutDataTran {

    public ExcelFileFtpOutPutDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	

	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
	}
	public ExcelFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,countDownLatch,  currentStatus);
	}
    @Override
    protected FileTransfer buildFileTransfer(FileOutputConfig fileOutputConfig)  {
        FileTransfer fileTransfer = new ExcelFileTransfer( (ExcelFileOutputConfig) fileOutputConfig,path,this);

        return fileTransfer;

    }
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
//                    List<CommonRecord> records = convertDatas( datas);
                    ExcelFileFtpTaskCommandImpl taskCommand = (ExcelFileFtpTaskCommandImpl) _buildTaskCommand(taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    taskCommandContext.addTask(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					taskNo++;
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,taskContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//
//				}
//				return taskNo;
			}
//			@Override
//			public boolean splitCheck(long totalCount) {
//				return _splitCheck( totalCount);
//			}


			@Override
			public void parrelCompleteAction() {
//				fileTransfer.sendFile();//传输文件
				sendFile();
			}
		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(TaskCommandContext taskCommandContext){
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
//                    List<CommonRecord> records = convertDatas( datas);
                    ExcelFileFtpTaskCommandImpl taskCommand = (ExcelFileFtpTaskCommandImpl) _buildTaskCommand(taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					taskNo++;
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,taskContext);
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
//
//				}
//				return taskNo;
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
				int taskNo = action(  taskCommandContext);
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
//			@Override
//			public boolean splitCheck(long totalCount) {
//				return _splitCheck( totalCount);
//			}

		};
	}
//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}
    @Override
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext) {
        return new ExcelFileFtpTaskCommandImpl(taskCommandContext, (ExcelFileTransfer) fileTransfer,outputPlugin.getOutputConfig());
    }






}
