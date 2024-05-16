package org.frameworkset.tran.output.fileftp;

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.TaskContextReinitCallback;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.input.file.GenFileInfo;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.schedule.JobExecuteMetric;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
import org.slf4j.Logger;

import java.io.File;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;

public class FileFtpOutPutDataTran extends BaseCommonRecordDataTran {

	protected FileOutputConfig fileOutputConfig;
	protected FileTransfer fileTransfer;
	protected String path;
	protected int fileSeq = 1;

//	protected JobCountDownLatch countDownLatch;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo + " start.");
	}
	protected String taskInfo ;


    private Object resetFileSeqLock = new Object(); 
    public void resetFileSeq(){
        synchronized (resetFileSeqLock) {
            fileSeq = 1;
        }
    }
	protected String[] generateFileName(){
		String[] fileInfo = null;
        String oldName = null;
		do {
			String name = fileOutputConfig.generateFileName(taskContext, fileSeq);
            if(name == null || name.equals("")){
                throw ImportExceptionUtil.buildDataImportException(importContext,"fileOutputConfig.generateFileName with fileSeq="+fileSeq+" failed: return name="+name);
            }
            if(oldName != null ){
                if(name.equals(oldName))
                    throw ImportExceptionUtil.buildDataImportException(importContext,"fileOutputConfig.generateFileName with fileSeq="+fileSeq+" failed: 每次返回的文件名重复，name="+name+",如果需要替换已存在文件，可以设置：fileOupputConfig.setExistFileReplace(true)替换重名文件");

            }
            oldName = name;

			String fileName = SimpleStringUtil.getPath(path, name);
			String remoteFileName = !fileOutputConfig.isDisableftp() ? SimpleStringUtil.getPath(fileOutputConfig.getRemoteFileDir(), name) : null;
            synchronized (resetFileSeqLock) {
                fileSeq++;
            }
			File file = new File(fileName);
			if(!file.exists()){
				fileInfo = new String[]{name,fileName,remoteFileName};
				break;
			}
            else{
                if(fileOutputConfig.getMaxFileRecordSize() <= 0){
                    if(fileOutputConfig.isExistFileReplace()){
                        try {
                            file.delete();
                        }
                        catch (Exception e){
                            logger.warn("Delete exist file["+fileName+"] failed:",e);
                        }
                        fileInfo = new String[]{name,fileName,remoteFileName};
                        break;
                    }
                }
            }
		}while (true);
		return fileInfo;
	}

	protected void traceFile(String localFile,String remoteFile){
		//添加文件信息到任务监控信息中
		if(fileOutputConfig.isEnableGenFileInfoMetric() && taskContext != null) {
			taskContext.taskExecuteMetric(new JobExecuteMetric() {
				@Override
				public void executeMetric(JobTaskMetrics jobTaskMetrics) {
					List<GenFileInfo> genFileInfos = (List<GenFileInfo>) jobTaskMetrics.readJobExecutorData(FileOutputConfig.JobExecutorDatas_genFileInfos);

					if (genFileInfos == null) {

						genFileInfos = new ArrayList<>();
						jobTaskMetrics.putJobExecutorData(FileOutputConfig.JobExecutorDatas_genFileInfos, genFileInfos);
					}

					GenFileInfo genFileInfo = new GenFileInfo();
					genFileInfo.setLocalFile(localFile);
					genFileInfo.setRemoteFile(remoteFile);
					genFileInfos.add(genFileInfo);
				}
			});
		}
	}
	protected FileTransfer buildFileTransfer(FileOutputConfig fileOutputConfig) {
		FileTransfer fileTransfer = new FileTransfer(fileOutputConfig,path,this);
		return fileTransfer;
	}
	protected FileTransfer initFileTransfer(){
		path = fileOutputConfig.getFileDir();
        FileTransfer fileTransfer = buildFileTransfer(  fileOutputConfig) ;

        if(fileOutputConfig.transferEmptyFiles()){
            fileTransfer.init();
            taskInfo = fileTransfer.getTaskInfo();
        }
        return fileTransfer;
	}



	protected boolean _splitCheck(long totalCount) {
		return fileOutputConfig.getMaxFileRecordSize() > 0 && totalCount > 0
				&& (totalCount % fileOutputConfig.getMaxFileRecordSize() == 0);
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
                    FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(taskCommandContext, fileTransfer);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    taskCommandContext.addTask(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), fileTransfer, lastValue, currentStatus, taskContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//
//				}
//				return taskNo;
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return FileFtpOutPutDataTran.this.buildStringRecord(context,writer);
			}
			@Override
			public boolean splitCheck(long totalCount) {
				return _splitCheck( totalCount);
			}
			@Override
			public void parrelCompleteAction() {
//				fileTransfer.sendFile();//传输文件
//				sendFile();
			}
		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                    List<CommonRecord> records = convertDatas( datas);
                    FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(taskCommandContext, fileTransfer);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), fileTransfer, lastValue, currentStatus,  taskContext);
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
                    FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(taskCommandContext, fileTransfer);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(taskCommand);


                }
                sendFile();
                return taskCommandContext.getTaskNo();
//				if(datas != null) {
//					taskNo ++;
//                    List<CommonRecord> records = convertDatas( datas);
//					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,
//							dataSize, taskNo, taskContext.getJobNo(), fileTransfer,lastValue,  currentStatus,taskContext);
//
//					taskCommand.setRecords(records);
//					TaskCall.call(taskCommand);
//				}
//				sendFile();
//				return taskNo;
			}

			@Override
			public boolean splitCheck(long totalCount) {
				return _splitCheck( totalCount);
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return FileFtpOutPutDataTran.this.buildStringRecord(context,writer);
			}
		};
	}
	public void init(){
		super.init();


		fileTransfer = initFileTransfer();


	}

	@Override
	protected String commonTran() throws DataImportException {
        try {
            String ret = super.commonTran();
            return ret;

        }
        finally {
            fileTransfer.close();
            fileTransfer = null;
        }

//		try {
//			String ret = super.tran();
//			fileTransfer.close();
//			fileTransfer = null;
//			return ret;
//		}
//		catch (DataImportException dataImportException){
//			if(this.countDownLatch != null)
//				countDownLatch.attachException(dataImportException);
//			throw dataImportException;
//		}
//		catch (Exception dataImportException){
//			if(this.countDownLatch != null)
//				countDownLatch.attachException(dataImportException);
//			throw new DataImportException(dataImportException);
//		}
//		catch (Throwable dataImportException){
//			if(this.countDownLatch != null)
//				countDownLatch.attachException(dataImportException);
//			throw new DataImportException(dataImportException);
//		}
//		finally {
//			if(this.countDownLatch != null)
//				countDownLatch.countDown();
//		}
	}
	public FileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,   currentStatus);
        taskContext.setTaskContextReinitCallback(new TaskContextReinitCallback() {
            @Override
            public void taskContextReinitCallback(TaskContext taskContext) {
                resetFileSeq();
            }
        });
		fileOutputConfig = (FileOutputConfig) importContext.getOutputConfig();
		taskInfo = "Import data to file";
	}


	public FileFtpOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,  JobCountDownLatch countDownLatch,Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,   currentStatus);
        taskContext.setTaskContextReinitCallback(new TaskContextReinitCallback() {
            @Override
            public void taskContextReinitCallback(TaskContext taskContext) {
                resetFileSeq();
            }
        });
		this.countDownLatch = countDownLatch;
		fileOutputConfig = (FileOutputConfig) importContext.getOutputConfig();
		taskInfo = "Import data to file";
	}

	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件?????
     * 分析一下，为什么不能并行生产文件,目前已经调整为并行处理和生成数据文件

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ) {
        /**
		if (!fileOutputConfig.isDisableftp()) {
			return batchExecute();
		}*/
		return super.parallelBatchExecute() ;
	}



	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
		CommonRecord record = context.getCommonRecord();
		fileOutputConfig.generateReocord(context.getTaskContext(),record, writer);
		writer.write(fileOutputConfig.getLineSeparator());
		return record;
	}


	protected void sendFile(){
			fileTransfer.sendFile2ndStopCheckers();
	}
    @Override
    public void stop(boolean fromException){
        if(dataTranStopped)
            return;
        sendFile();//串行执行时，sendFile将不起作用
        super.stop(fromException);
    }

    @Override
    public void stop2ndClearResultsetQueue(boolean fromException){
        if(dataTranStopped)
            return;
        sendFile();//串行执行时，sendFile将不起作用
        super.stop2ndClearResultsetQueue(fromException);
    }






}
