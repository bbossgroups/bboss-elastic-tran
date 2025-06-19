package org.frameworkset.tran.plugin.file.input;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.frameworkset.tran.AssertMaxThreshold;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.input.file.*;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.status.*;
import org.frameworkset.tran.util.TranConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 16:55
 * @author biaoping.yin
 * @version 1.0
 */
public class FileDataTranPluginImpl extends DataTranPluginImpl {
	private FileInputDataTranPlugin fileInputDataTranPlugin;
	private FileInputConfig fileInputConfig;
    private AssertMaxThreshold assertMaxFilesThreshold;

    protected FileListenerService fileListenerService;
	protected static Logger logger = LoggerFactory.getLogger(FileDataTranPluginImpl.class);
	@Override
	protected InitLastValueClumnName getInitLastValueClumnName(){
		return new InitLastValueClumnName (){

			public void initLastValueClumnName(){
                if(fileInputConfig.isDisableScanNewFiles() && fileInputConfig.isDisableScanNewFilesCheckpoint()){
                    statusManager.setIncreamentImport(false);
                }
			}
		};
	}
	@Override
	public SetLastValueType getSetLastValueType(){
		return new SetLastValueType (){

			public void set(){
				importContext.setLastValueType(ImportIncreamentConfig.NUMBER_TYPE);
                statusManager.initLastValueType();
			}
		};
	}

    public void setFileListenerService(FileListenerService fileListenerService) {
        this.fileListenerService = fileListenerService;
    }

    @Override
	public boolean useFilePointer(){
		return true;
	}

	@Override
	public  void beforeInit() {
		fileInputDataTranPlugin = (FileInputDataTranPlugin) inputPlugin;

		super.beforeInit();



	}



	public FileDataTranPluginImpl(ImportContext importContext){
		super(importContext);

		fileInputConfig = (FileInputConfig) importContext.getInputConfig();
        assertMaxFilesThreshold = fileInputConfig.getAssertMaxFilesThreshold();
	}
	public Status getCurrentStatus(){
		throw new UnsupportedOperationException("getCurrentStatus");
	}

//	@Override
//	protected void checkTranFinished(){
//
//		fileInputDataTranPlugin.checkTranFinished();
//	}



	private List<HistoryTaskStarter> historyFileReaderTasks ;



    interface HistoryTaskStarter{
		void start();
	}

	@Override
	public LoadCurrentStatus getLoadCurrentStatus(){
		return new LoadCurrentStatus(){

			@Override
			public void load() {
				List<Status> statuses  = statusManager.getPluginStatuses();
				loadCurrentStatus( statuses);
			}
		};
	}
	public void continueFailedTask(FileReaderTask fileReaderTask){
        if( assertMaxFilesThreshold != null && !assertMaxFilesThreshold.assertEnableNext())
            return ;
		if (logger.isInfoEnabled())
			logger.info("Start collect failed file {}", fileReaderTask.getFileInfo().getFilePath());
		Status status = fileReaderTask.getCurrentStatus();
		Status _status = statusManager.getStatus(status.getJobId(),status.getJobType(),status.getId());

		if(_status != null) {
			 status.setCurrentLastValueWrapper(_status.getCurrentLastValueWrapper());
		}
        LastValueWrapper lastValueWrapper = status.getCurrentLastValueWrapper();
        Object lastValue = lastValueWrapper.getLastValue();
        long pointer = 0;
        if (lastValue instanceof Long) {
            pointer = (Long) lastValue;
        } else if (lastValue instanceof Integer) {
            pointer = ((Integer) lastValue).longValue();
        } else if (lastValue instanceof Short) {
            pointer = ((Short) lastValue).longValue();
        }


        runFileReadTask(fileReaderTask.getFileInfo().getFileConfig(), status, pointer, true);



    }

//    protected boolean canFinishTran(boolean onceTaskFinish){
//        lock.lock();
//        try{
//            if(!onceTaskFinish) { //如果是一次性任务结束，不需要检查TranConstant.PLUGIN_INIT状态，如果是通过destroy结束任务，则需要判断TranConstant.PLUGIN_INIT
//                return status == TranConstant.PLUGIN_INIT || status == TranConstant.PLUGIN_STOPREADY || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
//            }
//            else{
//                return status == TranConstant.PLUGIN_STOPREADY  || status == TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
//            }
//        }
//        finally {
//            lock.unlock();
//        }
//    }
	public boolean runFileReadTask(FileConfig fileConfig,Status status,long pointer,boolean resetFileInfo){
		//创建一个文件对应的交换通道
        FileTaskContext taskContext = null;
        BaseDataTran fileDataTran = null;
        FileTranThread tranThread = null;
        boolean startTran = false;
        try {
            FileResultSet fileResultSet = new FileResultSet(importContext);
            taskContext = fileInputDataTranPlugin.createFileTaskContext(status, fileConfig);
            preCall(taskContext);//需要在任务完成时销毁taskContext

            fileDataTran = createBaseDataTran(taskContext, fileResultSet, null, status);
        }
        catch (RuntimeException e){
            if( assertMaxFilesThreshold != null){
                assertMaxFilesThreshold.decreament();
            }
            throw e;
        }
        catch (Throwable throwable){
            if( assertMaxFilesThreshold != null){
                assertMaxFilesThreshold.decreament();
            }
            throw throwable;
        }

        if(fileDataTran != null) {
            try {

                String tname = "file-log-tran|" + status.getRealPath();
                if (fileConfig.isEnableInode()) {
                    tname = tname + "|" + status.getFileId();
                }
                tranThread = new FileTranThread(assertMaxFilesThreshold,
                        fileDataTran,
                        taskContext, this);
                tranThread.setName(tname);
                startTran = tranThread.startTran();

                if (startTran) {

                    FileReaderTask task = fileInputDataTranPlugin.buildFileReaderTask(taskContext, new File(status.getRealPath())
                            , status.getFileId()
                            , fileConfig
                            , pointer
                            , fileListenerService, fileDataTran, status, fileInputConfig);
                    if (resetFileInfo) {
                        task.getFileInfo().setOriginFile(new File(status.getFilePath()));
                        task.getFileInfo().setOriginFilePath(status.getFilePath());
                    }
//								taskContext.setFileInfo(task.getFileInfo());
                    if (fileConfig.getAddFields() != null && fileConfig.getAddFields().size() > 0) {
                        task.addFields(fileConfig.getAddFields());
                    }
                    if (fileConfig.getIgnoreFields() != null && fileConfig.getIgnoreFields().size() > 0) {
                        task.ignoreFields(fileConfig.getIgnoreFields());
                    }
                    /**
                     * 根据文件信息动态添加文件标签
                     */
                    if (fileConfig.getFieldBuilder() != null) {
                        fileConfig.getFieldBuilder().buildFields(task.getFileInfo(), task);
                    }


                    fileListenerService.addFileTask(task.getFileId(), task);

                    if (logger.isInfoEnabled())
                        logger.info(tname + " started.");
                    task.start();
                } else {
                    if (assertMaxFilesThreshold != null) {
                        assertMaxFilesThreshold.decreament();
                    }
                }



            } catch (DataImportException e) {
                if (!startTran && assertMaxFilesThreshold != null) {
                    assertMaxFilesThreshold.decreament();
                }
                throwException(taskContext, e);
                fileDataTran.stop2ndClearResultsetQueue(true);

                throw e;
            } catch (Exception e) {
                if (!startTran && assertMaxFilesThreshold != null) {
                    assertMaxFilesThreshold.decreament();
                }
                throwException(taskContext, e);

                fileDataTran.stop2ndClearResultsetQueue(true);
                DataImportException dataImportException = ImportExceptionUtil.buildDataImportException(importContext,e);
                throw dataImportException;
            } catch (Throwable e) {
                if (!startTran && assertMaxFilesThreshold != null) {
                    assertMaxFilesThreshold.decreament();
                }
                throwException(taskContext, e);
                fileDataTran.stop2ndClearResultsetQueue(true);
                DataImportException dataImportException = ImportExceptionUtil.buildDataImportException(importContext,e);
                throw dataImportException;
            }
        }
        return startTran;
	}
	@Override
	public void loadCurrentStatus(List<Status> statuses){


		try {
			/**
			 * 初始化数据检索起始状态信息
			 */
//			List<Status> statuses = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectAllSQL);
			if(statuses == null || statuses.size() == 0){
				return;
			}
			boolean fromFirst = importContext.isFromFirst();

			/**
			 * 已经完成的任务
			 */
			List<Status> completed = new ArrayList<Status>();
			/**
			 * 记录任务存在，但是任务未完成，文件不存在的任务
			 */
			List<Status> lostedFileTasks = new ArrayList<Status>();
//			/**
//			 * 已经过期的任务，修改状态为已完成
//			 */
//			List<Status> olded = new ArrayList<Status>();
            long registLiveTime = fileInputConfig.getRegistLiveTime() != null?fileInputConfig.getRegistLiveTime():0L;
			for(Status status : statuses){
				status.setRealPath(status.getFilePath());
				//判断任务是否已经完成，如果完成，则对任务进行相应处理

				if(isComplete(status)){
                    if(registLiveTime > 0L && statusManager.isOldRegistRecord(status ,registLiveTime)) {
                        completed.add(status);
                    }
                    else {
                        fileListenerService.addCompletedFileTask(status.getFileId(), fileInputDataTranPlugin.buildFileReaderTask(status.getFileId()
                                , status, fileInputConfig));
                        logger.info("Ignore complete file {}",status.getFilePath());
                    }
					continue;
				}

				if(isLostFile(status)){

					fileListenerService.addLostedFileTask(status.getFileId(),fileInputDataTranPlugin.buildFileReaderTask(status.getFileId()
							,status,fileInputConfig));
					logger.info("Ignore losted file {}",status.getFilePath());
					continue;
				}

				FileConfig fileConfig = fileInputDataTranPlugin.getFileConfig(status.getFilePath());
				if(fileConfig == null) {
//                    completed.add(status);
//                    fileListenerService.addCompletedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
//                            ,status));
					logger.info("Ignore file {} which config is removed.",status.getFilePath());
					continue;
				}
				File logFile = new File(status.getFilePath());
				if(fileConfig.isEnableInode()) {

					if(!logFile.exists()){
						File inodeFile = FileInodeHandler.getFileByInode( fileConfig,status.getFileId());
						if(inodeFile != null){
							status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
						}
						else
						{
							continue;
						}
					}
					else {
						String inode = FileInodeHandler.linuxInode(logFile);
						if (inode == null ) {
							File inodeFile = FileInodeHandler.getFileByInode(fileConfig, status.getFileId());

							if (inodeFile != null) {
								logger.info("inodeFile:{},status.fileid:{}",inodeFile.getCanonicalPath(),status.getFileId());
								status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
							}
							else{
								continue;
							}
						}
						else if (!status.getFileId().equals(inode)) {
							File inodeFile = FileInodeHandler.getFileByInode(fileConfig, status.getFileId());
							if (inodeFile != null) {
								logger.info("inode:{},status.fileid:{} 不相等，老path:{},新path:{}",inode,status.getFileId(),status.getFilePath(),inodeFile.getCanonicalPath());
								status.setRealPath(FileInodeHandler.change(inodeFile.getCanonicalPath()));
							}
							else{
								handleOldedTask(status);
								logger.info("status.fileid:{} 对应的文件不存在，老path:{}，忽略本文件采集",status.getFileId(),status.getFilePath());
								continue;
							}
						}
					}
				}
				else {
					if(!logFile.exists()){
						status.setStatus(ImportIncreamentConfig.STATUS_LOSTFILE);
						lostedFileTasks.add(status);
						fileListenerService.addLostedFileTask(status.getFileId(),fileInputDataTranPlugin.buildFileReaderTask(status.getFileId()
								,status,fileInputConfig));
						logger.info("Ignore losted file {}",status.getFilePath());
						continue;
					}
				}

				//需判断文件是否存在，不存在需清除记录


				if(historyFileReaderTasks == null){
					historyFileReaderTasks = new ArrayList<>();
				}
				historyFileReaderTasks.add(new HistoryTaskStarter(){

					@Override
					public void start() {
                        if(assertMaxFilesThreshold != null && !assertMaxFilesThreshold.assertEnableNext())
                            return;

                        long pointer = 0;
                        LastValueWrapper currentLastValueWrapper = status.getCurrentLastValueWrapper();
                        if (!fromFirst){
                            Object lastValue = currentLastValueWrapper.getLastValue();
                            if (lastValue instanceof Long) {
                                pointer = (Long) lastValue;
                            } else if (lastValue instanceof Integer) {
                                pointer = ((Integer) lastValue).longValue();
                            } else if (lastValue instanceof Short) {
                                pointer = ((Short) lastValue).longValue();
                            }

                        }
                        else{
                            currentLastValueWrapper.setLastValue(0l);
                            pointer = 0L;
                        }
						runFileReadTask(  fileConfig,  status,  pointer,true);
					}
				});
			}
//			long registLiveTime = fileInputConfig.getRegistLiveTime() != null?fileInputConfig.getRegistLiveTime():0L;
			if(completed.size() > 0 ){
				statusManager.handleOldedRegistedRecordTasks(completed );
			}
			if(lostedFileTasks.size() > 0){
				statusManager.handleLostedTasks(lostedFileTasks ,false);
			}
//			if(olded.size() > 0){
//				handleOldedTasks(olded);
//			}
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}

	}
	private Object startHistoryTasksLock = new Object();
	private  void startHistoryTasks(){

		if(historyFileReaderTasks == null || historyFileReaderTasks.size() == 0)
			return ;
		synchronized(startHistoryTasksLock) {
			if(historyFileReaderTasks == null || historyFileReaderTasks.size() == 0)
				return ;
			for (HistoryTaskStarter historyTaskStarter : historyFileReaderTasks) {
//			preCall(taskContext);//需要在任务完成时销毁taskContext
//			fileListenerService.addFileTask(task.getFileId(), task);
//			task.start();
				historyTaskStarter.start();
			}
			historyFileReaderTasks = null;
		}
	}
    private boolean neadFinishJob(){
        if(fileInputConfig.isDisableScanNewFiles()){//不扫码新文件
            List<FileConfig> fileConfigs = fileInputConfig.getFileConfigList();
            boolean allEofclose = true;
            if(fileConfigs != null && fileConfigs.size() > 0){

                for(FileConfig fileConfig: fileConfigs){
                    if(fileConfig.isCloseEOF())
                        continue;
                    if(fileConfig.getCloseOlderTime() != null &&  fileConfig.getCloseOlderTime() > 0L)
                        continue;
                    if(fileConfig.getIgnoreOlderTime() != null &&  fileConfig.getIgnoreOlderTime() > 0L)
                        continue;
                    allEofclose = false;
                    break;
                }

            }
            return allEofclose;
        }
        return false;
    }
	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {
//        this.scheduleEndCall = scheduleEndCall;
        if(this.checkTranToStop())//任务处于停止状态，不再执行定时作业
        {
            return;
        }
        startHistoryTasks();
        if (!fileInputConfig.isUseETLScheduleForScanNewFile()) {//采用内置新文件扫描调度机制
            Exception e = null;
            try {
                long importStartTime = System.currentTimeMillis();
                this.doImportData(null);
                long importEndTime = System.currentTimeMillis();
                if (importContext.isPrintTaskLog())
                    logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
            }
            catch (DataImportException _e){
                e = _e;
                throw _e;
            }
            catch (Exception _e){
                e = ImportExceptionUtil.buildDataImportException(importContext.getOutputPlugin(),importContext,"",_e);
                throw (DataImportException)e;
            }
            finally {
                if(neadFinishJob()){//不扫码新文件
                    importContext.finishAndWaitTran(e);
                }
            }

        } else {
            super.importData(scheduleEndCall);
        }



	}
    @Override
    protected void PLUGIN_STOPAPPENDING(){

        fileInputDataTranPlugin.isScanInitOrFinish(new ScanStatusCall() {
            @Override
            public void call(boolean isScanInitOrFinish) {


                lock.lock();
                try {

                    if(status != TranConstant.PLUGIN_STOPREADY) {
                        if(hasTran) {
                            status = TranConstant.PLUGIN_STOPAPPENDING;
                        }
                        else{
                            //三种扫描状态：未开始扫描，扫描开始，扫描结束
                            if(isScanInitOrFinish) {//如果还未开始扫描或者扫描结束
                                status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                                latch.countDown();
                            }
                            else{//如果正在扫描
                                status = TranConstant.PLUGIN_STOPAPPENDING;
                            }
                            
                        }
                    }
                    else {
                        status = TranConstant.PLUGIN_STOPAPPENDING_STOPREADY;
                        latch.countDown();
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        });

    }
    @Override
    public void setNoTran(){
//        super.setNoTran();
        fileInputDataTranPlugin.isScanInitOrFinish(new ScanStatusCall(){

            @Override
            public void call(boolean isScanInitOrFinish) {
                lock.lock();
                try {
                    if (!isScanInitOrFinish) {
                        _setNoTran(false);
                    } else {
                        _setNoTran(true);
                    }
                }
                finally {
                    lock.unlock();
                }
            }
        });

        /**
         * 考虑到异步消息中间件处理的异步行，暂时先不关闭作业
        if(!hasTran){
            if(scheduleEndCall != null &&  fileInputConfig.isDisableScanNewFiles()){
                scheduleEndCall.call(false);
            }
        }*/
    }
	@Override
	protected void initStatusManager(){
		statusManager = new MultiStatusManager(this);
//		statusManager.init();
	}
	@Override
	public void initSchedule(){
		if(!fileInputConfig.isUseETLScheduleForScanNewFile()) {
			logger.info("Ignore initSchedule for plugin {}", this.getClass().getName());
		}
		else{
			super.initSchedule();
		}
	}
}
