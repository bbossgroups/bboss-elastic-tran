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

import com.frameworkset.common.poolman.SQLExecutor;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.file.monitor.FileInodeHandler;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.input.file.FileResultSet;
import org.frameworkset.tran.input.file.FileTaskContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.status.MultiStatusManager;
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
	protected static Logger logger = LoggerFactory.getLogger(FileDataTranPluginImpl.class);
	@Override
	public void initLastValueClumnName(){

	}


	@Override
	public  void beforeInit() {
		fileInputDataTranPlugin = (FileInputDataTranPlugin) inputPlugin;
		super.beforeInit();


	}

	public FileDataTranPluginImpl(ImportContext importContext){
		super(importContext);

		fileInputConfig = (FileInputConfig) importContext.getInputConfig();

	}
	public Status getCurrentStatus(){
		throw new UnsupportedOperationException("getCurrentStatus");
	}
	@Override
	public void destroy(boolean waitTranStop){
		this.status = TranConstant.PLUGIN_STOPAPPENDING;
//		stopScanThread();
//		fileListenerService.checkTranFinished();//检查所有的作业是否已经结束，并等待作业结束
		fileInputDataTranPlugin.destroy(waitTranStop);
		super.destroy( waitTranStop);//之前为什么是false super.destroy( false);
		// todo
	}

	@Override
	protected void loadCurrentStatus(){


		try {
			/**
			 * 初始化数据检索起始状态信息
			 */
			List<Status> statuses = SQLExecutor.queryListWithDBName(Status.class, statusDbname, selectAllSQL);
			if(statuses == null || statuses.size() == 0){
				return;
			}
			boolean fromFirst = importContext.isFromFirst();

			/**
			 * 已经完成的任务
			 */
			List<Status> completed = new ArrayList<Status>();
			/**
			 * 已经过期的任务，修改状态为已完成
			 */
			List<Status> olded = new ArrayList<Status>();
			for(Status status : statuses){
				status.setRealPath(status.getFilePath());
				//判断任务是否已经完成，如果完成，则对任务进行相应处理

				if(isComplete(status)){
					completed.add(status);
					fileInputDataTranPlugin.getFileListenerService().addCompletedFileTask(status.getFileId(),fileInputDataTranPlugin.buildFileReaderTask(status.getFileId()
							,status,fileInputConfig));
					logger.info("Ignore complete file {}",status.getFilePath());
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
						continue;
					}
				}
				/**
				 if(isOlded(status,fileConfig)){
				 olded.add(status);
				 logger.info("Ignore old file {}",status.getFilePath());
				 continue;
				 }
				 if(isNeedClosed(status,fileConfig)){
				 fileListenerService.addOldedFileTask(status.getFileId(),new FileReaderTask(status.getFileId()
				 ,status));
				 handleOldedTask(status);
				 logger.info("Ignore need closed file {} closed old time {}",status.getFilePath(),fileConfig.getCloseOlderTime());
				 continue;
				 }*/
				//需判断文件是否存在，不存在需清除记录
				//创建一个文件对应的交换通道
				FileResultSet kafkaResultSet = new FileResultSet(this.importContext);
				FileTaskContext taskContext = new FileTaskContext(importContext);
				final BaseDataTran fileDataTran = createBaseDataTran(taskContext,kafkaResultSet,null,status);

				Thread tranThread = null;
				try {
					if(fileDataTran != null) {
						String tname = "file-log-tran|"+status.getRealPath();
						if(fileConfig.isEnableInode()){
							tname = tname + "|" +status.getFileId();
						}
						tranThread = new Thread(new Runnable() {
							@Override
							public void run() {
								fileDataTran.tran();
							}
						}, tname);
						tranThread.start();
						if(logger.isInfoEnabled())
							logger.info(tname+" started.");
						Object lastValue = status.getLastValue();
						long pointer = 0;
						if (!fromFirst){
							if (lastValue instanceof Long) {
								pointer = (Long) lastValue;
							} else if (lastValue instanceof Integer) {
								pointer = ((Integer) lastValue).longValue();
							} else if (lastValue instanceof Short) {
								pointer = ((Short) lastValue).longValue();
							}
						}
						else{
							status.setLastValue(0l);
						}
						FileReaderTask task = fileInputDataTranPlugin.buildFileReaderTask(taskContext,new File(status.getRealPath())
								,status.getFileId()
								,fileConfig
								,pointer
								,fileInputDataTranPlugin.getFileListenerService(),fileDataTran,status,fileInputConfig);
						task.getFileInfo().setOriginFile(new File(status.getFilePath()));
						task.getFileInfo().setOriginFilePath(status.getFilePath());
						taskContext.setFileInfo(task.getFileInfo());
						if(fileConfig.getAddFields() != null && fileConfig.getAddFields().size() > 0){
							task.addFields(fileConfig.getAddFields());
						}
						if(fileConfig.getIgnoreFields() != null && fileConfig.getIgnoreFields().size() > 0){
							task.ignoreFields(fileConfig.getIgnoreFields());
						}
						/**
						 * 根据文件信息动态添加文件标签
						 */
						if(fileConfig.getFieldBuilder() != null){
							fileConfig.getFieldBuilder().buildFields(task.getFileInfo(),task);
						}
						preCall(taskContext);//需要在任务完成时销毁taskContext
						fileInputDataTranPlugin.getFileListenerService().addFileTask(task.getFileId(),task);
						task.start();
					}


				} catch (DataImportException e) {
					throw e;
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			if(completed.size() > 0 && fileInputConfig.getRegistLiveTime() != null){
				handleCompletedTasks(completed ,false,fileInputConfig.getRegistLiveTime());
			}
			if(olded.size() > 0){
				handleOldedTasks(olded);
			}
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new DataImportException(e);
		}

	}
	@Override
	public void importData() throws DataImportException {

		if (!fileInputConfig.isUseETLScheduleForScanNewFile()) {//采用内置新文件扫描调度机制
			long importStartTime = System.currentTimeMillis();
			this.doImportData(null);
			long importEndTime = System.currentTimeMillis();
			if (importContext.isPrintTaskLog())
				logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());
		}
		else{
			super.importData();
		}

	}
	@Override
	protected void initStatusManager(){
		statusManager = new MultiStatusManager(statusDbname, updateSQL, lastValueType,this);
		statusManager.init();
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
