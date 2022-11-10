package org.frameworkset.tran.plugin.file.output;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.output.fileftp.FailedResend;
import org.frameworkset.tran.output.fileftp.FileFtpOutPutDataTran;
import org.frameworkset.tran.output.fileftp.FileFtpOutPutUtil;
import org.frameworkset.tran.output.fileftp.SuccessFilesClean;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: file Log to file and ftp data tran plugin </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class FileOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	private FailedResend failedResend;
	private SuccessFilesClean successFilesClean;
	public FileOutputDataTranPlugin(ImportContext importContext){
		super(importContext);

	}

	@Override
	public void afterInit() {
		FileOutputConfig fileOutputConfig = (FileOutputConfig) importContext.getOutputConfig();
		if(!fileOutputConfig.isDisableftp() && fileOutputConfig.getFtpOutConfig() != null) {

			if(fileOutputConfig.getFailedFileResendInterval() > 0) {
				if (failedResend == null) {
					synchronized (FailedResend.class) {
						if (failedResend == null) {
							failedResend = new FailedResend(fileOutputConfig);
							failedResend.start();
						}
					}
				}
			}
			if(fileOutputConfig.getSuccessFilesCleanInterval() > 0){
				if(successFilesClean == null){
					synchronized (SuccessFilesClean.class) {
						if (successFilesClean == null) {
							successFilesClean = new SuccessFilesClean(fileOutputConfig);
							successFilesClean.start();
						}
					}
				}
			}
		}
	}

	@Override
	public void beforeInit() {

	}

	@Override
	public void init() {

	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(successFilesClean != null){
			successFilesClean.stop();
		}
		if(failedResend != null){
			failedResend.stopThread();
		}
	}


	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		FileFtpOutPutDataTran fileFtpOutPutDataTran = FileFtpOutPutUtil.buildFileFtpOutPutDataTran(taskContext,tranResultSet,importContext,  countDownLatch,   currentStatus);
		fileFtpOutPutDataTran.initTran();
		return fileFtpOutPutDataTran;
	}





}
