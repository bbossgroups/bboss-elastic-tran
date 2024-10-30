package org.frameworkset.tran.output.fileftp;
/**
 * Copyright 2020 bboss
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

import com.frameworkset.util.FileUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.util.StoppedThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * <p>Description: 失败文件重传</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/4 14:48
 * @author biaoping.yin
 * @version 1.0
 */
public class FailedResend extends StoppedThread{
	private String transferFailedFileDir;
	private String transferSuccessFileDir;
	private static final Logger logger = LoggerFactory.getLogger(FailedResend.class);
	private FileOutputConfig fileOutputConfig;
    private ImportContext importContext;
	public FailedResend(ImportContext importContext,FileOutputConfig fileOutputConfig){
		super("FailedFileResend-Thread");
        this.importContext = importContext;
		this.fileOutputConfig = fileOutputConfig;
		transferFailedFileDir = SimpleStringUtil.getPath(fileOutputConfig.getFileDir(),"transferFailedFileDir");
		transferSuccessFileDir = SimpleStringUtil.getPath(fileOutputConfig.getFileDir(),"transferSuccessFileDir");

	}
	public void start(){
		this.setDaemon(true);
		super.start();
	}
	public void run(){
		File transferFailedFileDir_ = new File(transferFailedFileDir);
        boolean isFtp = fileOutputConfig.getFtpOutConfig() != null && fileOutputConfig.getMinioFileConfig() == null;
		logger.info("FailedFileResend-Thread started,transferFailedFileDir["+transferFailedFileDir+"],failedFileResendInterval:"+ fileOutputConfig.getFailedFileResendInterval()+"毫秒");
		while(true){
			if(transferFailedFileDir_.exists()){
				File[] files = transferFailedFileDir_.listFiles();
				for(int i =0 ; i < files.length; i ++){
					File file = files[i];
					if(!file.isFile())
						continue;
					try {
						String remoteFilePath = isFtp ?SimpleStringUtil.getPath(fileOutputConfig.getRemoteFileDir(), file.getName()):null;
						if (file.length() <= 0) {
							if (fileOutputConfig.transferEmptyFiles()) {
                                /**
								if (fileOutputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
									FtpTransfer.sendFile(fileOutputConfig, file.getPath(), remoteFilePath);
								} else {
									SFTPTransfer.sendFile(fileOutputConfig, file.getPath());
								}
                                 */
                                fileOutputConfig.getSendFileFunction().sendFile(fileOutputConfig,file.getPath(),remoteFilePath,true);
							}
						} else {
                            /**
							if (fileOutputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {

								FtpTransfer.sendFile(fileOutputConfig, file.getPath(), remoteFilePath);
							} else {
								SFTPTransfer.sendFile(fileOutputConfig, file.getPath());
							}
                             */
                            fileOutputConfig.getSendFileFunction().sendFile(fileOutputConfig,file.getPath(),remoteFilePath,true);
						}
						if (fileOutputConfig.backupSuccessFiles()) {
							String transferSuccessFile = SimpleStringUtil.getPath(transferSuccessFileDir, file.getName());
							FileUtil.renameFile(file.getPath(), transferSuccessFile);//如果文件发送成功，将文件移除到成功目录，保留一天，过期自动清理
						} else {
							FileUtil.deleteFile(file.getPath());
						}
						logger.info("Resend file "+ file.getPath() + " success.");
					}
					catch (Exception e){
                        String msg = "Resend file "+ file.getPath() + " failed:";
                        importContext.reportJobMetricErrorLog(msg,e);
						logger.error(msg,e);
					}
					catch (Throwable e){
                        String msg = "Resend file "+ file.getPath() + " failed:";
                        importContext.reportJobMetricErrorLog(msg,e);
                        logger.error(msg,e);
					}
				}
			}

			if(stopped)
				break;
			try {
				synchronized (this) {
					wait(fileOutputConfig.getFailedFileResendInterval());
				}
				if(stopped)
					break;
			} catch (InterruptedException e) {
				logger.info("Resend file thread Interrupted");
				break;
			}
		}
	}



}
