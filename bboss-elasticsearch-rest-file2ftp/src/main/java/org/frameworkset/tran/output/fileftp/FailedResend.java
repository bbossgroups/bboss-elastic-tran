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
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
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
public class FailedResend extends Thread{
	private String transferFailedFileDir;
	private String transferSuccessFileDir;
	private static final Logger logger = LoggerFactory.getLogger(FailedResend.class);
	private FileFtpOupputContext fileFtpOupputContext;
	public FailedResend(FileFtpOupputContext fileFtpOupputContext){
		super("FailedFileResend-Thread");
		this.fileFtpOupputContext = fileFtpOupputContext;
		transferFailedFileDir = SimpleStringUtil.getPath(fileFtpOupputContext.getFileDir(),"transferFailedFileDir");
		transferSuccessFileDir = SimpleStringUtil.getPath(fileFtpOupputContext.getFileDir(),"transferSuccessFileDir");

	}
	public void start(){
		this.setDaemon(true);
		super.start();
	}
	public void run(){
		File transferFailedFileDir_ = new File(transferFailedFileDir);
		logger.info("FailedFileResend-Thread started,transferFailedFileDir["+transferFailedFileDir+"],failedFileResendInterval:"+fileFtpOupputContext.getFailedFileResendInterval()+"毫秒");
		while(true){
			if(transferFailedFileDir_.exists()){
				File[] files = transferFailedFileDir_.listFiles();
				for(int i =0 ; i < files.length; i ++){
					File file = files[i];
					if(!file.isFile())
						continue;
					try {
						String remoteFilePath = SimpleStringUtil.getPath(fileFtpOupputContext.getRemoteFileDir(), file.getName());
						if (file.length() <= 0) {
							if (fileFtpOupputContext.transferEmptyFiles()) {
								if (fileFtpOupputContext.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
									FtpTransfer.sendFile(fileFtpOupputContext, file.getPath(), remoteFilePath);
								} else {
									SFTPTransfer.sendFile(fileFtpOupputContext, file.getPath());
								}
							}
						} else {
							if (fileFtpOupputContext.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {

								FtpTransfer.sendFile(fileFtpOupputContext, file.getPath(), remoteFilePath);
							} else {
								SFTPTransfer.sendFile(fileFtpOupputContext, file.getPath());
							}
							logger.error("Resend file "+ file.getPath() + " to " + remoteFilePath + " complete.");
						}
						if (fileFtpOupputContext.backupSuccessFiles()) {
							String transferSuccessFile = SimpleStringUtil.getPath(transferSuccessFileDir, file.getName());
							FileUtil.renameFile(file.getPath(), transferSuccessFile);//如果文件发送成功，将文件移除到成功目录，保留一天，过期自动清理
						} else {
							FileUtil.deleteFile(file.getPath());
						}
						logger.error("Resend file "+ file.getPath() + " success.");
					}
					catch (Exception e){
						logger.error("Resend file "+ file.getPath() + " failed:",e);
					}
					catch (Throwable e){
						logger.error("Resend file "+ file.getPath() + " failed:",e);
					}
				}
			}

			try {
				synchronized (this) {
					wait(fileFtpOupputContext.getFailedFileResendInterval());
				}
			} catch (InterruptedException e) {
				logger.error("Resend file thread Interrupted",e);
				break;
			}
		}
	}



}
