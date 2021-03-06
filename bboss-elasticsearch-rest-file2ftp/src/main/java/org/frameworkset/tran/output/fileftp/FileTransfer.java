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
import org.frameworkset.tran.DataImportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/29 10:25
 * @author biaoping.yin
 * @version 1.0
 */
public class FileTransfer {
	private BufferedWriter bw = null;
	private FileWriter fw = null;
	private String filePath;
	private String remoteFilePath;
	private String transferFailedFile;
	private String transferSuccessFile;
	private String taskInfo;
	private File file;
	private FileFtpOupputContext fileFtpOupputContext;
	public FileTransfer(String taskInfo, FileFtpOupputContext fileFtpOupputContext, String dir, String filePath, String remoteFilePath, int buffsize) throws IOException {
		this.fileFtpOupputContext = fileFtpOupputContext;
		this.taskInfo  = taskInfo;
		File path = new File(dir);
		if(!path.exists())
		{
			path.mkdirs();
		}
		if(buffsize <= 0){
			buffsize = 8192;
		}
		this.filePath = filePath;
		this.remoteFilePath = remoteFilePath;
		file = new File(filePath);
		transferFailedFile = SimpleStringUtil.getPath(fileFtpOupputContext.getFileDir(),"transferFailedFileDir/"+file.getName());
		path = new File(transferFailedFile).getParentFile();
		if(!path.exists())
			path.mkdirs();

		transferSuccessFile = SimpleStringUtil.getPath(fileFtpOupputContext.getFileDir(),"transferSuccessFileDir/"+file.getName());
		path = new File(transferSuccessFile).getParentFile();
		if(!path.exists())
			path.mkdirs();

		fw = new FileWriter(file);
		bw = new BufferedWriter(fw,buffsize);
	}
	public synchronized void writeData(String data) throws IOException {
		bw.write(data);
	}
	private static Logger logger = LoggerFactory.getLogger(FileTransfer.class);

	public boolean isSended() {
		return sended;
	}

	private boolean sended;
	public void sendFile(){
		if(sended)
			return;
		sended = true;
		try {
			if(bw != null)
				bw.flush();
			this.close();
			if(!fileFtpOupputContext.disableftp()) {
				if (file.length() <= 0) {
					if (fileFtpOupputContext.transferEmptyFiles()) {
						if (fileFtpOupputContext.getTransferProtocol() == FileFtpOupputContext.TRANSFER_PROTOCOL_FTP) {
							FtpTransfer.sendFile(fileFtpOupputContext, filePath, remoteFilePath);
						} else {
							SFTPTransfer.sendFile(fileFtpOupputContext, this.filePath);
						}
					}
				} else {
					if (fileFtpOupputContext.getTransferProtocol() == FileFtpOupputContext.TRANSFER_PROTOCOL_FTP) {
						FtpTransfer.sendFile(fileFtpOupputContext, filePath, remoteFilePath);
					} else {
						SFTPTransfer.sendFile(fileFtpOupputContext, this.filePath);
					}
				}
				if (fileFtpOupputContext.backupSuccessFiles())
					FileUtil.bakFile(filePath, transferSuccessFile);//如果文件发送成功，将文件移除到成功目录，保留一天，过期自动清理
				else
					FileUtil.deleteFile(filePath);
			}
		}
		catch (IOException e){
			if(file.exists() && file.length() > 0) {
				try {
					FileUtil.bakFile(filePath,transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
				} catch (IOException ioException) {
					logger.error(taskInfo,ioException);
				}
			}
			logger.error(taskInfo,e);
		}
		catch (DataImportException e){
			logger.error(taskInfo,e);
			try {
				FileUtil.bakFile(filePath,transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
			} catch (IOException ioException) {
				logger.error(taskInfo,ioException);
			}
		}
		catch (Throwable e){
			logger.error(taskInfo,e);
			try {
				FileUtil.bakFile(filePath,transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
			} catch (IOException ioException) {
				logger.error(taskInfo,ioException);
			}
		}


	}

	public void close(){
		if(fw != null){
			try {
				fw.close();
				fw = null;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if(bw != null){
			try {

				bw.close();
				bw = null;
			} catch (IOException e) {
				//e.printStackTrace();
			}
		}
	}
}
