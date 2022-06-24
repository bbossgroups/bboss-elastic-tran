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
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.util.HeaderRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.tran.util.TranUtil;
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
	protected File file;
	protected FileOutputConfig excelFileOupputConfig;
	private HeaderRecordGenerator headerRecordGenerator;
	private int buffsize = 8192;
	public FileTransfer(String taskInfo, FileOutputConfig excelFileOupputConfig, String dir, String filePath) throws IOException {
		this.excelFileOupputConfig = excelFileOupputConfig;
		this.taskInfo  = taskInfo;
		File path = new File(dir);
		if(!path.exists())
		{
			path.mkdirs();
		}
		this.buffsize = excelFileOupputConfig.getFileWriterBuffsize();
		if(buffsize <= 0){
			buffsize = 8192;
		}
		this.filePath = filePath;

		file = new File(filePath);


	}
	public void initFtp(String remoteFilePath){
		if(!excelFileOupputConfig.isDisableftp()) {
			this.remoteFilePath = remoteFilePath;
			transferFailedFile = SimpleStringUtil.getPath(excelFileOupputConfig.getFileDir(), "transferFailedFileDir/" + file.getName());
			File path = new File(transferFailedFile).getParentFile();
			if (!path.exists())
				path.mkdirs();

			transferSuccessFile = SimpleStringUtil.getPath(excelFileOupputConfig.getFileDir(), "transferSuccessFileDir/" + file.getName());
			path = new File(transferSuccessFile).getParentFile();
			if (!path.exists())
				path.mkdirs();
		}
	}
	public void initTransfer() throws IOException {
		RecordGenerator recordGenerator = excelFileOupputConfig.getRecordGenerator();
		if(recordGenerator instanceof HeaderRecordGenerator){
			this.headerRecordGenerator = (HeaderRecordGenerator)recordGenerator;
		}
		fw = new FileWriter(file);
		bw = new BufferedWriter(fw,buffsize);
	}

	/**
	 * 添加标题行
	 * @throws Exception
	 */
	public void writeHeader() throws Exception {
		if(headerRecordGenerator != null) {
			BBossStringWriter writer = new BBossStringWriter();
			headerRecordGenerator.buildHeaderRecord(writer);
			bw.write(writer.toString());
			bw.write(TranUtil.lineSeparator);
		}
	}
	public synchronized void writeData(String data) throws IOException {
		bw.write(data);
	}
	private static Logger logger = LoggerFactory.getLogger(FileTransfer.class);

	public boolean isSended() {
		return sended;
	}

	private boolean sended;
	protected void flush() throws IOException {
		if(bw != null)
			bw.flush();
		this.close();
	}
	public void sendFile(){
		if(sended)
			return;
		sended = true;
		try {
			flush();
		}
		catch (Exception e){
			logger.error("flush task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",e);
			return;
		}
		try {

			if(!excelFileOupputConfig.isDisableftp()) {
				if (file.length() <= 0) {
					if (excelFileOupputConfig.transferEmptyFiles()) {
						if (excelFileOupputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
							FtpTransfer.sendFile(excelFileOupputConfig, filePath, remoteFilePath);
						} else {
							SFTPTransfer.sendFile(excelFileOupputConfig, this.filePath);
						}
					}
				} else {
					if (excelFileOupputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
						FtpTransfer.sendFile(excelFileOupputConfig, filePath, remoteFilePath);
					} else {
						SFTPTransfer.sendFile(excelFileOupputConfig, this.filePath);
					}
				}
				try {
					if (excelFileOupputConfig.backupSuccessFiles())
						FileUtil.bakFile(filePath, transferSuccessFile);//如果文件发送成功，将文件移除到成功目录，保留一天，过期自动清理
					else
						FileUtil.deleteFile(filePath);
				}
				catch (Exception e){
					if (excelFileOupputConfig.backupSuccessFiles())
						logger.error("backup Success File task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",e);
					else{
						logger.error("delete Success File task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",e);
					}
				}

			}
		}
		catch (Exception e){
			logger.error("sendFile task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",e);
			if(file.exists() && file.length() > 0) {
				try {
					FileUtil.bakFile(filePath,transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
				} catch (IOException ioException) {
					logger.error("backup failed send File task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",ioException);
				}
			}

		}

		catch (Throwable e){
			logger.error("sendFile task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",e);
			try {
				FileUtil.bakFile(filePath,transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
			} catch (IOException ioException) {
				logger.error("backup failed send File task["+taskInfo+"],file["+file.getAbsolutePath()+"] failed:",ioException);
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
