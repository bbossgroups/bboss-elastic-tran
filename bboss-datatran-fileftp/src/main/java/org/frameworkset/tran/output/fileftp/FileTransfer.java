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
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.ftp.FtpConfig;
import org.frameworkset.tran.ftp.FtpTransfer;
import org.frameworkset.tran.ftp.SFTPTransfer;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.util.HeaderRecordGenerator;
import org.frameworkset.tran.util.RecordGenerator;
import org.frameworkset.util.concurrent.LongCount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * <p>Description:
 * 文件切割记录规则：达到最大记录数或者空闲时间达到最大空闲时间阈值，进行文件切割
 * 如果不切割文件，达到最大最大空闲时间阈值，当切割文件标识为false时，只执行flush数据操作，不关闭文件也不生成新的文件，否则生成新的文件
 *
 * </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/29 10:25
 * @author biaoping.yin
 * @version 1.0
 */
public class FileTransfer {
	/**
	 * 写入的记录数
	 */
	protected LongCount records;
	private BufferedWriter bw = null;
	private FileWriter fw = null;
	private String filePath;
	private String remoteFilePath;
	private String transferFailedFile;
	private String transferSuccessFile;
	private String taskInfo;
	protected File file;
	protected FileOutputConfig fileOutputConfig;
	private HeaderRecordGenerator headerRecordGenerator;
	private int buffsize = 8192;
	protected long maxFileRecordSize;
	private FileFtpOutPutDataTran fileFtpOutPutDataTran;
    protected long lastWriteDataTime;
    protected boolean splitFile;
    protected long maxForceFileThreshold ;
    protected long maxForceFileThresholdInterval;
    protected MaxForceFileThresholdCheck maxForceFileThresholdCheck;
    protected Lock transferLock = new ReentrantLock();
	public FileTransfer( FileOutputConfig fileOutputConfig, String dir, FileFtpOutPutDataTran fileFtpOutPutDataTran){
		this.fileOutputConfig = fileOutputConfig;
		this.fileFtpOutPutDataTran = fileFtpOutPutDataTran;
        if(fileOutputConfig.getMaxForceFileThreshold() != null) {
            this.maxForceFileThreshold = fileOutputConfig.getMaxForceFileThreshold() * 1000L;
            this.splitFile = fileOutputConfig.isSplitFile();
            this.maxForceFileThresholdInterval = fileOutputConfig.getMaxForceFileThresholdInterval();
        }
        if(fileOutputConfig.getMaxFileRecordSize() > 0){
			maxFileRecordSize = fileOutputConfig.getMaxFileRecordSize();
		}
		RecordGenerator recordGenerator = fileOutputConfig.getRecordGenerator();
		if(recordGenerator instanceof HeaderRecordGenerator){
			this.headerRecordGenerator = (HeaderRecordGenerator)recordGenerator;
		}
		File path = new File(dir);
		if(!path.exists())
		{
			path.mkdirs();
		}
		this.buffsize = fileOutputConfig.getFileWriterBuffsize();
		if(buffsize <= 0){
			buffsize = 8192;
		}



	}

    public long getMaxForceFileThresholdInterval() {
        return maxForceFileThresholdInterval;
    }

    /**
	public void initFtp(String remoteFilePath){
		if(!fileOupputConfig.isDisableftp()) {
			this.remoteFilePath = remoteFilePath;
			transferFailedFile = SimpleStringUtil.getPath(fileOupputConfig.getFileDir(), "transferFailedFileDir/" + file.getName());
			File path = new File(transferFailedFile).getParentFile();
			if (!path.exists())
				path.mkdirs();

			transferSuccessFile = SimpleStringUtil.getPath(fileOupputConfig.getFileDir(), "transferSuccessFileDir/" + file.getName());
			path = new File(transferSuccessFile).getParentFile();
			if (!path.exists())
				path.mkdirs();
		}
	}
	public void initTransfer() throws IOException {

		fw = new FileWriter(file);
		bw = new BufferedWriter(fw,buffsize);
	}*/

	protected void initTransfer() throws IOException {
        if(!file.exists())
            file.createNewFile();
		fw = new FileWriter(file);
		bw = new BufferedWriter(fw,buffsize);
	}
	private boolean init;

	public final void init(){
		if(init)
			return;

		String[] fileInfos = fileFtpOutPutDataTran.generateFileName();
		this.filePath = fileInfos[1];
		file = new File(filePath);
		if(!fileOutputConfig.isDisableftp()) {
			this.remoteFilePath = fileInfos[2];
			transferFailedFile = SimpleStringUtil.getPath(fileOutputConfig.getFileDir(), "transferFailedFileDir/" + fileInfos[0]);
			transferSuccessFile = SimpleStringUtil.getPath(fileOutputConfig.getFileDir(), "transferSuccessFileDir/" + fileInfos[0]);
			File path = new File(transferFailedFile).getParentFile();
			if (!path.exists())
				path.mkdirs();

			path = new File(transferSuccessFile).getParentFile();
			if (!path.exists())
				path.mkdirs();
		}
		try {
			initTransfer();
			writeHeader();
		}
		catch (Exception e) {
			throw new DataImportException("init file writer failed:"+filePath,e);
		}

		if(!fileOutputConfig.isDisableftp() && fileOutputConfig.getFtpOutConfig() != null) {
			StringBuilder builder = new StringBuilder().append("Import data to ftp ip[").append(fileOutputConfig.getFtpIP())
					.append("] ftp user[").append(fileOutputConfig.getFtpUser())
					.append("] ftp password[******] ftp port[")
					.append(fileOutputConfig.getFtpPort()).append("] ")
					.append(filePath).append("]")
					.append(" remoteFileName[").append(remoteFilePath).append("]");
			taskInfo = builder.toString();
//				if(fileOutputConfig.getFailedFileResendInterval() > 0) {
//					if (failedResend == null) {
//						synchronized (FailedResend.class) {
//							if (failedResend == null) {
//								failedResend = new FailedResend(fileOutputConfig);
//								failedResend.start();
//							}
//						}
//					}
//				}
//				if(fileOutputConfig.getSuccessFilesCleanInterval() > 0){
//					if(successFilesClean == null){
//						synchronized (SuccessFilesClean.class) {
//							if (successFilesClean == null) {
//								successFilesClean = new SuccessFilesClean(fileOutputConfig);
//								successFilesClean.start();
//							}
//						}
//					}
//				}
		}
		else{
			StringBuilder builder = new StringBuilder().append("Import data to file [")
					.append(filePath).append("]");
			taskInfo = builder.toString();
		}
		//添加文件信息到任务监控信息中
		fileFtpOutPutDataTran.traceFile(filePath,remoteFilePath);
        init = true;
        sended = false;
        boolean recordsInit = false;
        if(maxFileRecordSize > 0L ){
            records = new LongCount();
            recordsInit = true;
        }
        if(this.maxForceFileThreshold > 0L){
            if(!recordsInit)
                records = new LongCount();
            if(maxForceFileThresholdCheck == null) {
                this.maxForceFileThresholdCheck = new MaxForceFileThresholdCheck(this);
                this.maxForceFileThresholdCheck.start();
            }
        }


	}
	public String getTaskInfo(){
		return taskInfo;
	}
	protected void reset(){
		this.init = false;
		this.sended = false;
		if(maxFileRecordSize > 0L || this.maxForceFileThreshold > 0L){
			records = new LongCount();
		}
	}

    public void maxForceFileThresholdCheck(){
        transferLock.lock();
        try {
            long currentTime = System.currentTimeMillis();
            if (records != null && records.getCountUnSynchronized() > 0){
                if(currentTime - lastWriteDataTime >= this.maxForceFileThreshold ){//如果空闲时间大于最大空闲阈值，则处理文件数据
                    lastWriteDataTime = currentTime;

                    if(splitFile) {
                        logger.info("Reach Max Force File Threshold: do force send file.");
                        this.sendFile();
                        reset();
                    }
                    else{
                        if(sended || !init)
                            return;
                        logger.info("Reach Max Force File Threshold: do force flush data to file.");
                        flush(false);//excel flush是否需要特殊处理？？？？
                        records = new LongCount();
                    }
                }
            }
        } catch (IOException e) {
            throw new DataImportException(e);
        } finally {
            transferLock.unlock();
        }
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
//			bw.write(TranUtil.lineSeparator);
			bw.write(fileOutputConfig.getLineSeparator());
		}
	}
	public void writeData(TaskCommand taskCommand,String data,long totalSize,long dataSize) throws IOException {
        transferLock.lock();
        try {
            init();
            if(records != null) {
                if (dataSize <= 0L)
                    dataSize = 1L;
                records.increamentUnSynchronized(dataSize);
            }

            bw.write(data);
            this.lastWriteDataTime = System.currentTimeMillis();
            if (maxFileRecordSize > 0) {
                boolean reachMaxedSize = records.getCountUnSynchronized() >= maxFileRecordSize;
                if (reachMaxedSize) {
                    sendFile();
                    reset();
                }
            }
//            else {
//                bw.write(data);
//            }
        }
        finally {
            transferLock.unlock();
        }

	}

//	public boolean splitCheck(long totalCount) {
//		return totalCount > 0 && (totalCount % maxFileRecordSize == 0);
//	}
	private static Logger logger = LoggerFactory.getLogger(FileTransfer.class);

	public boolean isSended() {
		return sended;
	}

	private boolean sended;
	protected void flush(boolean close) throws IOException {
		if(bw != null)
			bw.flush();
        if(close)
		    this.close();
	}

    public void sendFile2ndStopCheckers(){
        transferLock.lock();
        try {
            sendFile();
            if (this.maxForceFileThresholdCheck != null) {
                this.maxForceFileThresholdCheck.stopMaxForceFileThresholdCheck();
                maxForceFileThresholdCheck = null;
            }
        }
        finally {
            transferLock.unlock();
        }
    }
    /**
     *
     */
	public void sendFile(){
		if(sended || !init)
			return;
		sended = true;
		//添加文件信息到任务监控信息中
//		fileFtpOutPutDataTran.traceFile(filePath,remoteFilePath);
		try {
			flush(true);
		}
		catch (Throwable e){
            if(file != null) {
                String msg = "Flush task[" + taskInfo + "],file[" + file.getAbsolutePath() + "] failed:";
                logger.error(msg, e);
                throw new DataImportException(msg, e);
            }
            else{
                String msg = "Flush task[" + taskInfo + "] failed:";
                logger.error(msg, e);
                throw new DataImportException(msg, e);
            }
//			return;
		}


		if(!fileOutputConfig.isDisableftp()) {
			final File _file = file;
			final String _filePath = filePath;
			final String _remoteFilePath = remoteFilePath;
			final String _transferSuccessFile = transferSuccessFile;
			final String _transferFailedFile  = transferFailedFile;
			Runnable runnable = new Runnable() {
				@Override
				public void run() {
					try {
						if (_file.length() <= 0) {
							if (fileOutputConfig.transferEmptyFiles()) {
								if (fileOutputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
									FtpTransfer.sendFile(fileOutputConfig, _filePath, _remoteFilePath);
								} else {
									SFTPTransfer.sendFile(fileOutputConfig, _filePath);
								}
							}
						} else {
							if (fileOutputConfig.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {
								FtpTransfer.sendFile(fileOutputConfig, _filePath, _remoteFilePath);
							} else {
								SFTPTransfer.sendFile(fileOutputConfig, _filePath);
							}
						}
						try {
							if (fileOutputConfig.backupSuccessFiles())
								FileUtil.bakFile(_filePath, _transferSuccessFile);//如果文件发送成功，将文件移除到成功目录，保留一天，过期自动清理
							else
								FileUtil.deleteFile(_filePath);
						}
						catch (Exception e){
							if (fileOutputConfig.backupSuccessFiles()) {
//                                TaskCall.handleException(e,taskCommand.getImportCount(),taskCommand.getTaskMetrics(),taskCommand,taskCommand.getImportContext());
//                                logger.error("Backup Success File task[" + taskInfo + "],file[" + _file.getAbsolutePath() + "] failed:", e);

                                String msg = "Backup Success File task[" + taskInfo + "],file[" + _file.getAbsolutePath() + "] failed:";
                                logger.error(msg, e);

                            }
							else{
								logger.error("Delete Success File task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:",e);
							}
						}
					}
					catch (Exception e){
                        String msg = "SendFile task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:";
                        logger.error(msg, e);
//						logger.error("SendFile task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:",e);
						if(_file.exists() && _file.length() > 0) {
							try {
								FileUtil.bakFile(_filePath,_transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
							} catch (IOException ioException) {
								logger.error("Backup failed send File task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:",ioException);
							}
						}

//                        if(!fileOutputConfig.isSendFileAsyn() ) {
//                            throw new DataImportException(msg, e);
//                        }

					}

					catch (Throwable e){
                        String msg = "SendFile task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:";
                        logger.error(msg, e);
//						logger.error("SendFile task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:",e);
						try {
							FileUtil.bakFile(_filePath,_transferFailedFile);//如果文件发送失败，将文件移除到失败目录，定时重发
						} catch (IOException ioException) {
							logger.error("backup failed send File task["+taskInfo+"],file["+_file.getAbsolutePath()+"] failed:",ioException);
						}
//                        if(!fileOutputConfig.isSendFileAsyn() ) {
//                            throw new DataImportException(msg, e);
//                        }
					}
				}
			};
			if(!fileOutputConfig.isSendFileAsyn() ){
				runnable.run();
			}
			else{
				fileOutputConfig.getFileSend2Ftp().submitNewTask(runnable);
			}


		}




	}

	public void close(){
		if(fw != null){
			try {
				fw.close();
				fw = null;
			} catch (Exception e) {
				logger.warn("close fw failed:",e);
			}catch (Throwable e) {
                //e.printStackTrace();
            }
		}
		if(bw != null){
			try {

				bw.close();
				bw = null;
			} catch (Exception e) {
				//e.printStackTrace();
			}
            catch (Throwable e) {
                //e.printStackTrace();
            }
		}
	}
}
