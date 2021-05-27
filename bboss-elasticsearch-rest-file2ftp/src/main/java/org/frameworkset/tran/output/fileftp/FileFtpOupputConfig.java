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

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.util.RecordGenerator;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class FileFtpOupputConfig extends BaseImportConfig {
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.kafka.output.fileftp.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;

	/**
	 *  导出文件名称生成接口实现类型（必须指定）：org.frameworkset.tran.kafka.output.fileftp.FilenameGenerator
	 */
	private FilenameGenerator filenameGenerator;
	private boolean backupSuccessFiles;
	private boolean transferEmptyFiles;
	private String fileDir;
	private int fileWriterBuffsize ;
	private int maxFileRecordSize;
	private List<String> hostKeyVerifiers;
	private int transferProtocol;
	private boolean disableftp;

	/**
	 * 单位：毫秒,默认1分钟
	 * @return
	 */
	public long getSuccessFilesCleanInterval() {
		return successFilesCleanInterval;
	}

	public FileFtpOupputConfig setSuccessFilesCleanInterval(long successFilesCleanInterval) {
		this.successFilesCleanInterval = successFilesCleanInterval;
		return  this;
	}

	/**
	 * 单位：毫秒,默认1分钟
	 * 小于等于0时禁用归档功能
	 */
	private long successFilesCleanInterval = 60000;
	/**
	 * 单位：秒,默认2天
	 */
	private int fileLiveTime = 86400*2;
	public FileFtpOupputConfig addHostKeyVerifier(String hostKeyVerifier) {
		if(hostKeyVerifiers  == null){
			this.hostKeyVerifiers = new ArrayList<String>();
		}
		this.hostKeyVerifiers.add(hostKeyVerifier);
		return this;
	}

	public List<String> getHostKeyVerifiers() {
		return hostKeyVerifiers;
	}

	private String ftpIP;
	private int ftpPort;
	private String ftpUser;
	private String ftpPassword;
	private String ftpProtocol;
	private String ftpTrustmgr;
	private String ftpProxyHost;


	private int ftpProxyPort;

	private String ftpProxyPassword;
	private String ftpProxyUser;
	private boolean printHash;
 	private boolean binaryTransfer = true;

	/**
	 * 毫秒为单位
	 */
	private long keepAliveTimeout;
	private int controlKeepAliveReplyTimeout;
	private String encoding;
	private String ftpServerType;

	private boolean localActive;

	private boolean useEpsvWithIPv4;
	private String remoteFileDir;
	private long failedFileResendInterval = 300000l;

	public FileFtpOupputConfig setFailedFileResendInterval(long failedFileResendInterval) {
		this.failedFileResendInterval = failedFileResendInterval;
		return  this;
	}

	public long getFailedFileResendInterval() {//300000l
		return failedFileResendInterval;
	}
	public String getFtpProtocol() {
		return ftpProtocol;
	}

	public FileFtpOupputConfig setFtpProtocol(String ftpProtocol) {
		this.ftpProtocol = ftpProtocol;
		return  this;
	}

	public String getFtpTrustmgr() {
		return ftpTrustmgr;
	}

	public FileFtpOupputConfig setFtpTrustmgr(String ftpTrustmgr) {
		this.ftpTrustmgr = ftpTrustmgr;
		return  this;
	}

	public String getFtpProxyHost() {
		return ftpProxyHost;
	}

	public FileFtpOupputConfig setFtpProxyHost(String ftpProxyHost) {
		this.ftpProxyHost = ftpProxyHost;
		return  this;
	}

	public int getFtpProxyPort() {
		return ftpProxyPort;
	}

	public FileFtpOupputConfig setFtpProxyPort(int ftpProxyPort) {
		this.ftpProxyPort = ftpProxyPort;
		return  this;
	}

	public String getFtpProxyPassword() {
		return ftpProxyPassword;
	}

	public FileFtpOupputConfig setFtpProxyPassword(String ftpProxyPassword) {
		this.ftpProxyPassword = ftpProxyPassword;
		return  this;
	}

	public String getFtpProxyUser() {
		return ftpProxyUser;
	}

	public FileFtpOupputConfig setFtpProxyUser(String ftpProxyUser) {
		this.ftpProxyUser = ftpProxyUser;
		return  this;
	}

	public boolean isPrintHash() {
		return printHash;
	}

	public FileFtpOupputConfig setPrintHash(boolean printHash) {
		this.printHash = printHash;
		return  this;
	}

	public boolean isBinaryTransfer() {
		return binaryTransfer;
	}

	public FileFtpOupputConfig setBinaryTransfer(boolean binaryTransfer) {
		this.binaryTransfer = binaryTransfer;
		return  this;
	}

	public long getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	public FileFtpOupputConfig setKeepAliveTimeout(long keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
		return  this;
	}

	public int getControlKeepAliveReplyTimeout() {
		return controlKeepAliveReplyTimeout;
	}

	public FileFtpOupputConfig setControlKeepAliveReplyTimeout(int controlKeepAliveReplyTimeout) {
		this.controlKeepAliveReplyTimeout = controlKeepAliveReplyTimeout;
		return  this;
	}

	public String getEncoding() {
		return encoding;
	}

	public FileFtpOupputConfig setEncoding(String encoding) {
		this.encoding = encoding;
		return  this;
	}

	public String getFtpServerType() {
		return ftpServerType;
	}

	public FileFtpOupputConfig setFtpServerType(String ftpServerType) {
		this.ftpServerType = ftpServerType;
		return  this;
	}

	public boolean isLocalActive() {
		return localActive;
	}

	public FileFtpOupputConfig setLocalActive(boolean localActive) {
		this.localActive = localActive;
		return  this;
	}

	public boolean isUseEpsvWithIPv4() {
		return useEpsvWithIPv4;
	}

	public FileFtpOupputConfig setUseEpsvWithIPv4(boolean useEpsvWithIPv4) {
		this.useEpsvWithIPv4 = useEpsvWithIPv4;
		return  this;
	}

	public String getRemoteFileDir() {
		return remoteFileDir;
	}

	public FileFtpOupputConfig setRemoteFileDir(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
		return  this;
	}

	public String getFtpIP() {
		return ftpIP;
	}

	public FileFtpOupputConfig setFtpIP(String ftpIP) {
		this.ftpIP = ftpIP;
		return  this;
	}


	public int getFtpPort() {
		return ftpPort;
	}

	public FileFtpOupputConfig setFtpPort(int ftpPort) {
		this.ftpPort = ftpPort;
		return  this;
	}

	public String getFtpUser() {
		return ftpUser;
	}

	public FileFtpOupputConfig setFtpUser(String ftpUser) {
		this.ftpUser = ftpUser;
		return  this;
	}

	public String getFtpPassword() {
		return ftpPassword;
	}

	public FileFtpOupputConfig setFtpPassword(String ftpPassword) {
		this.ftpPassword = ftpPassword;
		return  this;
	}




	public String getFileDir() {
		return fileDir;
	}

	public FileFtpOupputConfig setFileDir(String fileDir) {
		this.fileDir = fileDir;
		return  this;
	}

	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}


	public FileFtpOupputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return  this;
	}

	public FilenameGenerator getFilenameGenerator() {
		return filenameGenerator;
	}

	public FileFtpOupputConfig setFilenameGenerator(FilenameGenerator filenameGenerator) {
		this.filenameGenerator = filenameGenerator;
		return  this;
	}

	public int getFileWriterBuffsize() {
		return fileWriterBuffsize;
	}

	public FileFtpOupputConfig setFileWriterBuffsize(int fileWriterBuffsize) {
		this.fileWriterBuffsize = fileWriterBuffsize;
		return  this;
	}

	public int getMaxFileRecordSize() {
		return maxFileRecordSize;
	}

	public FileFtpOupputConfig setMaxFileRecordSize(int maxFileRecordSize) {
		this.maxFileRecordSize = maxFileRecordSize;
		return  this;
	}

	public int getTransferProtocol() {
		return transferProtocol;
	}

	public FileFtpOupputConfig setTransferProtocol(int transferProtocol) {
		this.transferProtocol = transferProtocol;
		return  this;
	}

	public boolean isBackupSuccessFiles() {
		return backupSuccessFiles;
	}

	public FileFtpOupputConfig setBackupSuccessFiles(boolean backupSuccessFiles) {
		this.backupSuccessFiles = backupSuccessFiles;
		return  this;
	}

	public boolean isTransferEmptyFiles() {
		return transferEmptyFiles;
	}

	public FileFtpOupputConfig setTransferEmptyFiles(boolean transferEmptyFiles) {
		this.transferEmptyFiles = transferEmptyFiles;
		return  this;
	}

	public boolean isDisableftp() {
		return disableftp;
	}

	public FileFtpOupputConfig setDisableftp(boolean disableftp) {
		this.disableftp = disableftp;
		return  this;
	}

	public int getFileLiveTime() {
		return fileLiveTime;
	}

	/**
	 * 单位：秒
	 * @param fileLiveTime
	 * @return
	 */

	public FileFtpOupputConfig setFileLiveTime(int fileLiveTime) {
		this.fileLiveTime = fileLiveTime;
		return this;
	}
}
