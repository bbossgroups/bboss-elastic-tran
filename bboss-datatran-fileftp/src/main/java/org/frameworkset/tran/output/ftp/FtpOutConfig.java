package org.frameworkset.tran.output.ftp;
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

import org.frameworkset.tran.ftp.FtpConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/2/14 22:14
 * @author biaoping.yin
 * @version 1.0
 */
public class FtpOutConfig {
 
	private boolean backupSuccessFiles;
	private boolean transferEmptyFiles;
	private List<String> hostKeyVerifiers;
	private int transferProtocol = FtpConfig.TRANSFER_PROTOCOL_SFTP;
	/**
	 * 异步发送文件，默认同步发送
	 * true 异步发送 false同步发送
	 */
	private boolean sendFileAsyn;
	private int sendFileAsynWorkThreads = 10;
	/**
	 * 单位：毫秒,默认1分钟
	 * @return
	 */
	public long getSuccessFilesCleanInterval() {
		return successFilesCleanInterval;
	}

	public FtpOutConfig setSuccessFilesCleanInterval(long successFilesCleanInterval) {
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
	public FtpOutConfig addHostKeyVerifier(String hostKeyVerifier) {
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
	private Boolean binaryTransfer = true;

	/**
	 * 毫秒为单位
	 */
	private long keepAliveTimeout;


	/**
	 * 毫秒为单位
	 * Sets the timeout in milliseconds to use when reading from the data connection. This timeout will be set immediately after opening the data connection,
	 * provided that the value is &ge; 0.
	 * <p>
	 * <b>Note:</b> the timeout will also be applied when calling accept() whilst establishing an active local data connection.
	 * </p>
	 *
	 * @param socketTimeout The default timeout in milliseconds that is used when opening a data connection socket. The value 0 means an infinite timeout.
	 */
	private long socketTimeout;
	/**
	 * 建立连接超时时间，单位：毫秒
	 */
	private long connectTimeout;
	private int controlKeepAliveReplyTimeout;
	private String encoding;
	private String ftpServerType;

	private Boolean localActive;

	private Boolean useEpsvWithIPv4;
	private String remoteFileDir;
	private long failedFileResendInterval = 300000l;

	public FtpOutConfig setFailedFileResendInterval(long failedFileResendInterval) {
		this.failedFileResendInterval = failedFileResendInterval;
		return  this;
	}

	public long getFailedFileResendInterval() {//300000l
		return failedFileResendInterval;
	}
	public String getFtpProtocol() {
		return ftpProtocol;
	}

	public FtpOutConfig setFtpProtocol(String ftpProtocol) {
		this.ftpProtocol = ftpProtocol;
		return  this;
	}

	public String getFtpTrustmgr() {
		return ftpTrustmgr;
	}

	public FtpOutConfig setFtpTrustmgr(String ftpTrustmgr) {
		this.ftpTrustmgr = ftpTrustmgr;
		return  this;
	}

	public String getFtpProxyHost() {
		return ftpProxyHost;
	}

	public FtpOutConfig setFtpProxyHost(String ftpProxyHost) {
		this.ftpProxyHost = ftpProxyHost;
		return  this;
	}

	public int getFtpProxyPort() {
		return ftpProxyPort;
	}

	public FtpOutConfig setFtpProxyPort(int ftpProxyPort) {
		this.ftpProxyPort = ftpProxyPort;
		return  this;
	}

	public String getFtpProxyPassword() {
		return ftpProxyPassword;
	}

	public FtpOutConfig setFtpProxyPassword(String ftpProxyPassword) {
		this.ftpProxyPassword = ftpProxyPassword;
		return  this;
	}

	public String getFtpProxyUser() {
		return ftpProxyUser;
	}

	public FtpOutConfig setFtpProxyUser(String ftpProxyUser) {
		this.ftpProxyUser = ftpProxyUser;
		return  this;
	}

	public boolean isPrintHash() {
		return printHash;
	}

	public FtpOutConfig setPrintHash(boolean printHash) {
		this.printHash = printHash;
		return  this;
	}

	public Boolean isBinaryTransfer() {
		return binaryTransfer;
	}

	public FtpOutConfig setBinaryTransfer(Boolean binaryTransfer) {
		this.binaryTransfer = binaryTransfer;
		return  this;
	}

	public long getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	public FtpOutConfig setKeepAliveTimeout(long keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
		return  this;
	}

	public int getControlKeepAliveReplyTimeout() {
		return controlKeepAliveReplyTimeout;
	}

	public FtpOutConfig setControlKeepAliveReplyTimeout(int controlKeepAliveReplyTimeout) {
		this.controlKeepAliveReplyTimeout = controlKeepAliveReplyTimeout;
		return  this;
	}

	public String getEncoding() {
		return encoding;
	}

	public FtpOutConfig setEncoding(String encoding) {
		this.encoding = encoding;
		return  this;
	}

	public String getFtpServerType() {
		return ftpServerType;
	}

	public FtpOutConfig setFtpServerType(String ftpServerType) {
		this.ftpServerType = ftpServerType;
		return  this;
	}

	public Boolean isLocalActive() {
		return localActive;
	}

	public FtpOutConfig setLocalActive(Boolean localActive) {
		this.localActive = localActive;
		return  this;
	}

	public Boolean isUseEpsvWithIPv4() {
		return useEpsvWithIPv4;
	}

	public FtpOutConfig setUseEpsvWithIPv4(Boolean useEpsvWithIPv4) {
		this.useEpsvWithIPv4 = useEpsvWithIPv4;
		return  this;
	}

	public String getRemoteFileDir() {
		return remoteFileDir;
	}

	public FtpOutConfig setRemoteFileDir(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
		return  this;
	}

	public String getFtpIP() {
		return ftpIP;
	}

	public FtpOutConfig setFtpIP(String ftpIP) {
		this.ftpIP = ftpIP;
		return  this;
	}


	public int getFtpPort() {
		return ftpPort;
	}

	public FtpOutConfig setFtpPort(int ftpPort) {
		this.ftpPort = ftpPort;
		return  this;
	}

	public String getFtpUser() {
		return ftpUser;
	}

	public FtpOutConfig setFtpUser(String ftpUser) {
		this.ftpUser = ftpUser;
		return  this;
	}

	public String getFtpPassword() {
		return ftpPassword;
	}

	public FtpOutConfig setFtpPassword(String ftpPassword) {
		this.ftpPassword = ftpPassword;
		return  this;
	}




	public int getTransferProtocol() {
		return transferProtocol;
	}

	public FtpOutConfig setTransferProtocol(int transferProtocol) {
		this.transferProtocol = transferProtocol;
		return  this;
	}

	public boolean isBackupSuccessFiles() {
		return backupSuccessFiles;
	}

	public FtpOutConfig setBackupSuccessFiles(boolean backupSuccessFiles) {
		this.backupSuccessFiles = backupSuccessFiles;
		return  this;
	}

	public boolean isTransferEmptyFiles() {
		return transferEmptyFiles;
	}

	public FtpOutConfig setTransferEmptyFiles(boolean transferEmptyFiles) {
		this.transferEmptyFiles = transferEmptyFiles;
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

	public FtpOutConfig setFileLiveTime(int fileLiveTime) {
		this.fileLiveTime = fileLiveTime;
		return this;
	}
	public boolean isSendFileAsyn() {
		return sendFileAsyn;
	}
	/**
	 * 设置是否异步发送文件，默认同步发送
	 * true 异步发送 false同步发送
	 */
	public FtpOutConfig setSendFileAsyn(boolean sendFileAsyn) {
		this.sendFileAsyn = sendFileAsyn;
		return this;
	}

	public int getSendFileAsynWorkThreads() {
		return sendFileAsynWorkThreads;
	}

	public FtpOutConfig setSendFileAsynWorkThreads(int sendFileAsynWorkThreads) {
		this.sendFileAsynWorkThreads = sendFileAsynWorkThreads;
		return this;
	}

	public long getSocketTimeout() {
		return socketTimeout;
	}
	/**
	 * Sets the timeout in milliseconds to use when reading from the data connection. This timeout will be set immediately after opening the data connection,
	 * provided that the value is &ge; 0.
	 * <p>
	 * <b>Note:</b> the timeout will also be applied when calling accept() whilst establishing an active local data connection.
	 * </p>
	 *
	 * @param socketTimeout The default timeout in milliseconds that is used when opening a data connection socket. The value 0 means an infinite timeout.
	 */
	public FtpOutConfig setSocketTimeout(long socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}
	public long getConnectTimeout() {
		return connectTimeout;
	}
	/**
	 * 建立连接超时时间，单位：毫秒
	 */
	public FtpOutConfig setConnectTimeout(long connectTimeout) {
		this.connectTimeout = connectTimeout;
		return this;
	}
}
