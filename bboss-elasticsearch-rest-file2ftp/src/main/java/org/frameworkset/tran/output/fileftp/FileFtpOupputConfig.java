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
	private ReocordGenerator reocordGenerator;

	/**
	 *  导出文件名称生成接口实现类型（必须指定）：org.frameworkset.tran.kafka.output.fileftp.FilenameGenerator
	 */
	private FilenameGenerator filenameGenerator;
	private String fileDir;
	private int fileWriterBuffsize ;
	private int maxFileRecordSize;
	private List<String> hostKeyVerifiers;
	private int transferProtocol;

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
 	private boolean binaryTransfer;

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


	public String getFtpProtocol() {
		return ftpProtocol;
	}

	public void setFtpProtocol(String ftpProtocol) {
		this.ftpProtocol = ftpProtocol;
	}

	public String getFtpTrustmgr() {
		return ftpTrustmgr;
	}

	public void setFtpTrustmgr(String ftpTrustmgr) {
		this.ftpTrustmgr = ftpTrustmgr;
	}

	public String getFtpProxyHost() {
		return ftpProxyHost;
	}

	public void setFtpProxyHost(String ftpProxyHost) {
		this.ftpProxyHost = ftpProxyHost;
	}

	public int getFtpProxyPort() {
		return ftpProxyPort;
	}

	public void setFtpProxyPort(int ftpProxyPort) {
		this.ftpProxyPort = ftpProxyPort;
	}

	public String getFtpProxyPassword() {
		return ftpProxyPassword;
	}

	public void setFtpProxyPassword(String ftpProxyPassword) {
		this.ftpProxyPassword = ftpProxyPassword;
	}

	public String getFtpProxyUser() {
		return ftpProxyUser;
	}

	public void setFtpProxyUser(String ftpProxyUser) {
		this.ftpProxyUser = ftpProxyUser;
	}

	public boolean isPrintHash() {
		return printHash;
	}

	public void setPrintHash(boolean printHash) {
		this.printHash = printHash;
	}

	public boolean isBinaryTransfer() {
		return binaryTransfer;
	}

	public void setBinaryTransfer(boolean binaryTransfer) {
		this.binaryTransfer = binaryTransfer;
	}

	public long getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	public void setKeepAliveTimeout(long keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
	}

	public int getControlKeepAliveReplyTimeout() {
		return controlKeepAliveReplyTimeout;
	}

	public void setControlKeepAliveReplyTimeout(int controlKeepAliveReplyTimeout) {
		this.controlKeepAliveReplyTimeout = controlKeepAliveReplyTimeout;
	}

	public String getEncoding() {
		return encoding;
	}

	public void setEncoding(String encoding) {
		this.encoding = encoding;
	}

	public String getFtpServerType() {
		return ftpServerType;
	}

	public void setFtpServerType(String ftpServerType) {
		this.ftpServerType = ftpServerType;
	}

	public boolean isLocalActive() {
		return localActive;
	}

	public void setLocalActive(boolean localActive) {
		this.localActive = localActive;
	}

	public boolean isUseEpsvWithIPv4() {
		return useEpsvWithIPv4;
	}

	public void setUseEpsvWithIPv4(boolean useEpsvWithIPv4) {
		this.useEpsvWithIPv4 = useEpsvWithIPv4;
	}

	public String getRemoteFileDir() {
		return remoteFileDir;
	}

	public void setRemoteFileDir(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
	}

	public String getFtpIP() {
		return ftpIP;
	}

	public void setFtpIP(String ftpIP) {
		this.ftpIP = ftpIP;
	}


	public int getFtpPort() {
		return ftpPort;
	}

	public void setFtpPort(int ftpPort) {
		this.ftpPort = ftpPort;
	}

	public String getFtpUser() {
		return ftpUser;
	}

	public void setFtpUser(String ftpUser) {
		this.ftpUser = ftpUser;
	}

	public String getFtpPassword() {
		return ftpPassword;
	}

	public void setFtpPassword(String ftpPassword) {
		this.ftpPassword = ftpPassword;
	}




	public String getFileDir() {
		return fileDir;
	}

	public void setFileDir(String fileDir) {
		this.fileDir = fileDir;
	}

	public ReocordGenerator getReocordGenerator() {
		return reocordGenerator;
	}


	public void setReocordGenerator(ReocordGenerator reocordGenerator) {
		this.reocordGenerator = reocordGenerator;
	}

	public FilenameGenerator getFilenameGenerator() {
		return filenameGenerator;
	}

	public void setFilenameGenerator(FilenameGenerator filenameGenerator) {
		this.filenameGenerator = filenameGenerator;
	}

	public int getFileWriterBuffsize() {
		return fileWriterBuffsize;
	}

	public void setFileWriterBuffsize(int fileWriterBuffsize) {
		this.fileWriterBuffsize = fileWriterBuffsize;
	}

	public int getMaxFileRecordSize() {
		return maxFileRecordSize;
	}

	public void setMaxFileRecordSize(int maxFileRecordSize) {
		this.maxFileRecordSize = maxFileRecordSize;
	}

	public int getTransferProtocol() {
		return transferProtocol;
	}

	public void setTransferProtocol(int transferProtocol) {
		this.transferProtocol = transferProtocol;
	}
}
