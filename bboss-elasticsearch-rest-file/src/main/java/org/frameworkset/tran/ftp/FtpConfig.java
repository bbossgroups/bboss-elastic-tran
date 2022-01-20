package org.frameworkset.tran.ftp;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FtpFileFilter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/27 15:23
 * @author biaoping.yin
 * @version 1.0
 */
public class FtpConfig extends FileConfig {

	public static final int TRANSFER_PROTOCOL_FTP = 1;
	public static final int TRANSFER_PROTOCOL_SFTP = 2;
	private List<String> hostKeyVerifiers;
	private int transferProtocol = TRANSFER_PROTOCOL_SFTP;
	private String ftpIP;
	private int ftpPort;
	private String ftpUser;
	private String ftpPassword;
	private String ftpProtocol;
	private String ftpTrustmgr;
	private String ftpProxyHost;
	private FtpFileFilter ftpFileFilter;
	private String downloadTempDir;

	private int ftpProxyPort;

	private String ftpProxyPassword;
	private String ftpProxyUser;
	private boolean printHash;
	private boolean binaryTransfer = true;
	private boolean localActive;

	private boolean useEpsvWithIPv4;
	/**
	 * 毫秒为单位
	 */
	private long keepAliveTimeout;
	private int controlKeepAliveReplyTimeout;
	private String remoteFileDir;
	private String encoding;
	private boolean deleteRemoteFile;
	public FtpConfig addHostKeyVerifier(String hostKeyVerifier) {
		if(hostKeyVerifiers  == null){
			this.hostKeyVerifiers = new ArrayList<String>();
		}
		this.hostKeyVerifiers.add(hostKeyVerifier);
		return this;
	}

	public List<String> getHostKeyVerifiers() {
		return hostKeyVerifiers;
	}

	@Override
	public FileConfig init(){
		super.init();
		downloadTempDir = SimpleStringUtil.getPath(getSourcePath(),"temp");
		this.setEnableInode(false);
		this.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
		File f = new File(downloadTempDir);
		if(!f.exists())
			f.mkdirs();
		return this;
	}
	public int getTransferProtocol() {
		return transferProtocol;
	}

	public String getDownloadTempDir() {
		return downloadTempDir;
	}

	public FtpConfig setTransferProtocol(int transferProtocol) {
		this.transferProtocol = transferProtocol;
		return this;
	}

	public String getFtpIP() {
		return ftpIP;
	}

	public FtpConfig setFtpIP(String ftpIP) {
		this.ftpIP = ftpIP;
		return this;
	}

	public int getFtpPort() {
		return ftpPort;
	}

	public FtpConfig setFtpPort(int ftpPort) {
		this.ftpPort = ftpPort;
		return this;
	}

	public String getFtpUser() {
		return ftpUser;
	}

	public FtpConfig setFtpUser(String ftpUser) {
		this.ftpUser = ftpUser;
		return this;
	}

	public String getFtpPassword() {
		return ftpPassword;
	}

	public FtpConfig setFtpPassword(String ftpPassword) {
		this.ftpPassword = ftpPassword;
		return this;
	}

	public String getFtpProtocol() {
		return ftpProtocol;
	}

	public FtpConfig setFtpProtocol(String ftpProtocol) {
		this.ftpProtocol = ftpProtocol;
		return this;
	}

	public String getFtpTrustmgr() {
		return ftpTrustmgr;
	}

	public FtpConfig setFtpTrustmgr(String ftpTrustmgr) {
		this.ftpTrustmgr = ftpTrustmgr;
		return this;
	}

	public String getFtpProxyHost() {
		return ftpProxyHost;
	}

	public FtpConfig setFtpProxyHost(String ftpProxyHost) {
		this.ftpProxyHost = ftpProxyHost;
		return this;
	}

	public int getFtpProxyPort() {
		return ftpProxyPort;
	}

	public FtpConfig setFtpProxyPort(int ftpProxyPort) {
		this.ftpProxyPort = ftpProxyPort;
		return this;
	}

	public String getFtpProxyPassword() {
		return ftpProxyPassword;
	}

	public FtpConfig setFtpProxyPassword(String ftpProxyPassword) {
		this.ftpProxyPassword = ftpProxyPassword;
		return this;
	}

	public String getFtpProxyUser() {
		return ftpProxyUser;
	}

	public FtpConfig setFtpProxyUser(String ftpProxyUser) {
		this.ftpProxyUser = ftpProxyUser;
		return this;
	}

	public boolean isPrintHash() {
		return printHash;
	}

	public FtpConfig setPrintHash(boolean printHash) {
		this.printHash = printHash;
		return this;
	}

	public boolean isBinaryTransfer() {
		return binaryTransfer;
	}

	public FtpConfig setBinaryTransfer(boolean binaryTransfer) {
		this.binaryTransfer = binaryTransfer;
		return this;
	}

	public boolean isLocalActive() {
		return localActive;
	}

	public FtpConfig setLocalActive(boolean localActive) {
		this.localActive = localActive;
		return this;
	}

	public boolean isUseEpsvWithIPv4() {
		return useEpsvWithIPv4;
	}

	public FtpConfig setUseEpsvWithIPv4(boolean useEpsvWithIPv4) {
		this.useEpsvWithIPv4 = useEpsvWithIPv4;
		return this;
	}

	public long getKeepAliveTimeout() {
		return keepAliveTimeout;
	}

	public FtpConfig setKeepAliveTimeout(long keepAliveTimeout) {
		this.keepAliveTimeout = keepAliveTimeout;
		return this;
	}

	public int getControlKeepAliveReplyTimeout() {
		return controlKeepAliveReplyTimeout;
	}

	public FtpConfig setControlKeepAliveReplyTimeout(int controlKeepAliveReplyTimeout) {
		this.controlKeepAliveReplyTimeout = controlKeepAliveReplyTimeout;
		return this;
	}

	public String getRemoteFileDir() {
		return remoteFileDir;
	}

	public FtpConfig setRemoteFileDir(String remoteFileDir) {
		this.remoteFileDir = remoteFileDir;
		return this;
	}

	public FtpFileFilter getFtpFileFilter() {
		return ftpFileFilter;
	}

	public FtpConfig setFtpFileFilter(FtpFileFilter ftpFileFilter) {
		this.ftpFileFilter = ftpFileFilter;
		return this;
	}
	@Override
	protected void buildMsg(StringBuilder stringBuilder){
		super.buildMsg(stringBuilder);
		stringBuilder.append(",remoteFileDir:").append(this.remoteFileDir);
		stringBuilder.append(",ftpIP:").append(this.ftpIP);
		stringBuilder.append(",ftpPort:").append(this.ftpPort);
		stringBuilder.append(",ftpUser:").append(this.ftpUser);
//		stringBuilder.append(",ftpPassword:").append(this.ftpPassword);
		stringBuilder.append(",ftpPassword:******");

	}

	public String getEncoding() {
		return encoding;
	}

	public FtpConfig setEncoding(String encoding) {
		this.encoding = encoding;
		return this;
	}

	public boolean isDeleteRemoteFile() {
		return this.deleteRemoteFile;
	}

	public FtpConfig setDeleteRemoteFile(boolean deleteRemoteFile) {
		this.deleteRemoteFile = deleteRemoteFile;
		return this;
	}
}