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
import org.frameworkset.tran.input.RemoteContext;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.input.file.RemoteFileChannel;
import org.frameworkset.tran.jobflow.DownloadfileConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class FtpConfig extends RemoteContext<FtpConfig> {
    private Logger logger = LoggerFactory.getLogger(FtpConfig.class);


    
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

	private int ftpProxyPort;

	private String ftpProxyPassword;
	private String ftpProxyUser;
	private boolean printHash;
	private Boolean binaryTransfer = true;
	private Boolean localActive;

	private Boolean useEpsvWithIPv4;
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
    private Boolean enterLocalPassiveMode;
	private int controlKeepAliveReplyTimeout;
	private String encoding;
	private int downloadWorkThreads = 3;
	private String transferProtocolName;
	private boolean inited;
	public int getDownloadWorkThreads() {
		return downloadWorkThreads;
	}

	public String getTransferProtocolName() {
		return transferProtocolName;
	}

	public FtpConfig setDownloadWorkThreads(int downloadWorkThreads) {
		this.downloadWorkThreads = downloadWorkThreads;
		return this;
	}
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
 
//	@Override
	public void init(FileConfig fileConfig){
		if(inited)
			return;
		inited = true;
//		super.init();
//		this.setFtpConfig(fileConfig);
		if(transferProtocol == FtpConfig.TRANSFER_PROTOCOL_FTP)
			this.transferProtocolName = "FTP";
		else
			this.transferProtocolName = "SFTP";
		downloadTempDir = SimpleStringUtil.getPath(fileConfig.getSourcePath(),"temp");
		fileConfig.setEnableInode(false);
        if(fileConfig.getCloseOlderTime() != null && fileConfig.getCloseOlderTime() > 0L) {
            fileConfig.setCloseEOF(false);//指定CloseOlderTime
        }
        else if(fileConfig.getIgnoreOlderTime() != null && fileConfig.getIgnoreOlderTime() > 0L) {
            fileConfig.setCloseEOF(false);//指定CloseOlderTime
        }
        else{
            logger.info("ftp/sftp远程文件采集:setCloseEOF(true)");
            fileConfig.setCloseEOF(true);//已经结束的文件内容采集完毕后关闭文件对应的采集通道，后续不再监听对应文件的内容变化
        }
		File f = new File(downloadTempDir);
		if(!f.exists())
			f.mkdirs();
		if(remoteFileChannel == null && getDownloadWorkThreads() > 0) {
			remoteFileChannel = new RemoteFileChannel();
			//用远程文件路径作为线程池名称
			remoteFileChannel.setThreadName("RemoteFileDownloadChannel-"+getRemoteFileDir());
			remoteFileChannel.setWorkThreads(getDownloadWorkThreads());
			remoteFileChannel.init();

		}
//		return this;
	}

    public void initJob(DownloadfileConfig downloadfileConfig){
        if(inited)
            return;
        inited = true;
        if(transferProtocol == FtpConfig.TRANSFER_PROTOCOL_FTP)
            this.transferProtocolName = "FTP";
        else
            this.transferProtocolName = "SFTP";
        downloadTempDir = SimpleStringUtil.getPath(getSourcePath(),"temp");
         
        File f = new File(downloadTempDir);
        if(!f.exists())
            f.mkdirs();
        super.initJob();
        if(remoteFileChannel == null && getDownloadWorkThreads() > 0) {
            remoteFileChannel = new RemoteFileChannel();
            //用远程文件路径作为线程池名称
            remoteFileChannel.setThreadName("RemoteFileDownloadChannel-"+getRemoteFileDir());
            remoteFileChannel.setWorkThreads(getDownloadWorkThreads());
            remoteFileChannel.init();

        }
//		return this;
    }



	public int getTransferProtocol() {
		return transferProtocol;
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

	public Boolean isBinaryTransfer() {
		return binaryTransfer;
	}

	public FtpConfig setBinaryTransfer(Boolean binaryTransfer) {
		this.binaryTransfer = binaryTransfer;
		return this;
	}

	public Boolean isLocalActive() {
		return localActive;
	}

	public FtpConfig setLocalActive(Boolean localActive) {
		this.localActive = localActive;
		return this;
	}

	public Boolean isUseEpsvWithIPv4() {
		return useEpsvWithIPv4;
	}

	public FtpConfig setUseEpsvWithIPv4(Boolean useEpsvWithIPv4) {
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


	public FtpFileFilter getFtpFileFilter() {
		return ftpFileFilter;
	}

	public FtpConfig setFtpFileFilter(FtpFileFilter ftpFileFilter) {
		this.ftpFileFilter = ftpFileFilter;
		return this;
	}
//	@Override
	public void buildMsg(StringBuilder stringBuilder){
//		super.buildMsg(stringBuilder);
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

 
 

 

	public long getSocketTimeout() {
		return socketTimeout;
	}
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
	public FtpConfig setSocketTimeout(long socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}

	public long getConnectTimeout() {
		return connectTimeout;
	}
	/**
	 * 建立连接超时时间，单位：毫秒
	 */
	public FtpConfig setConnectTimeout(long connectTimeout) {
		this.connectTimeout = connectTimeout;
		return this;
	}

    public Boolean enterLocalPassiveMode() {
        return enterLocalPassiveMode;
    }

    public FtpConfig setEnterLocalPassiveMode(Boolean enterLocalPassiveMode) {
        this.enterLocalPassiveMode = enterLocalPassiveMode;
        return this;
    }
}
