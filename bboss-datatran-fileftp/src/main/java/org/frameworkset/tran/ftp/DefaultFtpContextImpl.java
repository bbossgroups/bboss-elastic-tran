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

import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileFilter;
import org.frameworkset.tran.input.file.FtpFileFilter;
import org.frameworkset.tran.jobflow.context.JobFlowNodeExecuteContext;
import org.frameworkset.tran.jobflow.scan.JobFileFilter;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/27 14:50
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultFtpContextImpl  implements FtpContext {
	protected FtpConfig ftpConfig ;
    protected JobFlowNodeExecuteContext jobFlowNodeExecuteContext;
	public DefaultFtpContextImpl(FtpConfig ftpConfig) {
		this.ftpConfig = ftpConfig;
	}

    public DefaultFtpContextImpl(FtpConfig ftpConfig,JobFlowNodeExecuteContext jobFlowNodeExecuteContext) {
        this.ftpConfig = ftpConfig;
        this.jobFlowNodeExecuteContext = jobFlowNodeExecuteContext;
    }

    @Override
    public JobFlowNodeExecuteContext getJobFlowNodeExecuteContext() {
        return this.jobFlowNodeExecuteContext;
    }

    public boolean deleteRemoteFile(){
		return this.ftpConfig.isDeleteRemoteFile();
	}

	@Override
	public String getFtpIP() {
		return ftpConfig.getFtpIP();
	}
 

	@Override
	public int getFtpPort() {
		return ftpConfig.getFtpPort();
	}

	@Override
	public FtpConfig getFtpConfig() {
		return ftpConfig;
	}

	@Override
	public FileConfig getFileConfig() {
		return ftpConfig.getFileConfig();
	}


	@Override
	public String getRemoteFileDir() {
		return ftpConfig.getRemoteFileDir();
	}

	@Override
	public String getFtpUser() {
		return ftpConfig.getFtpUser();
	}
    /**
     *       Sets the current data connection mode to {@code PASSIVE_LOCAL_DATA_CONNECTION_MODE}. Use this method only for data transfers between the client and
     *      server. This method causes a PASV (or EPSV) command to be issued to the server before the opening of every data connection, telling the server to open a
     *      data port to which the client will connect to conduct data transfers. The FTPClient will stay in PASSIVE_LOCAL_DATA_CONNECTION_MODE until the
     *      mode is changed by calling some other method such as enterLocalActiveMode enterLocalActiveMode()
     *      <p>
     *      <strong>N.B.</strong> currently calling any connect method will reset the mode to ACTIVE_LOCAL_DATA_CONNECTION_MODE.
     *      </p>
     */
    @Override
    public  Boolean enterLocalPassiveMode(){
        return ftpConfig.enterLocalPassiveMode();
    }
	@Override
	public String getFtpPassword() {
		return ftpConfig.getFtpPassword();
	}

	@Override
	public List<String> getHostKeyVerifiers() {
		return ftpConfig.getHostKeyVerifiers();
	}

	@Override
	public String getFtpProtocol() {
		return ftpConfig.getFtpProtocol();
	}

	@Override
	public String getFtpTrustmgr() {
		return ftpConfig.getFtpTrustmgr();
	}

	@Override
	public Boolean localActive() {
		return ftpConfig.isLocalActive();
	}

	@Override
	public Boolean useEpsvWithIPv4() {
		return ftpConfig.isUseEpsvWithIPv4();
	}

	@Override
	public int getTransferProtocol() {
		return ftpConfig.getTransferProtocol();
	}

	@Override
	public String getFtpProxyHost() {
		return ftpConfig.getFtpProxyHost();
	}

	@Override
	public int getFtpProxyPort() {
		return ftpConfig.getFtpProxyPort();
	}

	@Override
	public String getFtpProxyUser() {
		return ftpConfig.getFtpProxyUser();
	}

	@Override
	public String getFtpProxyPassword() {
		return ftpConfig.getFtpProxyPassword();
	}

	@Override
	public boolean printHash() {
		return ftpConfig.isPrintHash();
	}

	@Override
	public Boolean binaryTransfer() {
		return ftpConfig.isBinaryTransfer();
	}

	@Override
	public long getKeepAliveTimeout() {
		return ftpConfig.getKeepAliveTimeout();
	}
	@Override
	public long getSocketTimeout(){
		return ftpConfig.getSocketTimeout();
	}
	@Override
	public long getConnectTimeout(){
		return ftpConfig.getConnectTimeout();
	}

	@Override
	public int getControlKeepAliveReplyTimeout() {
		return ftpConfig.getControlKeepAliveReplyTimeout();
	}

	@Override
	public FileFilter getFileFilter() {
		return null;
	}

    @Override
    public JobFileFilter getJobFileFilter() {
        return null;
    }

    @Override
	public FtpFileFilter getFtpFileFilter() {
		return ftpConfig.getFtpFileFilter();
	}

	@Override
	public String getEncoding() {
		return ftpConfig.getEncoding();
	}


}
