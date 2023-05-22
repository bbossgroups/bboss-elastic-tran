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

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/27 14:50
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultFtpContextImpl implements FtpContext {
	protected FtpConfig ftpConfig ;
	public DefaultFtpContextImpl(FtpConfig ftpConfig) {
		this.ftpConfig = ftpConfig;
	}
	public boolean deleteRemoteFile(){
		return this.ftpConfig.isDeleteRemoteFile();
	}

	@Override
	public String getFtpIP() {
		return ftpConfig.getFtpIP();
	}

	@Override
	public RemoteFileValidate getRemoteFileValidate() {
		return ftpConfig.getRemoteFileValidate();
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
		return null;
	}


	@Override
	public String getRemoteFileDir() {
		return ftpConfig.getRemoteFileDir();
	}

	@Override
	public String getFtpUser() {
		return ftpConfig.getFtpUser();
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
	public FtpFileFilter getFtpFileFilter() {
		return ftpConfig.getFtpFileFilter();
	}

	@Override
	public String getEncoding() {
		return ftpConfig.getEncoding();
	}


}
