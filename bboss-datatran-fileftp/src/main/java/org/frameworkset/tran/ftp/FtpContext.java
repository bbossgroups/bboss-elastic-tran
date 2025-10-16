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
public interface FtpContext<T extends FtpContext> {
	String getFtpIP();
	int getFtpPort();
    Boolean enterLocalPassiveMode();
	FtpConfig getFtpConfig();
	FileConfig getFileConfig();
    JobFlowNodeExecuteContext getJobFlowNodeExecuteContext();
	String getRemoteFileDir();

	String getFtpUser() ;

	String getFtpPassword() ;
	List<String> getHostKeyVerifiers();

	String getFtpProtocol();
	String getFtpTrustmgr();
	Boolean localActive();
	Boolean useEpsvWithIPv4();
	int getTransferProtocol();
	String getFtpProxyHost();
	int getFtpProxyPort();
	String getFtpProxyUser();
	String getFtpProxyPassword();
	boolean printHash();
	Boolean binaryTransfer();
	long getKeepAliveTimeout();
	long getSocketTimeout();
	long getConnectTimeout();


	int getControlKeepAliveReplyTimeout();
	FileFilter getFileFilter();
    JobFileFilter getJobFileFilter();
	FtpFileFilter getFtpFileFilter();

	String getEncoding();
}
