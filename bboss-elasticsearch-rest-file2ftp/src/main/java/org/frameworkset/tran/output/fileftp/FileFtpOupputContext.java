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

import org.frameworkset.tran.CommonRecord;

import java.io.Writer;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 17:31
 * @author biaoping.yin
 * @version 1.0
 */
public interface FileFtpOupputContext {
	public final int TRANSFER_PROTOCOL_FTP = 1;
	public final int TRANSFER_PROTOCOL_SFTP = 2;

	public String generateFileName(int fileSeq);
	public void generateReocord(org.frameworkset.tran.context.Context context, CommonRecord record, Writer builder);

	public String getFtpIP();
	public int getFtpPort();
	public String getFtpUser() ;

	public String getFtpPassword() ;
	public List<String> getHostKeyVerifiers();
	public String getFileDir() ;
	public String getRemoteFileDir() ;
	public int getFileWriterBuffsize();
	public int getMaxFileRecordSize();
	public String getFtpProtocol();
	public String getFtpTrustmgr();

	public String getFtpProxyHost();
	public int getFtpProxyPort();
	public String getFtpProxyUser();
	public String getFtpProxyPassword();
	public boolean printHash();
	public boolean binaryTransfer();
	public long getKeepAliveTimeout();
	public int getControlKeepAliveReplyTimeout();
	public String getEncoding();
	public String getFtpServerType();
	public boolean localActive();
	public boolean useEpsvWithIPv4();
	public int getTransferProtocol();


}
