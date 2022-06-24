package org.frameworkset.tran.ftp;
/**
 * Copyright 2022 bboss
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

import java.io.File;

/**
 * <p>Description: FTP/SFTP工具类</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/5/28
 * @author biaoping.yin
 * @version 1.0
 */
public class RemoteFileTransfer {
	/**
	 * 文件下载
	 * @param ftpConfig
	 * @param remoteFile
	 * @param dest ftp协议时必须是存放的文件路径 sftp时是下载文件存放目录
	 */
	public static void downloadFile(final FtpConfig ftpConfig,final String remoteFile,final String dest){
		DefaultFtpContextImpl ftpContext = new DefaultFtpContextImpl(ftpConfig);
		if (ftpContext.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {

			FtpTransfer.downloadFile(ftpContext, remoteFile,  dest);

		} else {
			SFTPTransfer.downloadFile(ftpContext, remoteFile,  dest);
		}
	}
	/**
	 * 删除文件
	 * @param ftpConfig
	 * @param remoteFile
	 */
	public static void deleteFile(final FtpConfig ftpConfig,final String remoteFile){
		DefaultFtpContextImpl ftpContext = new DefaultFtpContextImpl(ftpConfig);
		if (ftpContext.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {

				FtpTransfer.deleteFile(ftpContext, remoteFile);

		} else {
				SFTPTransfer.deleteFile(ftpContext, remoteFile);
		}
	}

	/**
	 * 文件上传
	 * @param ftpConfig
	 * @param remote ftp协议时必须是上传远程文件路径 sftp时是上传文件存放目录
	 * @param filePath
	 */
	public static void sendFile(final FtpConfig ftpConfig,final String remote, final String filePath){
		DefaultFtpContextImpl ftpContext = new DefaultFtpContextImpl(ftpConfig);
		File file = new File(filePath);
		if (ftpContext.getTransferProtocol() == FtpConfig.TRANSFER_PROTOCOL_FTP) {

			FtpTransfer.sendFile(ftpContext, file, remote);

		} else {
			SFTPTransfer.sendFile(ftpContext, file,remote);
		}
	}

}
