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

import org.frameworkset.tran.input.RemoteContext;

import java.io.File;

/**
 * <p>Description: 封装校验数据文件信息
 *   dataFile 待校验零时数据文件，可以根据文件名称获取对应文件的md5签名文件名、数据量稽核文件名称等信息，
 *   remoteFile 通过数据文件对应的ftp/sftp文件路径，计算对应的目录获取md5签名文件、数据量稽核文件所在的目录地址
 *   ftpContext ftp配置上下文对象
 *   然后通过remoteFileAction下载md5签名文件、数据量稽核文件，再对数据文件进行校验即可
 *   redownload 标记校验来源是否是因校验失败重新下载文件导致的校验操作，true 为重下后 文件校验，false为第一次下载校验</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/26
 * @author biaoping.yin
 * @version 1.0
 */
public class ValidateContext {
	public ValidateContext(File dataFile, String remoteFile, RemoteContext ftpContext, RemoteFileAction remoteFileAction, boolean redownload) {
		this.dataFile = dataFile;
		this.remoteFile = remoteFile;
		this.ftpContext = ftpContext;
		this.remoteFileAction = remoteFileAction;
		this.redownload = redownload;
	}
	public ValidateContext(){

	}

	/**
	 * 下载的待校验零时数据文件，可以根据文件名称获取对应文件的md5签名文件名、数据量稽核文件名称等信息，然后通过
	 * remoteFileAction下载md5签名文件、数据量稽核文件，再对数据文件进行校验即可
	 */
	private File dataFile;
	/**
	 * 数据文件对应的ftp/sftp文件路径，可以根据对应的目录获取md5签名文件、数据量稽核文件所在的目录地址
	 */
	private String remoteFile;
	/**
	 * ftp配置上下文对象
	 */
	private RemoteContext ftpContext;
	/**
	 * 文件下载接口，提供了文件下载方法，用于下载md5签名文件、数据量稽核文件
	 */
	private RemoteFileAction remoteFileAction;
	/**
	 * 标记校验来源是否是因校验失败重新下载文件导致的校验操作，true 为重下后 文件校验，false为第一次下载校验
	 */
	private boolean redownload;

	public File getDataFile() {
		return dataFile;
	}

	public void setDataFile(File dataFile) {
		this.dataFile = dataFile;
	}

	public String getRemoteFile() {
		return remoteFile;
	}

	public void setRemoteFile(String remoteFile) {
		this.remoteFile = remoteFile;
	}

	public RemoteContext getFtpContext() {
		return ftpContext;
	}

	public void setFtpContext(RemoteContext ftpContext) {
		this.ftpContext = ftpContext;
	}

	public RemoteFileAction getRemoteFileAction() {
		return remoteFileAction;
	}

	public void setRemoteFileAction(RemoteFileAction remoteFileAction) {
		this.remoteFileAction = remoteFileAction;
	}

	public boolean isRedownload() {
		return redownload;
	}

	public void setRedownload(boolean redownload) {
		this.redownload = redownload;
	}
}
