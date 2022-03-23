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
 * <p>Description: 数据文件校验接口</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/23
 * @author biaoping.yin
 * @version 1.0
 */
public interface RemoteFileValidate {
	/**
	 * 文件内容校验成功
	 */
	public static int FILE_VALIDATE_OK = 1;
	/**
	 * 校验失败不处理文件
	 */
	public static int FILE_VALIDATE_FAILED = 2;
	/**
	 * 文件内容校验失败并删除已下载文件
	 */
	public static int FILE_VALIDATE_FAILED_DELETE = 5;

	/**
	 * 文件内容校验失败并备份已下载文件（保留字段，目前不起作用）
	 */
	public static int FILE_VALIDATE_FAILED_BACKUP = 3;

	/**
	 * 文件内容校验失败并重下文件（保留字段，目前不起作用）
	 */
	public static int FILE_VALIDATE_FAILED_REDOWNLOAD = 3;
	public static class Result{
		public static final Result default_ok = new Result();
		private int validateResult = FILE_VALIDATE_OK;
		private int redownloadCounts = 3;
		private String message;
		public boolean isOk(){
			return validateResult == FILE_VALIDATE_OK;
		}
		public int getValidateResult() {
			return validateResult;
		}

		public void setValidateResult(int validateResult) {
			this.validateResult = validateResult;
		}

		public int getRedownloadCounts() {
			return redownloadCounts;
		}

		public void setRedownloadCounts(int redownloadCounts) {
			this.redownloadCounts = redownloadCounts;
		}

		public String getMessage() {
			return message;
		}

		public void setMessage(String message) {
			this.message = message;
		}
	}


	/**
	 * 校验数据文件
	 * @param dataFile 下载的零时数据文件
	 * @param ftpContext ftp配置上下文
	 * @param remoteFileAction  下载文件和删除远程文件接口
	 * @param redownload 标记是否重新下载校验处理
	 * @return int
	 * 文件内容校验成功
	 * 	RemoteFileValidate.FILE_VALIDATE_OK = 1;
	 * 	校验失败不处理文件
	 * 	RemoteFileValidate.FILE_VALIDATE_FAILED = 2;
	 * 	文件内容校验失败并备份已下载文件
	 * 	RemoteFileValidate.FILE_VALIDATE_FAILED_BACKUP = 3;
	 * 	文件内容校验失败并删除已下载文件
	 * 	RemoteFileValidate.FILE_VALIDATE_FAILED_DELETE = 5;
	 */
	public Result validateFile(File dataFile, String remoteFile,FtpContext ftpContext, RemoteFileAction remoteFileAction,boolean redownload);
}
