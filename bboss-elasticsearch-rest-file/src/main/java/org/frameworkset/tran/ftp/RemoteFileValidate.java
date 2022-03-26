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
		/**
		 * 放置文件校验失败原因
		 */
		private String message;

		/**
		 * 标记校验是否成功，数据文件是否有效：true 有效，false 无效
		 * @return
		 */
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
	 * 校验数据文件合法性

	 * @param validateContext 封装校验数据文件信息
	 *     dataFile 待校验零时数据文件，可以根据文件名称获取对应文件的md5签名文件名、数据量稽核文件名称等信息，
	 *     remoteFile 通过数据文件对应的ftp/sftp文件路径，计算对应的目录获取md5签名文件、数据量稽核文件所在的目录地址
	 *     ftpContext ftp配置上下文对象
	 *     然后通过remoteFileAction下载md5签名文件、数据量稽核文件，再对数据文件进行校验即可
	 *     redownload 标记校验来源是否是因校验失败重新下载文件导致的校验操作，true 为重下后 文件校验，false为第一次下载校验
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
	public Result validateFile(ValidateContext validateContext);
}
