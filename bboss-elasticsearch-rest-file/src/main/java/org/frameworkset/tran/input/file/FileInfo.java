package org.frameworkset.tran.input.file;
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

import java.io.File;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/28 8:58
 * @author biaoping.yin
 * @version 1.0
 */
public class FileInfo {
	public FileInfo(String charsetEncode, String filePath,
					File file, String fileId, FileConfig fileConfig) {
		this.charsetEncode = charsetEncode;
		this.filePath = filePath;
		this.file = file;
		this.originFile = file;
		this.originFilePath = filePath;
		this.fileId = fileId;
		this.fileConfig = fileConfig;
	}

	public FileInfo( String fileId) {

		this.fileId = fileId;

	}

	private String charsetEncode;
	private String filePath;
	/**
	 * 实际被采集的文件
	 */
	private File file;
	/**
	 * 实际被采集的文件号
	 */
	private String fileId;
	/**
	 * 原始文件信息，采集的文件中间肯能会重命名
	 */
	private File originFile;
	private String originFilePath;

	private FileConfig fileConfig;

	public String getCharsetEncode() {
		return charsetEncode;
	}

	public String getFilePath() {
		return filePath;
	}

	public File getFile() {
		return file;
	}

	void setFile(File file) {
		this.file = file;
	}

	public String getFileId() {
		return fileId;
	}




	public FileConfig getFileConfig() {
		return fileConfig;
	}

	public File getOriginFile() {
		return originFile;
	}

	public void setOriginFile(File originFile) {
		this.originFile = originFile;
	}

	public String getOriginFilePath() {
		return originFilePath;
	}

	public void setOriginFilePath(String originFilePath) {
		this.originFilePath = originFilePath;
	}
	public String getFileName(){
		return file.getName();
	}
	public String getOriginFileName(){
		return originFile.getName();
	}
}
