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

import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.output.ftp.FtpOutConfig;
import org.frameworkset.tran.util.RecordGenerator;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:48
 * @author biaoping.yin
 * @version 1.0
 */
public class FileOupputConfig extends BaseImportConfig {
	private FtpOutConfig ftpOutConfig;
	/**
	 * 输出文件记录处理器:org.frameworkset.tran.kafka.output.fileftp.ReocordGenerator
	 */
	private RecordGenerator recordGenerator;

	/**
	 *  导出文件名称生成接口实现类型（必须指定）：org.frameworkset.tran.kafka.output.fileftp.FilenameGenerator
	 */
	private FilenameGenerator filenameGenerator;
	private String fileDir;
	private int fileWriterBuffsize ;
	private int maxFileRecordSize;
	private boolean disableftp;



	public String getFileDir() {
		return fileDir;
	}

	public FileOupputConfig setFileDir(String fileDir) {
		this.fileDir = fileDir;
		return  this;
	}

	public RecordGenerator getRecordGenerator() {
		return recordGenerator;
	}


	public FileOupputConfig setRecordGenerator(RecordGenerator recordGenerator) {
		this.recordGenerator = recordGenerator;
		return  this;
	}

	public FilenameGenerator getFilenameGenerator() {
		return filenameGenerator;
	}

	public FileOupputConfig setFilenameGenerator(FilenameGenerator filenameGenerator) {
		this.filenameGenerator = filenameGenerator;
		return  this;
	}

	public int getFileWriterBuffsize() {
		return fileWriterBuffsize;
	}

	public FileOupputConfig setFileWriterBuffsize(int fileWriterBuffsize) {
		this.fileWriterBuffsize = fileWriterBuffsize;
		return  this;
	}

	public int getMaxFileRecordSize() {
		return maxFileRecordSize;
	}

	public FileOupputConfig setMaxFileRecordSize(int maxFileRecordSize) {
		this.maxFileRecordSize = maxFileRecordSize;
		return  this;
	}

	public boolean isDisableftp() {
		return disableftp;
	}

	public FileOupputConfig setDisableftp(boolean disableftp) {
		this.disableftp = disableftp;
		return  this;
	}


	public FtpOutConfig getFtpOutConfig() {
		return ftpOutConfig;
	}

	public FileOupputConfig setFtpOutConfig(FtpOutConfig ftpOutConfig) {
		this.ftpOutConfig = ftpOutConfig;
		return this;
	}
}
