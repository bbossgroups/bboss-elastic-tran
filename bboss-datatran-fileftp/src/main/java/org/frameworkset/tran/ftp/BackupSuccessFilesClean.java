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

import org.frameworkset.tran.file.monitor.FileCleanThread;
import org.frameworkset.tran.plugin.file.input.FileInputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: 清理过期失效的备份文件</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/4 14:48
 * @author biaoping.yin
 * @version 1.0
 */
public class BackupSuccessFilesClean {
	private static final Logger logger = LoggerFactory.getLogger(BackupSuccessFilesClean.class);
	private FileInputConfig fileImportConfig;
	private FileCleanThread fileCleanThread;
	public BackupSuccessFilesClean(FileInputConfig fileImportConfig){
		this.fileImportConfig = fileImportConfig;

	}
	public void start(){
		FileCleanThread fileCleanThread = new FileCleanThread("BackupSuccessFilesClean-Thread",
															   fileImportConfig.getBackupSuccessFileDir(),
				                                               fileImportConfig.getBackupSuccessFileInterval(),
																fileImportConfig.getBackupSuccessFileLiveTime() * 1000l);
		fileCleanThread.start();
		this.fileCleanThread = fileCleanThread;

	}
	public void stop(){
		if(fileCleanThread != null) {
			fileCleanThread.stopThread();

		}
	}


}
