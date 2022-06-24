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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.file.monitor.FileCleanThread;
import org.frameworkset.tran.plugin.file.output.FileOutputConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: 清理发送成功的备份文件服务</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/4 14:48
 * @author biaoping.yin
 * @version 1.0
 */
public class SuccessFilesClean{
	private String transferSuccessFileDir;
	private static final Logger logger = LoggerFactory.getLogger(SuccessFilesClean.class);
	private FileOutputConfig fileOutputConfig;
	public SuccessFilesClean(FileOutputConfig fileOutputConfig){
		this.fileOutputConfig = fileOutputConfig;
		transferSuccessFileDir = SimpleStringUtil.getPath(fileOutputConfig.getFileDir(),"transferSuccessFileDir");

	}
	public void start(){
		FileCleanThread fileCleanThread = new FileCleanThread("SuccessFTPFilesClean-Thread",
				transferSuccessFileDir,
				fileOutputConfig.getSuccessFilesCleanInterval(),
				fileOutputConfig.getFileLiveTime() * 1000L);
		fileCleanThread.start();
	}




}
