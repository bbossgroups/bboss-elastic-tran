package org.frameworkset.tran.input.excel;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.input.file.FileConfig;
import org.frameworkset.tran.input.file.FileImportConfig;
import org.frameworkset.tran.input.file.FileListenerService;
import org.frameworkset.tran.input.file.FileReaderTask;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.File;

/**
 * <p>Description: excel文件数据采集配置</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/2/11 17:32
 * @author biaoping.yin
 * @version 1.0
 */
public class ExcelFileImportConfig extends FileImportConfig {

	public FileReaderTask buildFileReaderTask(TaskContext taskContext, File file, String fileId, FileConfig fileConfig, long pointer, FileListenerService fileListenerService, BaseDataTran fileDataTran,
											  Status currentStatus , FileImportConfig fileImportConfig ){
		FileReaderTask task = new ExcelFileReaderTask(taskContext,file,fileId,(ExcelFileConfig) fileConfig,pointer,
				fileListenerService,fileDataTran,currentStatus,fileImportConfig);
		return task;
	}
	public FileReaderTask buildFileReaderTask(String fileId,  Status currentStatus,FileImportConfig fileImportConfig ){
		FileReaderTask task =  new ExcelFileReaderTask(fileId,currentStatus,fileImportConfig);
		return task;
	}
}
