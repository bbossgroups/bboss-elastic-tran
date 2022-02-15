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

import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.output.excelftp.ExcelFileFtpOutPutDataTran;
import org.frameworkset.tran.output.excelftp.ExcelFileOupputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/2/15 17:17
 * @author biaoping.yin
 * @version 1.0
 */
public class FileFtpOutPutUtil {

	public static FileFtpOutPutDataTran buildFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet,
																   ImportContext importContext,
			ImportContext targetImportContext,CountDownLatch countDownLatch, Status currentStatus){
		FileOupputContext fileOupputContext = (FileOupputContext)targetImportContext;
		FileFtpOutPutDataTran fileFtpOutPutDataTran = null;
		if(fileOupputContext.getFileOupputConfig() instanceof ExcelFileOupputConfig){
			fileFtpOutPutDataTran = new ExcelFileFtpOutPutDataTran(taskContext,tranResultSet,importContext,   targetImportContext,  countDownLatch,currentStatus);
		}
		else{
			fileFtpOutPutDataTran = new FileFtpOutPutDataTran(taskContext,tranResultSet,importContext,   targetImportContext, countDownLatch, currentStatus);
		}

		return fileFtpOutPutDataTran;
	}
	public static FileFtpOutPutDataTran buildFileFtpOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext,
																   ImportContext targetImportContext, Status currentStatus){
		FileOupputContext fileOupputContext = (FileOupputContext)targetImportContext;
		FileFtpOutPutDataTran fileFtpOutPutDataTran = null;
		if(fileOupputContext.getFileOupputConfig() instanceof ExcelFileOupputConfig){
			fileFtpOutPutDataTran = new ExcelFileFtpOutPutDataTran(taskContext,tranResultSet,importContext,   targetImportContext,  currentStatus);
		}
		else{
			fileFtpOutPutDataTran = new FileFtpOutPutDataTran(taskContext,tranResultSet,importContext,   targetImportContext,  currentStatus);
		}

		return fileFtpOutPutDataTran;
	}
}
