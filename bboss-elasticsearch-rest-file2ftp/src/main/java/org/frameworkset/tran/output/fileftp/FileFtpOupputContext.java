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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.ftp.FtpContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.RecordGenerator;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 17:31
 * @author biaoping.yin
 * @version 1.0
 */
public interface FileFtpOupputContext extends FtpContext {


	public String generateFileName(TaskContext taskContext, int fileSeq);
	public void generateReocord(org.frameworkset.tran.context.Context context, CommonRecord record, Writer builder) throws Exception;
	public long getFailedFileResendInterval();
	public long getSuccessFilesCleanInterval();
	public int getFileLiveTime();

	public String getFileDir() ;
	public int getFileWriterBuffsize();
	public int getMaxFileRecordSize();

	public String getEncoding();
	public String getFtpServerType();

	public boolean transferEmptyFiles();
	public boolean backupSuccessFiles();
	public boolean disableftp();
	public RecordGenerator getRecordGenerator() ;

}
