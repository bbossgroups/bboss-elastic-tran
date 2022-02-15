package org.frameworkset.tran.output.excelftp;
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
import org.frameworkset.tran.output.fileftp.FileOupputContext;
import org.frameworkset.tran.output.fileftp.FileTransfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * <p>Description: 生产excel文件，可以根据配置将生成的文件发送到ftp服务器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/29 10:25
 * @author biaoping.yin
 * @version 1.0
 */
public class ExcelFileTransfer extends FileTransfer {

	private ExportExcelUtil exportExcelUtil;
	private ExcelFileOupputConfig excelFileOupputConfig;
	public ExcelFileTransfer(String taskInfo, FileOupputContext fileOupputContext, String dir, String filePath) throws IOException {
		super( taskInfo,  fileOupputContext,  dir,  filePath);
		excelFileOupputConfig = (ExcelFileOupputConfig) fileOupputContext.getFileOupputConfig();
	}

	@Override
	public void initTransfer() throws IOException {
		exportExcelUtil = new ExportExcelUtil();
		exportExcelUtil.buildSheet(excelFileOupputConfig,fileOupputContext);
	}

	@Override
	public void writeHeader() throws Exception {

	}
	public synchronized void writeData(List<CommonRecord> datas) throws IOException {
		exportExcelUtil.writeData(datas);
	}
	private static Logger logger = LoggerFactory.getLogger(ExcelFileTransfer.class);
	protected void flush() throws IOException {
		exportExcelUtil.write(file);
		this.close();
	}

	@Override
	public void close(){
		if(exportExcelUtil != null)
			exportExcelUtil.dispose();

	}
}
