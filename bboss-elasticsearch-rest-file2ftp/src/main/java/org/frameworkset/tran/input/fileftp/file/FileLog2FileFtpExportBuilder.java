package org.frameworkset.tran.input.fileftp.file;
/**
 * Copyright 2008 biaoping.yin
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.output.FileLogBaseImportBuilder;
import org.frameworkset.tran.output.fileftp.FileFtpOupputConfig;
import org.frameworkset.tran.output.fileftp.FileFtpOupputContextImpl;

/**
 * <p>Description: file Log to file and ftp data tran plugin builder</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class FileLog2FileFtpExportBuilder extends FileLogBaseImportBuilder {


	@JsonIgnore
	private FileFtpOupputConfig fileFtpOupputConfig;

	public FileLog2FileFtpExportBuilder setFileFtpOupputConfig(FileFtpOupputConfig fileFtpOupputConfig) {
		this.fileFtpOupputConfig = fileFtpOupputConfig;
		return this;

	}




	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		FileFtpOupputContextImpl fileFtpOupputContext = new FileFtpOupputContextImpl(fileFtpOupputConfig);
		fileFtpOupputContext.init();
		return fileFtpOupputContext;
	}


	protected void setTargetImportContext(DataStream dataStream){
			dataStream.setTargetImportContext(buildTargetImportContext(fileFtpOupputConfig) );
	}

	@Override
	protected FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext) {
		return new FileLog2FileFtpDataTranPlugin(importContext,targetImportContext);
	}



}
