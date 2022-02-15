package org.frameworkset.tran.input.fileftp.es;
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
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.input.ESExportBuilder;
import org.frameworkset.tran.output.fileftp.FileOupputConfig;
import org.frameworkset.tran.output.fileftp.FileOupputContextImpl;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2FileFtpExportBuilder extends ESExportBuilder {
	@JsonIgnore
	private FileOupputConfig fileOupputConfig;
	public ES2FileFtpExportBuilder setFileOupputConfig(FileOupputConfig fileOupputConfig) {
		this.fileOupputConfig = fileOupputConfig;
		return this;
	}

	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new ES2FileFtpDataTranPlugin(  importContext,  targetImportContext);
	}


	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		FileOupputContextImpl fileFtpOupputContext = new FileOupputContextImpl(fileOupputConfig);
		fileFtpOupputContext.init();
		return fileFtpOupputContext;
	}
	public DataStream builder(){
		DataStream dataStream = super.builder();
//		this.buildDBConfig();
//		this.buildStatusDBConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("ES2FileFtp Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}


		if(fileOupputConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(fileOupputConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}


}
