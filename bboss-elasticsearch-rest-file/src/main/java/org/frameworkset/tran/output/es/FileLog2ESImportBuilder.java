package org.frameworkset.tran.output.es;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileImportConfig;
import org.frameworkset.tran.input.file.FileImportContext;
import org.frameworkset.tran.input.file.FileListener;
import org.frameworkset.tran.input.file.FileListenerService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:21
 * @author yin-bp@163.com
 * @version 1.0
 */
public class FileLog2ESImportBuilder extends BaseImportBuilder {

	@JsonIgnore
	private FileImportConfig fileImportConfig;

	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){

		FileLog2ESDataTranPlugin file2ESDataTranPlugin = new FileLog2ESDataTranPlugin(  importContext,  targetImportContext);
		FileListener fileListener = new FileListener(new FileListenerService((FileImportContext) importContext,file2ESDataTranPlugin));
		file2ESDataTranPlugin.setFileListener(fileListener);
		return file2ESDataTranPlugin;
	}

	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}

	protected ImportContext buildImportContext(BaseImportConfig importConfig){
		return new FileImportContext(importConfig);
	}
//	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
//		return new ESImportContext(importConfig);
//	}
	public DataStream builder(){
		super.builderConfig();
		super.buildImportConfig(fileImportConfig);
		DataStream dataStream = createDataStream();
		dataStream.setImportConfig(fileImportConfig);
		dataStream.setImportContext(buildImportContext(fileImportConfig));

		try {
			if(logger.isInfoEnabled()) {
				logger.info("FileLog2Elasticsearch Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}


//		if(fileImportConfig != null)
//			dataStream.setTargetImportContext(buildTargetImportContext(fileImportConfig) );
//		else
//			dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}
	public FileImportConfig getFileImportConfig() {
		return fileImportConfig;
	}

	public void setFileImportConfig(FileImportConfig fileImportConfig) {
		this.fileImportConfig = fileImportConfig;
	}

}
