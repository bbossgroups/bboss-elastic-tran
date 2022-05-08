package org.frameworkset.tran.output;
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
import org.frameworkset.tran.input.file.*;

/**
 * <p>Description: 日志数据采集插件基础构建器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:21
 * @author yin-bp@163.com
 * @version 1.0
 */
public abstract class FileLogBaseImportBuilder extends BaseImportBuilder {
	protected String message = "FileLog 2 Elasticsearch Import Configs:";
	@JsonIgnore
	protected FileImportConfig fileImportConfig;



	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}

	protected ImportContext buildImportContext(BaseImportConfig importConfig){
		FileImportContext fileImportContext = new FileImportContext(importConfig);
		fileImportContext.init();
		return fileImportContext;
	}
//	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
//		return new ESImportContext(importConfig);
//	}
@Override
protected DataStream innerBuilder(){
		super.builderConfig();
		super.buildImportConfig(fileImportConfig);
		DataStream dataStream = createDataStream();
		dataStream.setImportConfig(fileImportConfig);
		dataStream.setImportContext(buildImportContext(fileImportConfig));

		try {
			if(logger.isInfoEnabled()) {
				logger.info(message);
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}


//		if(fileImportConfig != null)
//			dataStream.setTargetImportContext(buildTargetImportContext(fileImportConfig) );
//		else
//			dataStream.setTargetImportContext(dataStream.getImportContext());
		setTargetImportContext( dataStream);
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}

	protected void setTargetImportContext(DataStream dataStream){
		dataStream.setTargetImportContext(dataStream.getImportContext());
	}
	public FileImportConfig getFileImportConfig() {
		return fileImportConfig;
	}

	public void setFileImportConfig(FileImportConfig fileImportConfig) {
		this.fileImportConfig = fileImportConfig;
	}
	protected abstract FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext);
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){

		FileBaseDataTranPlugin file2ESDataTranPlugin = createFileBaseDataTranPlugin(  importContext,   targetImportContext);
		FileListenerService fileListenerService = new FileListenerService((FileImportContext) importContext,file2ESDataTranPlugin);
//		FileListener fileListener = new FileListener(new FileListenerService((FileImportContext) importContext,file2ESDataTranPlugin));
		fileListenerService.init();
		file2ESDataTranPlugin.setFileListenerService(fileListenerService);
		return file2ESDataTranPlugin;
	}
}
