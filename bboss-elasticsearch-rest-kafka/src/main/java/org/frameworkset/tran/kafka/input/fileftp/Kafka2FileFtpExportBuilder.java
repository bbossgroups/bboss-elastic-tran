package org.frameworkset.tran.kafka.input.fileftp;
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
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.kafka.KafkaExportBuilder;
import org.frameworkset.tran.output.fileftp.FileOupputConfig;
import org.frameworkset.tran.output.fileftp.FileOupputContextImpl;

import java.lang.reflect.InvocationTargetException;

/**
 * <p>Description: kafka Log to file and ftp data tran plugin builder</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2FileFtpExportBuilder  extends KafkaExportBuilder {
	private static final String Kafka2FileFtpInputPlugin = "org.frameworkset.tran.kafka.input.fileftp.Kafka2FileFtpDataTranPlugin";


	@JsonIgnore
	private FileOupputConfig fileOupputConfig;

	public Kafka2FileFtpExportBuilder setFileOupputConfig(FileOupputConfig fileOupputConfig) {
		this.fileOupputConfig = fileOupputConfig;
		return this;

	}


	@Override
	public DataStream builder() {
		if(fileOupputConfig.getMaxFileRecordSize() == 0){//默认1万条记录一个文件
			fileOupputConfig.setMaxFileRecordSize(10000);
		}
		return super.builder();
	}

	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		FileOupputContextImpl fileFtpOupputContext = new FileOupputContextImpl(fileOupputConfig);
		fileFtpOupputContext.init();
		return fileFtpOupputContext;
	}


	protected void setTargetImportContext(DataStream dataStream){
			dataStream.setTargetImportContext(buildTargetImportContext(fileOupputConfig) );
	}

//	@Override
//	protected FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext) {
//		return new FileLog2FileFtpDataTranPlugin(importContext,targetImportContext);
//	}
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext, ImportContext targetImportContext)
	{

		try {
			Class<DataTranPlugin> clazz = (Class<DataTranPlugin>) Class.forName(Kafka2FileFtpInputPlugin);
			return clazz.getConstructor(ImportContext.class,ImportContext.class).newInstance( importContext, targetImportContext);// Kafka2ESInputPlugin(this);
		} catch (ClassNotFoundException e) {
			throw new ESDataImportException(Kafka2FileFtpInputPlugin,e);
		} catch (InstantiationException e) {
			throw new ESDataImportException(Kafka2FileFtpInputPlugin,e);
		} catch (InvocationTargetException e) {
			throw new ESDataImportException(Kafka2FileFtpInputPlugin,e);
		} catch (NoSuchMethodException e) {
			throw new ESDataImportException(Kafka2FileFtpInputPlugin,e);
		} catch (IllegalAccessException e) {
			throw new ESDataImportException(Kafka2FileFtpInputPlugin,e);
		}


	}


}
