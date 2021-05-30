package org.frameworkset.tran.kafka.output.filelog;
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
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.kafka.output.KafkaOutputContextImpl;
import org.frameworkset.tran.output.FileLogBaseImportBuilder;

import java.lang.reflect.InvocationTargetException;

/**
 * <p>Description: 采集日志数据并发送到kafka插件构建器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:21
 * @author yin-bp@163.com
 * @version 1.0
 */
public class FileLog2KafkaImportBuilder extends FileLogBaseImportBuilder {
	private static final String FileLog2KafkaDataTranPlugin = "org.frameworkset.tran.kafka.output.filelog.FileLog2KafkaDataTranPlugin";
	public KafkaOutputConfig getKafkaOutputConfig() {
		return kafkaOutputConfig;
	}

	public FileLog2KafkaImportBuilder setKafkaOutputConfig(KafkaOutputConfig kafkaOutputConfig) {
		this.kafkaOutputConfig = kafkaOutputConfig;
		return this;
	}

	@JsonIgnore
	private KafkaOutputConfig kafkaOutputConfig;

	@Override
	protected FileBaseDataTranPlugin createFileBaseDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		try {
			Class clazz = Class.forName(FileLog2KafkaDataTranPlugin);
			return (FileBaseDataTranPlugin)clazz.getConstructor(ImportContext.class,ImportContext.class).newInstance( importContext, targetImportContext);// FileLog2KafkaDataTranPlugin(this);
		} catch (ClassNotFoundException e) {
			throw new ESDataImportException(FileLog2KafkaDataTranPlugin,e);
		} catch (InstantiationException e) {
			throw new ESDataImportException(FileLog2KafkaDataTranPlugin,e);
		} catch (InvocationTargetException e) {
			throw new ESDataImportException(FileLog2KafkaDataTranPlugin,e);
		} catch (NoSuchMethodException e) {
			throw new ESDataImportException(FileLog2KafkaDataTranPlugin,e);
		} catch (IllegalAccessException e) {
			throw new ESDataImportException(FileLog2KafkaDataTranPlugin,e);
		}
	}
	@Override
	protected ImportContext buildTargetImportContext(BaseImportConfig targetImportConfig) {
		KafkaOutputContextImpl kafkaOutputContext = new KafkaOutputContextImpl(targetImportConfig);
		kafkaOutputContext.init();
		return kafkaOutputContext;
	}
	@Override
	protected void setTargetImportContext(DataStream dataStream){
		if(kafkaOutputConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(kafkaOutputConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
	}


}
