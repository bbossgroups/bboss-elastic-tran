package org.frameworkset.tran.kafka.output.es;
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
import org.frameworkset.tran.es.input.ESExportBuilder;
import org.frameworkset.tran.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.kafka.output.KafkaOutputContextImpl;

import java.lang.reflect.InvocationTargetException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2KafkaExportBuilder extends ESExportBuilder {
	private static final String ES2KafkaDataTranPlugin = "org.frameworkset.tran.kafka.output.es.ES2KafkaDataTranPlugin";
	public KafkaOutputConfig getKafkaOutputConfig() {
		return kafkaOutputConfig;
	}

	public void setKafkaOutputConfig(KafkaOutputConfig kafkaOutputConfig) {
		this.kafkaOutputConfig = kafkaOutputConfig;
	}

	@JsonIgnore
	private KafkaOutputConfig kafkaOutputConfig;
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		try {
			Class<DataTranPlugin> clazz = (Class<DataTranPlugin>) Class.forName(ES2KafkaDataTranPlugin);
			return clazz.getConstructor(ImportContext.class,ImportContext.class).newInstance( importContext, targetImportContext);// ES2KafkaDataTranPlugin(this);
		} catch (ClassNotFoundException e) {
			throw new ESDataImportException(ES2KafkaDataTranPlugin,e);
		} catch (InstantiationException e) {
			throw new ESDataImportException(ES2KafkaDataTranPlugin,e);
		} catch (InvocationTargetException e) {
			throw new ESDataImportException(ES2KafkaDataTranPlugin,e);
		} catch (NoSuchMethodException e) {
			throw new ESDataImportException(ES2KafkaDataTranPlugin,e);
		} catch (IllegalAccessException e) {
			throw new ESDataImportException(ES2KafkaDataTranPlugin,e);
		}
	}
	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		KafkaOutputContextImpl kafkaOutputContext = new KafkaOutputContextImpl(importConfig);

		kafkaOutputContext.init();
		return kafkaOutputContext;
	}
	public DataStream builder(){
		DataStream dataStream = super.builder();

		try {
			if(logger.isInfoEnabled()) {
				logger.info("ES2KAFKA Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}


		if(kafkaOutputConfig != null) {

			dataStream.setTargetImportContext(buildTargetImportContext(kafkaOutputConfig));
		}
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;

	}



}
