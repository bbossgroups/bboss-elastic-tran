package org.frameworkset.tran.kafka.output.db;
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
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBExportBuilder;
import org.frameworkset.tran.db.DBImportConfig;
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
public class DB2KafkaExportBuilder extends DBExportBuilder {
	private static final String DB2KafkaDataTranPlugin = "org.frameworkset.tran.kafka.output.db.DB2KafkaDataTranPlugin";


	@JsonIgnore
	private KafkaOutputConfig kafkaOutputConfig;
	public KafkaOutputConfig getKafkaOutputConfig() {
		return kafkaOutputConfig;
	}

	public DB2KafkaExportBuilder setKafkaOutputConfig(KafkaOutputConfig kafkaOutputConfig) {
		this.kafkaOutputConfig = kafkaOutputConfig;
		return this;
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		try {
			Class<DataTranPlugin> clazz = (Class<DataTranPlugin>) Class.forName(DB2KafkaDataTranPlugin);
			return clazz.getConstructor(ImportContext.class,ImportContext.class).newInstance( importContext, targetImportContext);// ES2KafkaDataTranPlugin(this);
		} catch (ClassNotFoundException e) {
			throw new ESDataImportException(DB2KafkaDataTranPlugin,e);
		} catch (InstantiationException e) {
			throw new ESDataImportException(DB2KafkaDataTranPlugin,e);
		} catch (InvocationTargetException e) {
			throw new ESDataImportException(DB2KafkaDataTranPlugin,e);
		} catch (NoSuchMethodException e) {
			throw new ESDataImportException(DB2KafkaDataTranPlugin,e);
		} catch (IllegalAccessException e) {
			throw new ESDataImportException(DB2KafkaDataTranPlugin,e);
		}
	}

	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		KafkaOutputContextImpl esOutputContext = new KafkaOutputContextImpl(importConfig);
		esOutputContext.init();
		return esOutputContext;
	}
	@Override
	protected DataStream innerBuilder(){
		super.builderConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("DB2Kafka Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		DBImportConfig importConfig = new DBImportConfig();

		super.buildImportConfig(importConfig);

		super.buildDBImportConfig(importConfig);
		DataStream  dataStream = this.createDataStream();
		dataStream.setImportConfig(importConfig);
		dataStream.setConfigString(this.toString());
		dataStream.setImportContext(this.buildImportContext(importConfig));
		dataStream.setTargetImportContext(buildTargetImportContext(kafkaOutputConfig));
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));
		return dataStream;
	}


	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler(exportResultHandler);
	}




}
