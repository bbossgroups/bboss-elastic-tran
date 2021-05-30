package org.frameworkset.tran.kafka.input.dummy;
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
import org.frameworkset.tran.ouput.dummy.DummyOupputConfig;
import org.frameworkset.tran.ouput.dummy.DummyOupputContextImpl;

import java.lang.reflect.InvocationTargetException;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2DummyExportBuilder extends KafkaExportBuilder {
	private static final String Kafka2DummyInputPlugin = "org.frameworkset.tran.kafka.input.dummy.Kafka2DummyInputPlugin";

	public Kafka2DummyExportBuilder setDummyOupputConfig(DummyOupputConfig dummyOupputConfig) {
		this.dummyOupputConfig = dummyOupputConfig;
		return this;
	}

	@JsonIgnore
	private DummyOupputConfig dummyOupputConfig;




	protected void setTargetImportContext(DataStream dataStream){

		if(dummyOupputConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(dummyOupputConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
	}



	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		DummyOupputContextImpl dummyOupputContext = new DummyOupputContextImpl((DummyOupputConfig) importConfig);
		dummyOupputContext.init();
		return dummyOupputContext;
	}



	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext)
	{

		try {
			Class<DataTranPlugin> clazz = (Class<DataTranPlugin>) Class.forName(Kafka2DummyInputPlugin);
			return clazz.getConstructor(ImportContext.class,ImportContext.class).newInstance( importContext, targetImportContext);// Kafka2ESInputPlugin(this);
		} catch (ClassNotFoundException e) {
			throw new ESDataImportException(Kafka2DummyInputPlugin,e);
		} catch (InstantiationException e) {
			throw new ESDataImportException(Kafka2DummyInputPlugin,e);
		} catch (InvocationTargetException e) {
			throw new ESDataImportException(Kafka2DummyInputPlugin,e);
		} catch (NoSuchMethodException e) {
			throw new ESDataImportException(Kafka2DummyInputPlugin,e);
		} catch (IllegalAccessException e) {
			throw new ESDataImportException(Kafka2DummyInputPlugin,e);
		}


	}



//	@Override
//	public DataStream builder() {
//
//
//		DataStream dataStream = super.builder();
////		super.buildImportConfig(dbImportConfig);
//		if(dbmportConfig != null)
//			dataStream.setTargetImportContext(buildTargetImportContext(dbmportConfig) );
//		else
//			dataStream.setTargetImportContext(dataStream.getImportContext());
//		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));
//
//		return dataStream;
//	}


}
