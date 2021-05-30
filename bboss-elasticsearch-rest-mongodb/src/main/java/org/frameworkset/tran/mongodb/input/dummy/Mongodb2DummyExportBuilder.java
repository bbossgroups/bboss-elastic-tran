package org.frameworkset.tran.mongodb.input.dummy;
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
import org.frameworkset.tran.mongodb.MongoDBExportBuilder;
import org.frameworkset.tran.ouput.dummy.DummyOupputConfig;
import org.frameworkset.tran.ouput.dummy.DummyOupputContextImpl;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class Mongodb2DummyExportBuilder  extends MongoDBExportBuilder {


	@JsonIgnore
	private DummyOupputConfig dummyOupputConfig;



	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<Object,Object>(exportResultHandler);
	}


	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		DummyOupputContextImpl dummyOupputContext = new DummyOupputContextImpl((DummyOupputConfig) importConfig);
		dummyOupputContext.init();
		return dummyOupputContext;
	}


	protected void setTargetImportContext(DataStream dataStream){
			dataStream.setTargetImportContext(buildTargetImportContext(dummyOupputConfig) );
	}

	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext, ImportContext targetImportContext){
		return new Mongodb2DummyDataTranPlugin(  importContext,  targetImportContext);
	}

	public DummyOupputConfig getDummyOupputConfig() {
		return dummyOupputConfig;
	}

	public Mongodb2DummyExportBuilder setDummyOupputConfig(DummyOupputConfig dummyOupputConfig) {
		this.dummyOupputConfig = dummyOupputConfig;
		return this;
	}

}
