package org.frameworkset.tran.db.input.dummy;
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
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBExportBuilder;
import org.frameworkset.tran.db.DBImportConfig;
import org.frameworkset.tran.es.ESExportResultHandler;
import org.frameworkset.tran.ouput.custom.CustomDummyUtil;
import org.frameworkset.tran.ouput.dummy.DummyOupputConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DummyExportBuilder extends DBExportBuilder {


	@JsonIgnore
	private DummyOupputConfig dummyOupputConfig;


	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new DB2DummyDataTranPlugin(  importContext,  targetImportContext);
	}





	@Override
	protected DataStream innerBuilder(){
		super.builderConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("DB2Log Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		DBImportConfig importConfig = new DBImportConfig();
//		esjdbc.setImportBuilder(this);
		super.buildImportConfig(importConfig);
//		esjdbcResultSet.setMetaData(statementInfo.getMeta());
//		esjdbcResultSet.setResultSet(resultSet);

		super.buildDBImportConfig(importConfig);
		DataStream  dataStream = this.createDataStream();
		dataStream.setImportConfig(importConfig);
		dataStream.setConfigString(this.toString());
		dataStream.setImportContext(this.buildImportContext(importConfig));
//		dataStream.setTargetImportContext(this.buildTargetImportContext(importConfig));
		CustomDummyUtil.setTargetImportContext(dummyOupputConfig,dataStream);
//		dataStream.init();
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));
		return dataStream;
	}





	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		ESExportResultHandler db2ESExportResultHandler = new ESExportResultHandler(exportResultHandler);
		return db2ESExportResultHandler;
	}



	public DummyOupputConfig getDummyOupputConfig() {
		return dummyOupputConfig;
	}

	public DB2DummyExportBuilder setDummyOupputConfig(DummyOupputConfig dummyOupputConfig) {
		this.dummyOupputConfig = dummyOupputConfig;
		return this;
	}

}
