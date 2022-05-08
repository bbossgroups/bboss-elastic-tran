package org.frameworkset.tran.db.input.es;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBExportBuilder;
import org.frameworkset.tran.db.DBImportConfig;
import org.frameworkset.tran.es.ESExportResultHandler;
import org.frameworkset.tran.es.output.ESOutputConfig;
import org.frameworkset.tran.es.output.ESOutputContextImpl;

public class DB2ESImportBuilder extends DBExportBuilder {

	@JsonIgnore
	private ESOutputConfig esOutputConfig;
	protected DB2ESImportBuilder(){

	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new DBDataTranPlugin(  importContext,  targetImportContext);
	}











	public static DB2ESImportBuilder newInstance(){
		return new DB2ESImportBuilder();
	}

	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		ESOutputContextImpl esOutputContext = new ESOutputContextImpl(importConfig);
		esOutputContext.init();

		return esOutputContext;
	}
	public ESOutputConfig getEsOutputConfig() {
		return esOutputConfig;
	}

	public void setEsOutputConfig(ESOutputConfig esOutputConfig) {
		this.esOutputConfig = esOutputConfig;
	}


	@Override
	protected DataStream innerBuilder(){
		super.builderConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("DB2ES Import Configs:");
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
		if(esOutputConfig != null) {
			if(esOutputConfig.getTargetIndex() != null) {
				ESIndexWrapper esIndexWrapper = new ESIndexWrapper(esOutputConfig.getTargetIndex(), esOutputConfig.getTargetIndexType());
//			esIndexWrapper.setUseBatchContextIndexName(this.useBatchContextIndexName);
				esOutputConfig.setEsIndexWrapper(esIndexWrapper);
			}
			dataStream.setTargetImportContext(buildTargetImportContext(esOutputConfig));
		}
		else {
			dataStream.setTargetImportContext(dataStream.getImportContext());
		}
//		dataStream.init();
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));
		return dataStream;
	}





	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		ESExportResultHandler db2ESExportResultHandler = new ESExportResultHandler(exportResultHandler);
		return db2ESExportResultHandler;
	}




}
