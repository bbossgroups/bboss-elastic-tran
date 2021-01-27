package org.frameworkset.tran.es.input.db;
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
import org.frameworkset.tran.db.DBImportConfig;
import org.frameworkset.tran.db.DBImportContext;
import org.frameworkset.tran.es.input.ESExportBuilder;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2DBExportBuilder extends ESExportBuilder {
	@JsonIgnore
	private DBImportConfig dbmportConfig;
	public void setOutputDBConfig(DBImportConfig dbmportConfig) {
		this.dbmportConfig = dbmportConfig;
	}

	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new ES2DBDataTranPlugin(  importContext,  targetImportContext);
	}


	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		return new DBImportContext(importConfig);
	}
	public DataStream builder(){
		DataStream dataStream = super.builder();
//		this.buildDBConfig();
//		this.buildStatusDBConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("ES2DB Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}


		if(dbmportConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(dbmportConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}


}
