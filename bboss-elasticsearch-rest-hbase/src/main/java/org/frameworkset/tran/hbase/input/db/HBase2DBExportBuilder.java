package org.frameworkset.tran.hbase.input.db;
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
import org.frameworkset.tran.hbase.HBaseExportBuilder;

/**
 * <p>Description: hbase to database data tran plugin builder</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class HBase2DBExportBuilder extends HBaseExportBuilder {


	public HBase2DBExportBuilder setOutputDBConfig(DBImportConfig dboutputImportConfig) {
		this.outputDBConfig = dboutputImportConfig;
		return this;
	}

	@JsonIgnore
	private DBImportConfig outputDBConfig;
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new HBase2DBInputPlugin(  importContext,  targetImportContext);
	}
	@Override
	protected ImportContext buildTargetImportContext(BaseImportConfig targetImportConfig) {
		DBImportContext dbImportContext =  new DBImportContext(targetImportConfig);
		dbImportContext.init();
		return dbImportContext;
	}
	@Override
	protected void setTargetImportContext(DataStream dataStream){
		if(outputDBConfig != null)
			dataStream.setTargetImportContext(buildTargetImportContext(outputDBConfig) );
		else
			dataStream.setTargetImportContext(dataStream.getImportContext());
	}
	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler(exportResultHandler);
	}
}
