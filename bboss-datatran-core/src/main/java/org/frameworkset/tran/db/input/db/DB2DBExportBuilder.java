package org.frameworkset.tran.db.input.db;
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBExportBuilder;
import org.frameworkset.tran.db.DBImportConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DBExportBuilder extends DBExportBuilder {

	private DBConfig targetDBConfig;




	public static DB2DBExportBuilder newInstance(){
		return new DB2DBExportBuilder();
	}
	public  DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new DB2DBDataTranPlugin(  importContext,  targetImportContext);
	}

	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}
	private void checkTargetDBConfig(){
		if(targetDBConfig == null)
			targetDBConfig = new DBConfig();
	}


	public DataStream builder(){
		super.builderConfig();

		try {
			if(logger.isInfoEnabled()) {
				logger.info("DB2DB Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		DBImportConfig db2DBImportConfig = new DBImportConfig();
		super.buildImportConfig(db2DBImportConfig);
		db2DBImportConfig.setUseJavaName(false);
		db2DBImportConfig.setTargetDBConfig(this.targetDBConfig);
		super.buildDBImportConfig(db2DBImportConfig);

		DataStream dataStream = this.createDataStream();
		dataStream.setImportConfig(db2DBImportConfig);
		dataStream.setImportContext(this.buildImportContext(db2DBImportConfig));
//		dataStream.setTargetImportContext(this.buildTargetImportContext(db2DBImportConfig));
		dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));
		return dataStream;
	}




	public DB2DBExportBuilder setTargetDbDriver(String targetDbDriver) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbDriver(targetDbDriver);
		return this;
	}

	public DB2DBExportBuilder setTargetDbUrl(String targetDbUrl) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbUrl(targetDbUrl);
		return this;
	}

	public DB2DBExportBuilder setTargetDbUser(String targetDbUser) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbUser(targetDbUser);
		return this;
	}

	public DB2DBExportBuilder setTargetDbPassword(String targetDbPassword) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbPassword(targetDbPassword);
		return this;
	}

	public DB2DBExportBuilder setTargetInitSize(int targetInitSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setInitSize(targetInitSize);
		return this;
	}

	public DB2DBExportBuilder setTargetMinIdleSize(int targetMinIdleSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setMinIdleSize(targetMinIdleSize);
		return this;
	}

	public DB2DBExportBuilder setTargetMaxSize(int targetMaxSize) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setMaxSize(targetMaxSize);
		return this;
	}

	public DB2DBExportBuilder setTargetDbName(String targetDbName) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbName(targetDbName);
		return this;
	}

	public DB2DBExportBuilder setTargetShowSql(boolean targetShowSql) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setShowSql(targetShowSql);
		return this;
	}

	public DB2DBExportBuilder setTargetUsePool(boolean targetUsePool) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setUsePool(targetUsePool);
		return this;
	}

	public DB2DBExportBuilder setTargetDbtype(String targetDbtype) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbtype(targetDbtype);
		return this;
	}

	public DB2DBExportBuilder setTargetDbAdaptor(String targetDbAdaptor) {
		this.checkTargetDBConfig();
		this.targetDBConfig.setDbAdaptor(targetDbAdaptor);
		return this;
	}




	public DB2DBExportBuilder setTargetValidateSQL(String validateSQL) {
		this.checkTargetDBConfig();
		targetDBConfig.setValidateSQL(validateSQL);
		return this;
	}

}
