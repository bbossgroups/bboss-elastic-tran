package org.frameworkset.tran.mongodb.input.fileftp;/*
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
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.DefualtExportResultHandler;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.MongoDBExportBuilder;
import org.frameworkset.tran.output.fileftp.FileFtpOupputConfig;
import org.frameworkset.tran.output.fileftp.FileFtpOupputContextImpl;

public class Mongodb2FileFtpImportBuilder   extends MongoDBExportBuilder {

	@JsonIgnore
	private FileFtpOupputConfig fileFtpOupputConfig;
	public Mongodb2FileFtpImportBuilder(){

	}
	public Mongodb2FileFtpImportBuilder setFileFtpOupputConfig(FileFtpOupputConfig fileFtpOupputConfig) {
		this.fileFtpOupputConfig = fileFtpOupputConfig;
		return this;
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new Mongodb2FileFtpDataTranPlugin(  importContext,  targetImportContext);
	}


	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<Object,Object>(exportResultHandler);
	}



	protected ImportContext buildTargetImportContext(BaseImportConfig importConfig){
		FileFtpOupputContextImpl fileFtpOupputContext = new FileFtpOupputContextImpl(fileFtpOupputConfig);
		fileFtpOupputContext.init();
		return fileFtpOupputContext;
	}









}
