package org.frameworkset.tran.es.input.es;
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
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class ES2ESExportBuilder extends BaseImportBuilder {

	private String scrollLiveTime = "100m";

	private Map params;
	/**indexName/_search*/
	private String queryUrl;
	private String dsl2ndSqlFile;
	private String dslName;
	private boolean sliceQuery;
	private int sliceSize;


	public ES2ESExportBuilder setTargetElasticsearch(String targetElasticsearch) {
		super.setTargetElasticsearch( targetElasticsearch) ;
		return this;
	}


	private String targetIndex;
	private String targetIndexType;



	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new DefualtExportResultHandler<String,String>(exportResultHandler);
	}
	protected ImportContext buildImportContext(BaseImportConfig importConfig){
		return new ES2ESImportContext(importConfig);
	}

	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new ES2ESDataTranPlugin(  importContext,  targetImportContext);
	}
	public DataStream builder(){
		super.builderConfig();
//		this.buildDBConfig();
//		this.buildStatusDBConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("ES2ES Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		ES2ESImportConfig es2DBImportConfig = new ES2ESImportConfig();
		super.buildImportConfig(es2DBImportConfig);
		es2DBImportConfig.setDsl2ndSqlFile(this.dsl2ndSqlFile);
//		es2DBImportConfig.setSqlFilepath(dsl2ndSqlFile);
//		es2DBImportConfig.setSqlName(sqlName);
//		es2DBImportConfig.setSql(this.sql);

		es2DBImportConfig.setQueryUrl(this.queryUrl);
		es2DBImportConfig.setScrollLiveTime(this.scrollLiveTime);


		es2DBImportConfig.setDslName(this.dslName);
		es2DBImportConfig.setSliceQuery(this.sliceQuery);
		es2DBImportConfig.setSliceSize(this.sliceSize);
		es2DBImportConfig.setParams(this.params);

		es2DBImportConfig.setTargetIndex(this.targetIndex);
		es2DBImportConfig.setTargetIndexType(this.targetIndexType);
		DataStream dataStream = createDataStream();
		dataStream.setImportConfig(es2DBImportConfig);
		dataStream.setImportContext(buildImportContext(es2DBImportConfig));
		dataStream.setTargetImportContext(dataStream.getImportContext());
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}




	public String getQueryUrl() {
		return queryUrl;
	}

	public ES2ESExportBuilder setQueryUrl(String queryUrl) {
		this.queryUrl = queryUrl;
		return this;
	}

	public String getDsl2ndSqlFile() {
		return dsl2ndSqlFile;
	}

	public ES2ESExportBuilder setDsl2ndSqlFile(String dsl2ndSqlFile) {
		this.dsl2ndSqlFile = dsl2ndSqlFile;
		return this;
	}

	public String getDslName() {
		return dslName;
	}

	public ES2ESExportBuilder setDslName(String dslName) {
		this.dslName = dslName;
		return this;
	}

	public String getScrollLiveTime() {
		return scrollLiveTime;
	}

	public ES2ESExportBuilder setScrollLiveTime(String scrollLiveTime) {
		this.scrollLiveTime = scrollLiveTime;
		return this;
	}

	public boolean isSliceQuery() {
		return sliceQuery;
	}

	public ES2ESExportBuilder setSliceQuery(boolean sliceQuery) {
		this.sliceQuery = sliceQuery;
		return this;
	}

	public int getSliceSize() {
		return sliceSize;
	}

	public ES2ESExportBuilder setSliceSize(int sliceSize) {
		this.sliceSize = sliceSize;
		return this;
	}
	public ES2ESExportBuilder addParam(String key, Object value){
		if(params == null)
			params = new HashMap();
		this.params.put(key,value);
		return this;
	}


	public String getTargetIndex() {
		return targetIndex;
	}

	public ES2ESExportBuilder setTargetIndex(String targetIndex) {
		this.targetIndex = targetIndex;
		return this;
	}

	public String getTargetIndexType() {
		return targetIndexType;
	}

	public ES2ESExportBuilder setTargetIndexType(String targetIndexType) {
		this.targetIndexType = targetIndexType;
		return this;
	}
}
