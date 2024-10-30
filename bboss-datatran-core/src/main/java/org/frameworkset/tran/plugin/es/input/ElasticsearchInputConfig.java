package org.frameworkset.tran.plugin.es.input;
/**
 * Copyright 2022 bboss
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
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.es.ESConfig;
import org.frameworkset.tran.plugin.es.ESField;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public class ElasticsearchInputConfig extends BaseConfig implements InputConfig {

	private String sourceElasticsearch = "default";
	private String scrollLiveTime = "100m";


	/**indexName/_search*/
	private String queryUrl;
	@JsonIgnore
	private QueryUrlFunction queryUrlFunction;
	private String dslFile;
	private String dslName;

	public ElasticsearchInputConfig setDslNamespace(String dslNamespace) {
		this.dslNamespace = dslNamespace;
		return this;
	}

	private String dslNamespace;
	private String dsl;
	private boolean sliceQuery;
	private int sliceSize;
	private ESConfig esConfig;

	private ClientOptions clientOptions;
	public String getSourceElasticsearch() {
		return sourceElasticsearch;
	}

	public ElasticsearchInputConfig setSourceElasticsearch(String sourceElasticsearch) {
		this.sourceElasticsearch = sourceElasticsearch;
		return this;
	}

	public ClientOptions getClientOptions() {
		return clientOptions;
	}

	public ElasticsearchInputConfig setClientOptions(ClientOptions clientOptions) {
		this.clientOptions = clientOptions;
		return this;
	}

	public ElasticsearchInputConfig setEsIdField(ESField esIdField) {
		if(this.clientOptions == null){
			clientOptions = new ClientOptions();
		}
		clientOptions.setIdField(esIdField);
		return this;
	}

	public ElasticsearchInputConfig setRefreshOption(String refreshOption) {
		if(this.clientOptions == null){
			clientOptions = new ClientOptions();
		}
		clientOptions.setRefreshOption(  refreshOption);
		return this;
	}





	public ElasticsearchInputConfig setEsConfig(ESConfig esConfig) {
		this.esConfig = esConfig;
		return this;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
//		if(esConfig != null){
//			ElasticSearchPropertiesFilePlugin.init(esConfig.getConfigs());
//		}
//		if (importBuilder.getApplicationPropertiesFile() != null) {
//
//			ElasticSearchPropertiesFilePlugin.init(importBuilder.getApplicationPropertiesFile());
////					propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
//		}
		if(SimpleStringUtil.isNotEmpty(dsl)) {
			if(SimpleStringUtil.isEmpty(dslName))
				dslName = "datatranDslName";
			if(SimpleStringUtil.isEmpty(dslNamespace))
				dslNamespace = "datatranDslName"+SimpleStringUtil.getUUID();

		}
	}

	public ESConfig getEsConfig() {
		return esConfig;
	}

	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new ElasticsearchInputDataTranPlugin(importContext);
	}

	public String getScrollLiveTime() {
		return scrollLiveTime;
	}

	public ElasticsearchInputConfig setScrollLiveTime(String scrollLiveTime) {
		this.scrollLiveTime = scrollLiveTime;
		return this;
	}

	public String getQueryUrl() {
		return queryUrl;
	}

	public ElasticsearchInputConfig setQueryUrl(String queryUrl) {
		this.queryUrl = queryUrl;
		return this;
	}

	public QueryUrlFunction getQueryUrlFunction() {
		return queryUrlFunction;
	}

	public ElasticsearchInputConfig setQueryUrlFunction(QueryUrlFunction queryUrlFunction) {
		this.queryUrlFunction = queryUrlFunction;
		return this;
	}

	public String getDslFile() {
		return dslFile;
	}

	public ElasticsearchInputConfig setDslFile(String dslFile) {
		this.dslFile = dslFile;
		return this;
	}

	public String getDslName() {
		return dslName;
	}

	public ElasticsearchInputConfig setDslName(String dslName) {
		this.dslName = dslName;
		return this;
	}

	public boolean isSliceQuery() {
		return sliceQuery;
	}

	public ElasticsearchInputConfig setSliceQuery(boolean sliceQuery) {
		this.sliceQuery = sliceQuery;
		return this;
	}

	public int getSliceSize() {
		return sliceSize;
	}

	public ElasticsearchInputConfig setSliceSize(int sliceSize) {
		this.sliceSize = sliceSize;
		return this;
	}






	public ElasticsearchInputConfig addSourceElasticsearch(String name,String sourceElasticsearch) {
		this.sourceElasticsearch = sourceElasticsearch;
		if(this.esConfig == null){
			esConfig = new ESConfig();
		}
		return addElasticsearchProperty( name, sourceElasticsearch);
	}

	/**
	 * 添加es客户端配置属性，具体的配置项参考文档：
	 * https://esdoc.bbossgroups.com/#/development?id=_2-elasticsearch%e9%85%8d%e7%bd%ae
	 *
	 * 如果在代码中指定配置项，就不会去加载application.properties中指定的数据源配置，如果没有配置则去加载applciation.properties中的对应数据源配置
	 * @param name
	 * @param value
	 * @return
	 */
	public ElasticsearchInputConfig addElasticsearchProperty(String name, String value){
		if(this.esConfig == null){
			esConfig = new ESConfig();
		}
		esConfig.addElasticsearchProperty(name,value);
		return this;
	}

	public String getDsl() {
		return dsl;
	}

	public ElasticsearchInputConfig setDsl(String dsl) {
		this.dsl = dsl;
		return this;
	}

	public String getDslNamespace() {
		return dslNamespace;
	}
}
