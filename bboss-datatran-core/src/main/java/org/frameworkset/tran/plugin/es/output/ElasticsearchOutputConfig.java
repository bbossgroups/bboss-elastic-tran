package org.frameworkset.tran.plugin.es.output;
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
import com.frameworkset.orm.annotation.ESIndexWrapper;
import org.frameworkset.tran.DefaultEsIdGenerator;
import org.frameworkset.tran.EsIdGenerator;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.es.ESConfig;
import org.frameworkset.tran.plugin.es.input.ESExportResultHandler;
import org.frameworkset.tran.plugin.es.ESField;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public class ElasticsearchOutputConfig extends BaseConfig implements OutputConfig {
	private String index;
	/**抽取数据的sql语句*/
	private String indexType;
	public static EsIdGenerator DEFAULT_EsIdGenerator = new DefaultEsIdGenerator();
	private transient EsIdGenerator esIdGenerator = DEFAULT_EsIdGenerator;

	/**
	 * 是否不需要返回响应，不需要的情况下，可以设置为true，默认为true
	 * 提升性能，如果debugResponse设置为true，那么强制返回并打印响应到日志文件中
	 */
	private boolean discardBulkResponse = true;
	/**是否调试bulk响应日志，true启用，false 不启用，*/
	private boolean debugResponse;
	public boolean isDiscardBulkResponse() {
		return discardBulkResponse;
	}

	public ElasticsearchOutputConfig setDiscardBulkResponse(boolean discardBulkResponse) {
		this.discardBulkResponse = discardBulkResponse;
		return this;
	}

	public boolean isDebugResponse() {
		return debugResponse;
	}

	private void checkclientOptions(){
		if(clientOptions == null){
			clientOptions = new ClientOptions();
		}
	}


	private void copy(ClientOptions oldClientOptions,ClientOptions newClientOptions){
		if(oldClientOptions.getIdField() !=null )
			newClientOptions.setIdField(oldClientOptions.getIdField());
		if(oldClientOptions.getRefreshOption() != null)
			newClientOptions.setRefreshOption(oldClientOptions.getRefreshOption());
	}
	public ElasticsearchOutputConfig setClientOptions(ClientOptions clientOptions) {
		if(this.clientOptions != null){
			copy(this.clientOptions,clientOptions);
		}
		this.clientOptions = clientOptions;

		return this;
	}


	public ElasticsearchOutputConfig setEsParentIdValue(String esParentIdValue) {
		checkclientOptions();
		clientOptions.setEsParentIdValue(esParentIdValue);
//		this.esParentIdValue = esParentIdValue;
		return this;
	}



	public ElasticsearchOutputConfig setEsVersionValue(Object esVersionValue) {
		checkclientOptions();
		clientOptions.setVersion(esVersionValue);
//		this.esVersionValue = esVersionValue;
		return this;
	}



	public ElasticsearchOutputConfig setEsDetectNoop(Object esDetectNoop) {
		checkclientOptions();
		clientOptions.setDetectNoop(esDetectNoop);
		return this;
	}
	public ElasticsearchOutputConfig setDebugResponse(boolean debugResponse) {
		this.debugResponse = debugResponse;
		return this;
	}
	public ESIndexWrapper getEsIndexWrapper() {
		return esIndexWrapper;
	}

	public ElasticsearchOutputConfig setEsIndexWrapper(ESIndexWrapper esIndexWrapper) {
		this.esIndexWrapper = esIndexWrapper;
		return this;
	}

	private ESIndexWrapper esIndexWrapper;
	@JsonIgnore
	public EsIdGenerator getEsIdGenerator() {
		return esIdGenerator;
	}
	@JsonIgnore
	public ElasticsearchOutputConfig setEsIdGenerator(EsIdGenerator esIdGenerator) {
		if(esIdGenerator != null)
			this.esIdGenerator = esIdGenerator;
		return this;
	}
	private ESConfig esConfig;

	public ESConfig getEsConfig() {
		return esConfig;
	}

	private ClientOptions clientOptions;
	public ElasticsearchOutputConfig setTargetElasticsearch(String targetElasticsearch) {
		this.targetElasticsearch = targetElasticsearch;
		return this;
	}
	private String targetElasticsearch = "default";
	public String getTargetElasticsearch() {
		return targetElasticsearch;
	}


	public ElasticsearchOutputConfig setEsConfig(ESConfig esConfig) {
		this.esConfig = esConfig;
		return this;
	}
	public ClientOptions getClientOptions() {
		return clientOptions;
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
	public ElasticsearchOutputConfig addElasticsearchProperty(String name, String value){
		if(this.esConfig == null){
			esConfig = new ESConfig();
		}
		esConfig.addElasticsearchProperty(name,value);
		return this;
	}

	public ElasticsearchOutputConfig addTargetElasticsearch(String name,String targetElasticsearch) {
		this.targetElasticsearch = targetElasticsearch;
		return addElasticsearchProperty( name, targetElasticsearch);
	}

	@Override
	public void build(ImportBuilder importBuilder) {
//		if(esConfig != null){
//			ElasticSearchPropertiesFilePlugin.init(esConfig.getConfigs());
//		}
//		else if (importBuilder.getApplicationPropertiesFile() != null) {
//
//			ElasticSearchPropertiesFilePlugin.init(importBuilder.getApplicationPropertiesFile());
////					propertiesContainer.addConfigPropertiesFile(applicationPropertiesFile);
//		}
		if(index != null) {
			ESIndexWrapper esIndexWrapper = new ESIndexWrapper(index, indexType);
//			esIndexWrapper.setUseBatchContextIndexName(this.useBatchContextIndexName);
			setEsIndexWrapper(esIndexWrapper);
		}
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new ElasticsearchOutputDataTranPlugin(importContext);
	}

	public String getIndex() {
		return index;
	}

	public ElasticsearchOutputConfig setIndex(String index) {
		this.index = index;
		return this;
	}

	public String getIndexType() {
		return indexType;
	}

	public ElasticsearchOutputConfig setIndexType(String indexType) {
		this.indexType = indexType;
		return this;
	}
	@Override
	public WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		ESExportResultHandler db2ESExportResultHandler = new ESExportResultHandler(exportResultHandler);
		return db2ESExportResultHandler;
	}



	public ElasticsearchOutputConfig setTimeout(String timeout) {
		checkclientOptions();
		clientOptions.setTimeout(timeout);
		return this;
	}



	public ElasticsearchOutputConfig setMasterTimeout(String masterTimeout) {
		checkclientOptions();
		clientOptions.setMasterTimeout(masterTimeout);
		return this;
	}



	public ElasticsearchOutputConfig setWaitForActiveShards(Integer waitForActiveShards) {
		checkclientOptions();
		clientOptions.setWaitForActiveShards(waitForActiveShards);
		return this;
	}


	public ElasticsearchOutputConfig setSourceUpdateExcludes(List<String> sourceUpdateExcludes) {
		checkclientOptions();
		clientOptions.setSourceUpdateExcludes(sourceUpdateExcludes);
		return this;
	}

	public ElasticsearchOutputConfig setSourceUpdateIncludes(List<String> sourceUpdateIncludes) {
		checkclientOptions();
		clientOptions.setSourceUpdateIncludes(sourceUpdateIncludes);
		return this;
	}
	public ElasticsearchOutputConfig setRefreshOption(String refreshOption) {
		this.checkclientOptions();
		this.clientOptions.setRefreshOption(refreshOption);
		return this;
	}
	public ElasticsearchOutputConfig setEsVersionType(String esVersionType) {
		checkclientOptions();
		clientOptions.setVersionType(esVersionType);
		return this;
	}

	public ElasticsearchOutputConfig setEsVersionField(String esVersionField) {
		checkclientOptions();
		if (!esVersionField.startsWith("meta:"))
			clientOptions.setVersionField(new ESField(false,esVersionField));
		else{
			clientOptions.setVersionField(new ESField(true,esVersionField.substring(5)));

		}
		return this;
	}

	public ElasticsearchOutputConfig setEsReturnSource(Boolean esReturnSource) {
		checkclientOptions();
		clientOptions.setReturnSource(esReturnSource);
		return this;
	}

	public ElasticsearchOutputConfig setEsRetryOnConflict(Integer esRetryOnConflict) {
		checkclientOptions();
		clientOptions.setEsRetryOnConflict(esRetryOnConflict);
		return this;
	}

	public ElasticsearchOutputConfig setEsDocAsUpsert(Boolean esDocAsUpsert) {
		checkclientOptions();
		clientOptions.setDocasupsert(esDocAsUpsert);
		return this;
	}

	public ElasticsearchOutputConfig setRoutingValue(String routingValue) {
		checkclientOptions();
		clientOptions.setRouting(routingValue);
		return this;
	}

	/**
	 * 设置Elasticsearch批处理入库的filter_path参数
	 * https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
	 * @param filterPath
	 * @return
	 */
	public ElasticsearchOutputConfig setFilterPath(String filterPath) {
		checkclientOptions();
		clientOptions.setFilterPath(filterPath);
		return this;
	}

	public ElasticsearchOutputConfig setRoutingField(String routingField) {
		checkclientOptions();
		if (!routingField.startsWith("meta:"))
			clientOptions.setRoutingField(new ESField(false,routingField));
		else{
			clientOptions.setRoutingField(new ESField(true,routingField.substring(5)));

		}
		return this;
	}

	public ElasticsearchOutputConfig setEsParentIdField(String esParentIdField) {
		checkclientOptions();
		if (!esParentIdField.startsWith("meta:"))
			clientOptions.setParentIdField(new ESField(false,esParentIdField));
		else{
			clientOptions.setParentIdField(new ESField(true,esParentIdField.substring(5)));

		}
		return this;
	}

	public ElasticsearchOutputConfig setEsIdField(String esIdField) {
		checkclientOptions();
		if (!esIdField.startsWith("meta:"))
			clientOptions.setIdField(new ESField(false,esIdField));
		else{
			clientOptions.setIdField(new ESField(true,esIdField.substring(5)));

		}

		return this;
	}
}
