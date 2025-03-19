package org.frameworkset.tran.config;
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

import org.frameworkset.elasticsearch.bulk.BulkActionConfig;
import org.frameworkset.tran.plugin.es.ESField;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: 传递es操作的相关控制参数，采用ClientOptions后，定义在对象中的相关注解字段将不会起作用（失效）
 * 可以在ClientOption中指定以下参数：
 * 	private String parentIdField;
 * 	private String idField;
 * 	private String esRetryOnConflictField;
 * 	private String versionField;
 * 	private String versionTypeField;
 * 	private String rountField;
 * 	private String refreshOption;
 * </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/31 23:48
 * @author biaoping.yin
 * @version 1.0
 */
public class ClientOptions implements BulkActionConfig,Serializable {
	public static final Object NULL_OBJECT_VALUE = new Object();
	public static final String NULL_STRING_VALUE = new String();

	public static final Integer NULL_INTEGER_VALUE = 0;
	public static final List<String> NULL_LIST_VALUE = new ArrayList<String>(0);
	public static final Long NULL_LONG_VALUE = 0l;

	public static final Boolean NULL_BOOLEAN_VALUE = true;
	public static final ESField NULL_ESFIELD_VALUE = new ESField("");
	private ESField esRetryOnConflictField;
	private ESField versionField;
	private ESField versionTypeField;
	private ESField routingField;

	private Object esRetryOnConflict;
	private Object version;
	private Object versionType;
	private Object routing;

	private String pipeline;
	private String filterPath;
	private String opType;
	protected String refreshOption;
	private ESField detectNoopField;
	private ESField docasupsertField;
	private Object detectNoop;
	private Object docasupsert;
	private Boolean returnSource;
	protected ESField parentIdField;
	protected Object esParentIdValue;
	private Long ifSeqNo;
	private Long ifPrimaryTerm;

	protected List<String> sourceUpdateExcludes;
	protected List<String> sourceUpdateIncludes;
	protected String timeout ;
	protected String masterTimeout ;
	protected Integer waitForActiveShards;
	/**
	 * Valid values: true, false, wait_for. Default: false
	 */
	protected String refresh;
	protected ESField idField;
	private ClientOptions parentClientOptions;
	public String getRefreshOption() {
		return refreshOption;
	}

	public ClientOptions setRefreshOption(String refreshOption) {
		this.refreshOption = refreshOption;
		return this;
	}

	public ClientOptions setEsParentIdValue(Object esParentIdValue) {
		this.esParentIdValue = esParentIdValue;
		return this;
	}

	public Object getEsParentIdValue() {
		if(esParentIdValue == NULL_OBJECT_VALUE)
			return null;
		if(esParentIdValue != null)
			return esParentIdValue;
		if(parentClientOptions != null){
			return parentClientOptions.getEsParentIdValue();
		}
		return null;
	}

	/**
	 * Valid values: true, false, wait_for. Default: false
	 * @param refresh
	 * @return
	 */
	public ClientOptions setRefresh(String refresh) {
		this.refresh = refresh;
		return this;
	}

	public String getRefresh() {

		if(refresh == NULL_STRING_VALUE)
			return null;
		if(refresh != null)
			return refresh;
		if(parentClientOptions != null){
			return parentClientOptions.getRefresh();
		}
		return null;

	}

	public ESField getDetectNoopField() {
		if(detectNoopField == NULL_ESFIELD_VALUE)
			return null;
		if(detectNoopField != null)
			return detectNoopField;
		if(parentClientOptions != null){
			return parentClientOptions.getDetectNoopField();
		}
		return null;
	}

	public ClientOptions setDetectNoopField(ESField detectNoopField) {
		this.detectNoopField = detectNoopField;
		return this;
	}

	public ESField getDocasupsertField() {
		if(docasupsertField == NULL_ESFIELD_VALUE)
			return null;
		if(docasupsertField != null)
			return docasupsertField;
		if(parentClientOptions != null){
			return parentClientOptions.getDocasupsertField();
		}
		return null;
	}

	public ClientOptions setDocasupsertField(ESField docasupsertField) {
		this.docasupsertField = docasupsertField;
		return this;
	}


	public Long getIfSeqNo() {
		if(ifSeqNo == NULL_LONG_VALUE)
			return null;
		if(ifSeqNo != null)
			return ifSeqNo;
		if(parentClientOptions != null){
			return parentClientOptions.getIfSeqNo();
		}
		return null;
	}

	public ClientOptions setIfSeqNo(Long ifSeqNo) {
		this.ifSeqNo = ifSeqNo;
		return this;
	}

	public Long getIfPrimaryTerm() {
		if(ifPrimaryTerm == NULL_LONG_VALUE)
			return null;
		if(ifPrimaryTerm != null)
			return ifPrimaryTerm;
		if(parentClientOptions != null){
			return parentClientOptions.getIfPrimaryTerm();
		}
		return null;
	}

	public ClientOptions setIfPrimaryTerm(Long ifPrimaryTerm) {
		this.ifPrimaryTerm = ifPrimaryTerm;
		return this;
	}
	public ESField getIdField() {
		if(idField == NULL_ESFIELD_VALUE)
			return null;
		if(idField != null)
			return idField;
		if(parentClientOptions != null){
			return parentClientOptions.getIdField();
		}
		return null;
	}

	public ClientOptions setIdField(ESField idField) {
		this.idField = idField;
		return this;
	}

	public ESField getParentIdField() {
		if(parentIdField == NULL_ESFIELD_VALUE)
			return null;
		if(parentIdField != null)
			return parentIdField;
		if(parentClientOptions != null){
			return parentClientOptions.getParentIdField();
		}
		return null;
	}

	public ClientOptions setParentIdField(ESField parentIdField) {
		this.parentIdField = parentIdField;
		return this;
	}

	public Object getDocasupsert() {
		if(docasupsert == NULL_OBJECT_VALUE)
			return null;
		if(docasupsert != null)
			return docasupsert;
		if(parentClientOptions != null){
			return parentClientOptions.getDocasupsert();
		}
		return null;
	}

	public ClientOptions setDocasupsert(Object docasupsert) {
		this.docasupsert = docasupsert;
		return this;
	}

	public Object getDetectNoop() {
		if(detectNoop == NULL_OBJECT_VALUE)
			return null;
		if(detectNoop != null)
			return detectNoop;
		if(parentClientOptions != null){
			return parentClientOptions.getDetectNoop();
		}
		return null;
	}

	public ClientOptions setDetectNoop(Object detectNoop) {
		this.detectNoop = detectNoop;
		return this;
	}

	public Boolean getReturnSource() {
		if(returnSource == NULL_BOOLEAN_VALUE)
			return null;
		if(returnSource != null)
			return returnSource;
		if(parentClientOptions != null){
			return parentClientOptions.getReturnSource();
		}
		return null;
	}

	public ClientOptions setReturnSource(Boolean returnSource) {
		this.returnSource = returnSource;
		return this;
	}


	public String getTimeout() {
		if(timeout == NULL_STRING_VALUE)
			return null;
		if(timeout != null)
			return timeout;
		if(parentClientOptions != null){
			return parentClientOptions.getTimeout();
		}
		return null;
	}

	public ClientOptions setTimeout(String timeout) {
		this.timeout = timeout;
		return this;
	}

	public String getMasterTimeout() {
		if(masterTimeout == NULL_STRING_VALUE)
			return null;
		if(masterTimeout != null)
			return masterTimeout;
		if(parentClientOptions != null){
			return parentClientOptions.getMasterTimeout();
		}
		return null;
	}

	public ClientOptions setMasterTimeout(String masterTimeout) {
		this.masterTimeout = masterTimeout;
		return this;
	}

	public Integer getWaitForActiveShards() {
		if(waitForActiveShards == NULL_INTEGER_VALUE)
			return null;
		if(waitForActiveShards != null)
			return waitForActiveShards;
		if(parentClientOptions != null){
			return parentClientOptions.getWaitForActiveShards();
		}
		return null;
	}

	public ClientOptions setWaitForActiveShards(Integer waitForActiveShards) {
		this.waitForActiveShards = waitForActiveShards;
		return this;
	}

	public List<String> getSourceUpdateExcludes() {
		if(sourceUpdateExcludes == NULL_LIST_VALUE)
			return null;
		if(sourceUpdateExcludes != null)
			return sourceUpdateExcludes;
		if(parentClientOptions != null){
			return parentClientOptions.getSourceUpdateExcludes();
		}
		return null;
	}

	public List<String> getSourceUpdateIncludes() {

		if(sourceUpdateIncludes == NULL_LIST_VALUE)
			return null;
		if(sourceUpdateIncludes != null)
			return sourceUpdateIncludes;
		if(parentClientOptions != null){
			return parentClientOptions.getSourceUpdateIncludes();
		}
		return null;
	}

	public ClientOptions setSourceUpdateExcludes(List<String> sourceUpdateExcludes) {
		this.sourceUpdateExcludes = sourceUpdateExcludes;
		return this;
	}

	public ClientOptions setSourceUpdateIncludes(List<String> sourceUpdateIncludes) {
		this.sourceUpdateIncludes = sourceUpdateIncludes;
		return this;
	}
	public ESField getRoutingField() {
		if(routingField == NULL_ESFIELD_VALUE)
			return null;
		if(routingField != null)
			return routingField;
		if(parentClientOptions != null){
			return parentClientOptions.getRoutingField();
		}
		return null;
	}

	public ClientOptions setRoutingField(ESField routingField) {
		this.routingField = routingField;
		return this;
	}



	public ESField getEsRetryOnConflictField() {
		if(esRetryOnConflictField == NULL_ESFIELD_VALUE)
			return null;
		if(esRetryOnConflictField != null)
			return esRetryOnConflictField;
		if(parentClientOptions != null){
			return parentClientOptions.getEsRetryOnConflictField();
		}
		return null;
	}

	public ClientOptions setEsRetryOnConflictField(ESField esRetryOnConflictField) {
		this.esRetryOnConflictField = esRetryOnConflictField;
		return this;
	}

	public ESField getVersionField() {
		if(versionField == NULL_ESFIELD_VALUE)
			return null;
		if(versionField != null)
			return versionField;
		if(parentClientOptions != null){
			return parentClientOptions.getVersionField();
		}
		return null;
	}

	public ClientOptions setVersionField(ESField versionField) {
		this.versionField = versionField;
		return this;
	}

	public ESField getVersionTypeField() {
		if(versionTypeField == NULL_ESFIELD_VALUE)
			return null;
		if(versionTypeField != null)
			return versionTypeField;
		if(parentClientOptions != null){
			return parentClientOptions.getVersionTypeField();
		}
		return null;
	}

	public ClientOptions setVersionTypeField(ESField versionTypeField) {
		this.versionTypeField = versionTypeField;
		return this;
	}

	public Object getEsRetryOnConflict() {
		if(esRetryOnConflict == NULL_OBJECT_VALUE)
			return null;
		if(esRetryOnConflict != null)
			return esRetryOnConflict;
		if(parentClientOptions != null){
			return parentClientOptions.getEsRetryOnConflict();
		}
		return null;
	}

	public ClientOptions setEsRetryOnConflict(Object esRetryOnConflict) {
		this.esRetryOnConflict = esRetryOnConflict;
		return this;
	}

	public Object getVersion() {
		if(version == NULL_OBJECT_VALUE)
			return null;
		if(version != null)
			return version;
		if(parentClientOptions != null){
			return parentClientOptions.getVersion();
		}
		return null;
	}

	public ClientOptions setVersion(Object version) {
		this.version = version;
		return this;
	}

	public Object getVersionType() {
		if(versionType == NULL_OBJECT_VALUE)
			return null;
		if(versionType != null)
			return versionType;
		if(parentClientOptions != null){
			return parentClientOptions.getVersionType();
		}
		return null;
	}

	public ClientOptions setVersionType(Object versionType) {
		this.versionType = versionType;
		return this;
	}

	public Object getRouting() {
		if(routing == NULL_OBJECT_VALUE)
			return null;
		if(routing != null)
			return routing;
		if(parentClientOptions != null){
			return parentClientOptions.getRouting();
		}
		return null;
	}

	public ClientOptions setRouting(Object routing) {
		this.routing = routing;
		return this;
	}


	public String getPipeline() {
		if(pipeline == NULL_STRING_VALUE)
			return null;
		if(pipeline != null)
			return pipeline;
		if(parentClientOptions != null){
			return parentClientOptions.getPipeline();
		}
		return null;
	}

	@Override
	public String getFilterPath() {
		return this.filterPath;
	}

	public void setFilterPath(String filterPath) {
		this.filterPath = filterPath;
	}

	/**
	 * (Optional, string) ID of the pipeline to use to preprocess incoming documents.
	 * @param pipeline
	 * @return
	 */
	public ClientOptions setPipeline(String pipeline) {
		this.pipeline = pipeline;
		return this;
	}

	public String getOpType() {
		if(opType == NULL_STRING_VALUE)
			return null;
		if(opType != null)
			return opType;
		if(parentClientOptions != null){
			return parentClientOptions.getOpType();
		}
		return null;
	}

	/**
	 * op_type
	 * (Optional, enum) Set to create to only index the document if it does not already exist (put if absent). If a document with the specified _id already exists, the indexing operation will fail. Same as using the <index>/_create endpoint. Valid values: index, create. If document id is specified, it defaults to index. Otherwise, it defaults to create.
	 * Valid values: index, create
	 * @param opType
	 * @return
	 */
	public ClientOptions setOpType(String opType) {
		this.opType = opType;
		return this;
	}

	public ClientOptions getParentClientOptions() {
		return parentClientOptions;
	}

	public void setParentClientOptions(ClientOptions parentClientOptions) {
		this.parentClientOptions = parentClientOptions;
	}
}
