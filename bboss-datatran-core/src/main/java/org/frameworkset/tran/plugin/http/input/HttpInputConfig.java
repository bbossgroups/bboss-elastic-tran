package org.frameworkset.tran.plugin.http.input;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.http.BaseHttpConfig;
import org.frameworkset.tran.plugin.http.DynamicHeader;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpInputConfig extends BaseHttpConfig implements InputConfig {
	private Map<String,Object> httpConfigs;
	private String sourceHttpPool;

	private String queryDslName;
	private String queryDsl;

	private String queryUrl;

	private int pageSize;
	public final static String defaultPagineFromKey = "httpPagineFrom";
	public final static String defaultPagineSizeKey = "httpPagineSize";

	public String pagineFromKey = "httpPagineFrom";
	public String pagineSizeKey = "httpPagineSize";
	private HttpResultParser httpResultParser;
	public boolean isPagine() {
		return pagine;
	}

	public HttpInputConfig addDynamicHeader(String header, DynamicHeader dynamicHeader){
		_addDynamicHeader(header, dynamicHeader);
		return this;
	}
	public HttpResultParser getHttpResultParser() {
		return httpResultParser;
	}

	public HttpInputConfig setHttpResultParser(HttpResultParser httpResultParser) {
		this.httpResultParser = httpResultParser;
		return this;
	}



	public HttpInputConfig setPagine(boolean pagine) {
		this.pagine = pagine;
		return this;
	}

	/**
	 * 控制是否分页获取数据，需要对应的http服务提供支持，在数据量比较大，并且http服务支持分页查询是有效
	 * from:分页起始位置,从0开始
	 * size：每页数据记录数，如果实际返回的记录数小于size或者为0，则标识分页获取数据结束
	 */
	private boolean pagine;

	public Map<String, Object> getHttpConfigs() {
		return httpConfigs;
	}

	private void checkConfigs(){
		if(httpConfigs == null)
			httpConfigs = new LinkedHashMap<>();
	}
	public HttpInputConfig addSourceHttpPoolName(String nameProperty,String httpPoolName){
		checkConfigs();
		this.httpConfigs.put(nameProperty,httpPoolName);
		this.sourceHttpPool = httpPoolName;
		return this;
	}

	public HttpInputConfig addHttpInputConfig(String property,Object value){
		checkConfigs();
		this.httpConfigs.put(property,value);
		return this;
	}

	public String getQueryDsl() {
		return queryDsl;
	}

	public HttpInputConfig setQueryDsl(String queryDsl) {
		this.queryDsl = queryDsl;
		return this;
	}

	public String getSourceHttpPool() {
		return sourceHttpPool;
	}

	public HttpInputConfig setSourceHttpPool(String sourceHttpPool) {
		this.sourceHttpPool = sourceHttpPool;
		return this;
	}

	public String getQueryDslName() {
		return queryDslName;
	}

	public HttpInputConfig setQueryDslName(String queryDslName) {
		this.queryDslName = queryDslName;
		return this;
	}

	public HttpInputConfig setPagineFromKey(String pagineFromKey) {
		this.pagineFromKey = pagineFromKey;
		return this;
	}

	public HttpInputConfig setPagineSizeKey(String pagineSizeKey) {
		this.pagineSizeKey = pagineSizeKey;
		return this;
	}

	public String getPagineFromKey() {
		return pagineFromKey;
	}

	public String getPagineSizeKey() {
		return pagineSizeKey;
	}


	@Override
	public void build(ImportBuilder importBuilder) {
		if(SimpleStringUtil.isEmpty(this.getQueryDsl())){
			if(SimpleStringUtil.isEmpty(getDslFile()) || SimpleStringUtil.isEmpty(getQueryDslName()) ){
				throw new DataImportException("Input http query dsl is not setted.");
			}
		}
		else{
			if(SimpleStringUtil.isEmpty(queryDslName))
				queryDslName = "datatranQueryDslName";
			if(SimpleStringUtil.isEmpty(dslNamespace))
				dslNamespace = "datatranQueryDslNamespace"+SimpleStringUtil.getUUID();
			if(SimpleStringUtil.isEmpty(pagineFromKey))
				pagineFromKey = defaultPagineFromKey;
			if(SimpleStringUtil.isEmpty(pagineSizeKey))
				pagineSizeKey = defaultPagineSizeKey;

		}
		if(SimpleStringUtil.isEmpty(this.getQueryUrl())){

			throw new DataImportException("Input http query url is not setted.");
		}
		if(SimpleStringUtil.isEmpty(httpMethod)){
			httpMethod = "post";
		}

		if(!httpMethod.equals("post") && !httpMethod.equals("put") ){
			throw new DataImportException("Input httpMethod must be post or put.");
		}
		pageSize = importBuilder.getFetchSize() > 0? importBuilder.getFetchSize():5000;
	}

	public int getPageSize() {
		return pageSize;
	}

	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new HttpInputDataTranPlugin(importContext);
	}

	public String getQueryUrl() {
		return queryUrl;
	}

	public HttpInputConfig setQueryUrl(String queryUrl) {
		this.queryUrl = queryUrl;
		return this;
	}


	public HttpInputConfig setShowDsl(boolean showDsl) {
		this.showDsl = showDsl;
		return this;
	}

	public HttpInputConfig setDslFile(String dslFile) {
		this.dslFile = dslFile;
		return this;
	}

	public HttpInputConfig setDslNamespace(String dslNamespace) {
		this.dslNamespace = dslNamespace;
		return this;
	}

	public HttpInputConfig setHttpMethod(String httpMethod) {
		this.httpMethod = httpMethod;
		return this;
	}
	public HttpInputConfig addHttpHeaders(Map<String, String> _httpHeaders){
		_addHttpHeaders(_httpHeaders);
		return this;
	}
	public HttpInputConfig addHttpHeader(String header,String value){
		_addHttpHeader(header,value);
		return this;
	}

}
