package org.frameworkset.tran.plugin.http;
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

import org.frameworkset.tran.plugin.BaseConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/23
 * @author biaoping.yin
 * @version 1.0
 */
public class BaseHttpConfig extends BaseConfig {
	protected boolean showDsl;
	protected String dslNamespace;
	protected String dslFile;
	protected Map<String,DynamicHeader> dynamicHeaders;
	protected String httpMethod;
	protected boolean postMethod;
    protected boolean getMethod;
    protected boolean putMethod;

	public Map<String, DynamicHeader> getDynamicHeaders() {
		return dynamicHeaders;
	}

    public boolean isGetMethod() {
        return getMethod;
    }

    public boolean isPutMethod() {
        return putMethod;
    }

    protected void _addDynamicHeader(String header, DynamicHeader dynamicHeader){
		if(dynamicHeaders == null){
			dynamicHeaders = new LinkedHashMap<>();
		}
		dynamicHeaders.put(header,dynamicHeader);
	}

	public String getDslFile() {
		return dslFile;
	}
	public String getDslNamespace() {
		return dslNamespace;
	}

	public boolean isPostMethod(){
		return postMethod;
	}

	public String getHttpMethod() {
		return httpMethod;
	}
	public boolean isShowDsl() {
		return showDsl;
	}
	protected Map<String,String> httpHeaders;
	protected void _addHttpHeader(String header, String value){
		if (httpHeaders == null) {
			httpHeaders = new LinkedHashMap<>();

		}
		httpHeaders.put(header,value);
	}
	protected void _addHttpHeaders(Map<String, String> _httpHeaders){
		if (httpHeaders == null) {
			httpHeaders = new LinkedHashMap<>();

		}
		httpHeaders.putAll(_httpHeaders);
	}

	public Map<String, String> getHttpHeaders() {
		return httpHeaders;
	}
}
