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

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.entity.ContentType;
import org.frameworkset.elasticsearch.client.ConfigHolder;
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.elasticsearch.template.ConfigDSLUtil;
import org.frameworkset.elasticsearch.template.ESTemplateHelper;
import org.frameworkset.spi.remote.http.BaseURLResponseHandler;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.ResponseUtil;
import org.frameworkset.spi.remote.http.proxy.HttpProxyRequestException;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpConfigClientProxy {
	private  Logger logger = LoggerFactory.getLogger(HttpConfigClientProxy.class);
	protected String configFile;
	protected ConfigDSLUtil configDSLUtil;
	private  static ConfigHolder configHolder = new ConfigHolder("HttpProxy");
	static
	{
		ShutdownUtil.addShutdownHook(new Runnable(){

			public void run() {
				configHolder.stopmonitor();
				configHolder.destory();

			}});
	}
	public HttpConfigClientProxy(String configFile){
		this.configFile = configFile;
		configDSLUtil = configHolder.getConfigDSLUtil(configFile);
	}
	public HttpConfigClientProxy(BaseTemplateContainerImpl templateContainer){
		templateContainer.setConfigHolder(configHolder);
		configDSLUtil = configHolder.getConfigDSLUtil(templateContainer);
	}
	protected String evalTemplate(String templateName, Object params){
		return ESTemplateHelper.evalTemplate(configDSLUtil,templateName, params);
	}


	public  String httpGetforString(String url) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforString(url);
	}
	public  <T> T httpGetforObject(String url,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforObject(url,resultType);
	}
	public  String httpGetforString(String poolname, String url) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforString(poolname, url);
	}

	public  <T> T httpGetforObject(String poolname, String url,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforObject(poolname, url, resultType) ;
	}

	public  <T> List<T> httpGetforList(String url, final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforList(  url,  resultType);
	}

	public  <K,T> Map<K,T> httpGetforMap(String url,final Class<K> keyType,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforMap(  url,  keyType,  resultType);
	}

	public  <T> Set<T> httpGetforSet(String url, final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforSet(url, resultType);
	}

	public  <T> List<T> httpGetforList(String poolName,String url,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforList(  poolName,  url,  resultType);
	}
	public  <D,T> D httpGetforTypeObject(String url,final Class<D> containType,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforTypeObject(  url, containType, resultType);
	}
	public  <D,T> D httpGetforTypeObject(String poolName,String url,final Class<D> containType,final Class<T> resultType) throws HttpProxyRequestException {
		return HttpRequestProxy.httpGetforTypeObject(  poolName,  url,  containType, resultType);
	}

	public   <T> HttpResult<T>  sendBodyForList(String poolname, String url,String dslName,Map params,final Class<T> resultType) throws HttpProxyRequestException {
		final HttpResult<T> httpResult = new HttpResult<>();
		List<T> datas =  HttpRequestProxy.sendBody(  poolname,  this.evalTemplate(dslName,params),   url, (Map<String, String>)null, ContentType.APPLICATION_JSON, new BaseURLResponseHandler<List<T>>() {
			@Override
			public List<T> handleResponse(final HttpResponse response)
					throws ClientProtocolException, IOException {
				httpResult.setResponse(response);
				return ResponseUtil.handleListResponse( url, response, resultType);
			}
		});
		httpResult.setDatas(datas);
		return httpResult;
	}

	public   <T> T sendBody(final String poolname,  String requestBody, String url,
								 final Map<String, String> headers, ContentType contentType,
								 final ResponseHandler<T> responseHandler) throws HttpProxyRequestException {
		return HttpRequestProxy.sendBody( poolname,   requestBody,   url,
		  headers,   contentType,
		  responseHandler);

	}

}
