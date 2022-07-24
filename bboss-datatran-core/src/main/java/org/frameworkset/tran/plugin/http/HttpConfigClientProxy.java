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

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.entity.ContentType;
import org.apache.http.util.EntityUtils;
import org.frameworkset.elasticsearch.client.ConfigHolder;
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.elasticsearch.template.ConfigDSLUtil;
import org.frameworkset.elasticsearch.template.ESTemplateHelper;
import org.frameworkset.spi.remote.http.BaseURLResponseHandler;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.ResponseUtil;
import org.frameworkset.spi.remote.http.proxy.HttpProxyRequestException;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.plugin.http.input.HttpInputConfig;
import org.frameworkset.tran.plugin.http.input.HttpInputDataTranPlugin;
import org.frameworkset.tran.plugin.http.input.HttpResultParserContext;
import org.frameworkset.tran.plugin.http.output.HttpOutputConfig;
import org.frameworkset.tran.plugin.http.output.HttpOutputDataTranPlugin;
import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.frameworkset.spi.remote.http.ResponseUtil.converJson2List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpConfigClientProxy {
	private static Logger logger = LoggerFactory.getLogger(HttpConfigClientProxy.class);
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
	public   <T> HttpResult<T>  putBodyForList(HttpInputDataTranPlugin httpInputDataTranPlugin, HttpResultParserContext httpResultParserContext, DynamicHeaderContext dynamicHeaderContext, Map params, final Class<T> resultType) throws HttpProxyRequestException {
		final HttpResult<T> httpResult = new HttpResult<>();
		HttpInputConfig httpInputConfig = httpInputDataTranPlugin.getHttpInputConfig();
		String dsl = this.evalTemplate(httpInputConfig.getQueryDslName(),params);
		if(logger.isInfoEnabled() && httpInputConfig.isShowDsl()){
			logger.info(dsl);
		}
		dynamicHeaderContext.setDatas(dsl);
		List<T> datas =  HttpRequestProxy.putBody(  httpInputConfig.getSourceHttpPool(),  dsl,   httpInputConfig.getQueryUrl(), HttpProxyHelper.getHttpHeaders(httpInputConfig,dynamicHeaderContext), ContentType.APPLICATION_JSON, new BaseURLResponseHandler<List<T>>() {
			@Override
			public List<T> handleResponse(final HttpResponse response)
					throws ClientProtocolException, IOException {
				httpResult.setResponse(response);
				handleListResponse(httpResult,httpInputConfig,   httpResultParserContext,url, response, resultType);
				return httpResult.getDatas();
			}
		});
		httpResult.setDatas(datas);
		return httpResult;
	}

	public static <T> void handleListResponse(HttpResult<T> httpResult, HttpInputConfig httpInputConfig, HttpResultParserContext httpResultParserContext, String url, HttpResponse response, Class<T> resultType)
			throws ClientProtocolException, IOException {
		int status = response.getStatusLine().getStatusCode();

		if (status >= 200 && status < 300) {
			if(httpInputConfig.getHttpResultParser() == null) {
				HttpEntity entity = response.getEntity();
				List<T> datas = entity != null ? converJson2List(  entity,  resultType) : null;
				httpResult.setDatas( datas);
			}
			else{
				try {
					httpInputConfig.getHttpResultParser().parserHttpResult(httpResult,httpResultParserContext);
				} catch (IOException e) {
					throw e;
				} catch (RuntimeException e) {
					throw e;
				}catch (Exception e) {
					throw new DataImportException("httpInputConfig.getHttpResultParser().parserHttpResult failed:",e);
				}
			}

		} else {
			HttpEntity entity = response.getEntity();
			if (entity != null ) {
				if (logger.isDebugEnabled()) {
					logger.debug(new StringBuilder().append("Request url:").append(url).append(",status:").append(status).toString());
				}
				throw new HttpProxyRequestException(new StringBuilder().append("Request url:").append(url).append(",error:").append(EntityUtils.toString(entity)).toString());
			}
			else
				throw new HttpProxyRequestException(new StringBuilder().append("Request url:").append(url).append(",Unexpected response status: ").append( status).toString());
		}
	}
	public String putJson(HttpOutputDataTranPlugin httpOutputDataTranPlugin , DynamicHeaderContext dynamicHeaderContext,
						  Map params) throws HttpProxyRequestException {
		HttpOutputConfig httpOutputConfig = httpOutputDataTranPlugin.getHttpOutputConfig();
		String requestBody = this.evalTemplate(httpOutputConfig.getDataDslName(),params);
		if(logger.isInfoEnabled() && httpOutputConfig.isShowDsl()){
			logger.info(requestBody);
		}


		return HttpRequestProxy. putJson(  httpOutputConfig.getTargetHttpPool(),  requestBody,   httpOutputConfig.getServiceUrl(), HttpProxyHelper.getHttpHeaders(httpOutputConfig,dynamicHeaderContext));

	}
	public String sendBody(		HttpOutputDataTranPlugin httpOutputDataTranPlugin, DynamicHeaderContext dynamicHeaderContext,

						   Map params
						  ) throws HttpProxyRequestException {
		HttpOutputConfig httpOutputConfig = httpOutputDataTranPlugin.getHttpOutputConfig();
		String requestBody = this.evalTemplate(httpOutputConfig.getDataDslName(),params);
		if(logger.isInfoEnabled() && httpOutputConfig.isShowDsl()){
			logger.info(requestBody);
		}

		return HttpRequestProxy. sendBody(  httpOutputConfig.getTargetHttpPool(),  requestBody,   httpOutputConfig.getServiceUrl(), HttpProxyHelper.getHttpHeaders(httpOutputConfig,dynamicHeaderContext),   ContentType.APPLICATION_JSON, new BaseURLResponseHandler<String>() {

			@Override
			public String handleResponse(final HttpResponse response)
					throws ClientProtocolException, IOException {
				return ResponseUtil.handleStringResponse(url, response);
			}

		});

	}
	public   <T> HttpResult<T>  sendBodyForList(HttpInputDataTranPlugin httpInputDataTranPlugin, HttpResultParserContext httpResultParserContext, DynamicHeaderContext dynamicHeaderContext,Map params, final Class<T> resultType) throws HttpProxyRequestException {
		final HttpResult<T> httpResult = new HttpResult<>();
		HttpInputConfig httpInputConfig = httpInputDataTranPlugin.getHttpInputConfig();
		String dsl = this.evalTemplate(httpInputConfig.getQueryDslName(),params);
		if(logger.isInfoEnabled() && httpInputConfig.isShowDsl()){
			logger.info(dsl);
		}
		dynamicHeaderContext.setDatas(dsl);
		List<T> datas =  HttpRequestProxy.sendBody(  httpInputConfig.getSourceHttpPool(),  dsl,   httpInputConfig.getQueryUrl(), HttpProxyHelper.getHttpHeaders(httpInputConfig,dynamicHeaderContext), ContentType.APPLICATION_JSON, new BaseURLResponseHandler<List<T>>() {
			@Override
			public List<T> handleResponse(final HttpResponse response)
					throws ClientProtocolException, IOException {
				httpResult.setResponse(response);
				handleListResponse(httpResult,httpInputConfig,   httpResultParserContext,url, response, resultType);
				return httpResult.getDatas();
			}
		});
//		httpResult.setDatas(datas);
		return httpResult;
	}
	public   <T> HttpResult<T>  sendBodyForList(String poolname, String url,String dslName,Map params,final Class<T> resultType) throws HttpProxyRequestException {
		final HttpResult<T> httpResult = new HttpResult<>();
		String dsl = this.evalTemplate(dslName,params);
		if(logger.isInfoEnabled()){
			logger.info(dsl);
		}
		List<T> datas =  HttpRequestProxy.sendBody(  poolname,  dsl,   url, (Map<String, String>)null, ContentType.APPLICATION_JSON, new BaseURLResponseHandler<List<T>>() {
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
