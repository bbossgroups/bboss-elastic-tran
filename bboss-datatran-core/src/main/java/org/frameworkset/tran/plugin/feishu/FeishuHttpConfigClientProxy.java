package org.frameworkset.tran.plugin.feishu;
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


import org.frameworkset.spi.remote.http.HttpConfigInf;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.spi.remote.http.proxy.HttpProxyRequestException;
import org.frameworkset.spi.remote.http.template.BaseDslTemplateContainerImpl;
import org.frameworkset.spi.remote.http.template.ConfigDSLUtil;
import org.frameworkset.spi.remote.http.template.ConfigHolder;
import org.frameworkset.spi.remote.http.template.DslTemplateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class FeishuHttpConfigClientProxy {
	private static Logger logger = LoggerFactory.getLogger(FeishuHttpConfigClientProxy.class);
	protected String configFile;
	protected ConfigDSLUtil configDSLUtil;
	private ConfigHolder configHolder = null;//new ConfigHolder("HttpProxy");
//	static
//	{
//		ShutdownUtil.addShutdownHook(new Runnable(){
//
//			public void run() {
//				configHolder.stopmonitor();
//				configHolder.destory();
//
//			}});
//	}
	public FeishuHttpConfigClientProxy(ConfigHolder configHolder, String configFile){
        this.configHolder = configHolder;
		this.configFile = configFile;
		configDSLUtil = configHolder.getConfigDSLUtil(configFile);
	}
	public FeishuHttpConfigClientProxy(ConfigHolder configHolder, BaseDslTemplateContainerImpl templateContainer){
		templateContainer.setConfigHolder(configHolder);
		configDSLUtil = configHolder.getConfigDSLUtil(templateContainer);
	}
	protected String evalTemplate(String templateName, Object params){
		return DslTemplateHelper.evalTemplate(configDSLUtil,templateName, params);
	}

 

    public   <T> T  sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
                                      Map headers,
                                         Class<T> resultType) throws HttpProxyRequestException {
        String dsl = this.evalTemplate(queryDslName,params);
        if(logger.isInfoEnabled() && httpConfigInf.isShowDsl()){
            logger.info(dsl);
        }
        
         
        T datas =  HttpRequestProxy.sendJsonBody(  httpConfigInf.getDatasource(),  dsl,
                searchUrl, headers, resultType);
//		httpResult.setDatas(datas);
        return datas;
    }
 

}
