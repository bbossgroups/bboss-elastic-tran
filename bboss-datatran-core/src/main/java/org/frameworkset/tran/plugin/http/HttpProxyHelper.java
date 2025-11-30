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

import org.frameworkset.elasticsearch.client.ConfigHolder;
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.tran.DataImportException;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/1
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpProxyHelper {
	private Map<String, HttpConfigClientProxy> configDSLUtils = new ConcurrentHashMap<>();
    
    private ConfigHolder configHolder = new ConfigHolder("HttpProxy");
	public HttpConfigClientProxy getHttpConfigClientProxy( String configDSLFile){
		HttpConfigClientProxy httpConfigClientProxy = configDSLUtils.get(configDSLFile);
		if(httpConfigClientProxy != null)
			return httpConfigClientProxy;
		synchronized (configDSLUtils){
			httpConfigClientProxy = configDSLUtils.get(configDSLFile);
			if(httpConfigClientProxy != null)
				return httpConfigClientProxy;
			// TODO Auto-generated method stub
			httpConfigClientProxy =  new HttpConfigClientProxy(configHolder,configDSLFile);
			configDSLUtils.put(configDSLFile,httpConfigClientProxy);
		}
		return httpConfigClientProxy;
	}

	public HttpConfigClientProxy getHttpConfigClientProxy(BaseTemplateContainerImpl templateContainer){
		String namespace = templateContainer.getNamespace();
		HttpConfigClientProxy httpConfigClientProxy = configDSLUtils.get(namespace);
		if(httpConfigClientProxy != null)
			return httpConfigClientProxy;
		synchronized (configDSLUtils){
			httpConfigClientProxy = configDSLUtils.get(namespace);
			if(httpConfigClientProxy != null)
				return httpConfigClientProxy;
			// TODO Auto-generated method stub
			httpConfigClientProxy =  new HttpConfigClientProxy(configHolder,templateContainer);
			configDSLUtils.put(namespace,httpConfigClientProxy);
		}
		return httpConfigClientProxy;
	}


	public static Map<String,String> getHttpHeaders(BaseHttpConfig basicHttpConfig,DynamicHeaderContext dynamicHeaderContext) throws DataImportException {
		Map<String,String> httpHeaders = basicHttpConfig.getHttpHeaders();
		Map<String, DynamicHeader> dynamicHeaders = basicHttpConfig.getDynamicHeaders();
		if(dynamicHeaders == null || dynamicHeaders.size() == 0){
			return httpHeaders;
		}
		else{
			Map<String,String> ret = new LinkedHashMap<>();
			if(httpHeaders != null && httpHeaders.size() > 0){
				ret.putAll(httpHeaders);
			}
			Iterator<Map.Entry<String, DynamicHeader>> iterator = dynamicHeaders.entrySet().iterator();
			while (iterator.hasNext()){
				Map.Entry<String, DynamicHeader> entry = iterator.next();
				String value = null;
				try {
					value = entry.getValue().getValue(entry.getKey(),dynamicHeaderContext);
				} catch (DataImportException e) {
					throw e;
				} catch (Exception e) {
					throw new DataImportException("get value of "+entry.getKey() + " failed:",e);
				}
				if(value != null)
					ret.put(entry.getKey(),value);
			}
			return ret;
		}

	}
    private boolean destoryed = false;
    private Object lock = new Object();
    public void destory(){
//    	for(HttpConfigClientProxy httpConfigClientProxy:configDSLUtils.values()){
//    		httpConfigClientProxy.destory();
//    	}
        if(destoryed)
            return;
        synchronized (lock){
            if(destoryed){
                return;
            }
            destoryed = true;
        }
        configHolder.destory();
        configDSLUtils.clear();
    }

}
