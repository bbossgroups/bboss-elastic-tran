package org.frameworkset.tran.plugin.feishu;
/**
 * Copyright 2026 bboss
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

import com.frameworkset.util.JsonUtil;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.client.ConfigHolder;
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.elasticsearch.template.BaseTemplateMeta;
import org.frameworkset.elasticsearch.template.DSLParserException;
import org.frameworkset.elasticsearch.template.TemplateMeta;
import org.frameworkset.spi.feishu.BaseFeishuConfigInf;
import org.frameworkset.spi.feishu.FeishuException;
import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.spi.remote.http.HttpConfigInf;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.plugin.feishu.input.FeishuTableInputConfig;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author biaoping.yin
 * @Date 2026/4/26
 */
public class ConfigFeishuHelper extends FeishuHelper {

    protected FeishuHttpConfigClientProxy feishuHttpConfigClientProxy ;
    public ConfigFeishuHelper(BaseFeishuConfigInf baseFeishuConfig) {
        super(baseFeishuConfig);
    }

    private Map<String, FeishuHttpConfigClientProxy> configDSLUtils = new ConcurrentHashMap<>();

    private ConfigHolder configHolder = new ConfigHolder("FeishHttpProxy");
    public FeishuHttpConfigClientProxy getHttpConfigClientProxy( String configDSLFile){
        FeishuHttpConfigClientProxy httpConfigClientProxy = configDSLUtils.get(configDSLFile);
        if(httpConfigClientProxy != null)
            return httpConfigClientProxy;
        synchronized (configDSLUtils){
            httpConfigClientProxy = configDSLUtils.get(configDSLFile);
            if(httpConfigClientProxy != null)
                return httpConfigClientProxy;
            // TODO Auto-generated method stub
            httpConfigClientProxy =  new FeishuHttpConfigClientProxy(configHolder,configDSLFile);
            configDSLUtils.put(configDSLFile,httpConfigClientProxy);
        }
        return httpConfigClientProxy;
    }

    public FeishuHttpConfigClientProxy getHttpConfigClientProxy(BaseTemplateContainerImpl templateContainer){
        String namespace = templateContainer.getNamespace();
        FeishuHttpConfigClientProxy httpConfigClientProxy = configDSLUtils.get(namespace);
        if(httpConfigClientProxy != null)
            return httpConfigClientProxy;
        synchronized (configDSLUtils){
            httpConfigClientProxy = configDSLUtils.get(namespace);
            if(httpConfigClientProxy != null)
                return httpConfigClientProxy;
            // TODO Auto-generated method stub
            httpConfigClientProxy =  new FeishuHttpConfigClientProxy(configHolder,templateContainer);
            configDSLUtils.put(namespace,httpConfigClientProxy);
        }
        return httpConfigClientProxy;
    }

    public FeishuHttpConfigClientProxy getFeishuHttpConfigClientProxy() {
        return feishuHttpConfigClientProxy;
    }

    /**
     * 根据requestBodyName，从配置文件或者配置容器中解析和获取dsl
     * @param accessToken
     * @param params
     * @return
     */
    public Map searchDataConfigable(FeishuTableInputConfig feishuTableInputConfig,String searchUrl,String accessToken, Map params){
        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        Map listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( feishuTableInputConfig,
                searchUrl,feishuTableInputConfig.getQueryDslName(),params,headers,Map.class);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

        //是否成功 非0为不成功
        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
        }else{
            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
        }
        return listFieldsResult;
    }

    @Override
    public void init() {
        super.init();
        if(baseFeishuConfig instanceof FeishuTableInputConfig) {
            FeishuTableInputConfig feishuTableInputConfig = (FeishuTableInputConfig)baseFeishuConfig;
            if (SimpleStringUtil.isNotEmpty(feishuTableInputConfig.getRequestBody())) {
                feishuHttpConfigClientProxy = getHttpConfigClientProxy(new BaseTemplateContainerImpl(feishuTableInputConfig.getDslNamespace()) {
                    @Override
                    protected Map<String, TemplateMeta> loadTemplateMetas(String namespace) {
                        try {
                            BaseTemplateMeta baseTemplateMeta = new BaseTemplateMeta();
                            baseTemplateMeta.setName(feishuTableInputConfig.getQueryDslName());
                            baseTemplateMeta.setNamespace(namespace);
                            baseTemplateMeta.setDslTemplate(feishuTableInputConfig.getRequestBody());
                            baseTemplateMeta.setMultiparser(true);
                            Map<String, TemplateMeta> templateMetaMap = new LinkedHashMap<>();
                            templateMetaMap.put(baseTemplateMeta.getName(), baseTemplateMeta);
                            return templateMetaMap;
                        } catch (Exception e) {
                            throw new DSLParserException(e);
                        }
                    }

                    @Override
                    protected long getLastModifyTime(String namespace) {
                        return -1;
                    }
                    @Override
                    public boolean needMonitor(){
                        return false;
                    }
                });
            } else {
                feishuHttpConfigClientProxy = getHttpConfigClientProxy(feishuTableInputConfig.getDslFile());
            }
        }
    }
}
