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
import org.frameworkset.spi.feishu.BaseFeishuConfigInf;
import org.frameworkset.spi.feishu.FeishuException;
import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.spi.remote.http.template.*;
import org.frameworkset.tran.plugin.feishu.input.FeishuTableInputConfig;

import java.util.LinkedHashMap;
import java.util.List;
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

    @Override
    public void destroy(){
        super.destroy();
        destoryConfigHolder();
    }
    /**
     * 只能在系统退出时调用
     */
    public void destoryConfigHolder(){
        try {
            if(configDSLUtils != null){
                configDSLUtils.clear();
            }
            if (configHolder != null)
                configHolder.destory();
        }
        catch (Exception e){
            
        }
    } 
 

    public FeishuHttpConfigClientProxy getHttpConfigClientProxy(BaseDslTemplateContainerImpl templateContainer){
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
     * @param params
     * @return
     */
    public Map searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig, String searchUrl, Map params){
        return searchDataConfigable(  feishuTableInputConfig,  searchUrl, feishuTableInputConfig.getQueryDslName(), params);
//        
    }

    public Map searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,String searchUrl, String queryDslName,Map params){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(BaseFeishuConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        Map listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( feishuTableInputConfig,
                searchUrl,queryDslName,params,(Map)null,Map.class);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

        //是否成功 非0为不成功
        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
        }else{
            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
        }
        return listFieldsResult;
    }


    public <T> T searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,String searchUrl, String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        T listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( feishuTableInputConfig,
                searchUrl,queryDslName,params,(Map)null,type);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

//        //是否成功 非0为不成功
//        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
//        }else{
//            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
//        }
        return listFieldsResult;
    }
    

    public <T> T searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,String tableAppToken, String tableId,
                                      String pageToken,int pageSize,String userIdType,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        T listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( feishuTableInputConfig,
                FeishuHelper.buildSearchUrl(tableAppToken,tableId,pageToken,pageSize,userIdType),queryDslName,params,(Map)null,type);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

//        //是否成功 非0为不成功
//        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
//        }else{
//            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
//        }
        return listFieldsResult;
    }
    public <T> T searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,
                                      String tableAppToken, String tableId,int pageSize,String userIdType,String queryDslName,Map params,Class<T> type){
 
        return searchDataConfigable(  feishuTableInputConfig,
                  tableAppToken,   tableId, (String)null, pageSize,  userIdType,  queryDslName,  params,  type);
    }

    public <T> T searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,String tableAppToken, String tableId,int pageSize,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        String userIdType = "open_id";
        return searchDataConfigable(  feishuTableInputConfig,  tableAppToken, 
                  tableId,  pageSize,  userIdType,  queryDslName,  params,  type);
    }

    public <T> T searchDataConfigable(BaseFeishuConfigInf feishuTableInputConfig,String tableAppToken, 
                                      String tableId,String pageToken,
                                      int pageSize,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        String userIdType = "open_id";
        return searchDataConfigable(  feishuTableInputConfig,  tableAppToken,
                tableId, pageToken, pageSize,  userIdType,  queryDslName,  params,  type);
    }




 

    public Map searchDataConfigable(String searchUrl, String queryDslName,Map params){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        Map listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( this.baseFeishuConfig,
                searchUrl,queryDslName,params,(Map)null,Map.class);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

        //是否成功 非0为不成功
        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
        }else{
            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
        }
        return listFieldsResult;
    }


    public <T> T searchDataConfigable(String searchUrl, String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        T listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( this.baseFeishuConfig,
                searchUrl,queryDslName,params,(Map)null,type);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

//        //是否成功 非0为不成功
//        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
//        }else{
//            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
//        }
        return listFieldsResult;
    }


    public <T> T searchDataConfigable(String tableAppToken, String tableId,
                                      String pageToken,int pageSize,String userIdType,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        T listFieldsResult = feishuHttpConfigClientProxy.sendBodyForObject( this.baseFeishuConfig,
                FeishuHelper.buildSearchUrl(tableAppToken,tableId,pageToken,pageSize,userIdType),queryDslName,params,(Map)null,type);

//        logger.info("推送多维表格结果:{}", JsonUtil.object2json(message_));

//        //是否成功 非0为不成功
//        if(listFieldsResult != null && Integer.valueOf(0).equals(listFieldsResult.get("code"))){
//        }else{
//            throw new FeishuException("查询数据失败："+ JsonUtil.object2json(listFieldsResult));
//        }
        return listFieldsResult;
    }
    public <T> T searchDataConfigable(
                                      String tableAppToken, String tableId,int pageSize,String userIdType,String queryDslName,Map params,Class<T> type){

        return searchDataConfigable(  
                tableAppToken,   tableId, (String)null, pageSize,  userIdType,  queryDslName,  params,  type);
    }

    public <T> T searchDataConfigable(String tableAppToken, String tableId,int pageSize,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        String userIdType = "open_id";
        return searchDataConfigable(    tableAppToken,
                tableId,  pageSize,  userIdType,  queryDslName,  params,  type);
    }

    public <T> T searchDataConfigable(String tableAppToken,
                                      String tableId,String pageToken,
                                      int pageSize,String queryDslName,Map params,Class<T> type){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        String userIdType = "open_id";
        return searchDataConfigable(    tableAppToken,
                tableId, pageToken, pageSize,  userIdType,  queryDslName,  params,  type);
    }


 //-------------------------------------------


    public void searchDataConfigable(String tableAppToken, String tableId,
                                       int pageSize,String userIdType,String queryDslName,Map params,FeishuRowHandler feishuRowHandler){
        boolean hasMore = false;
        String pageToken = null;
        if(userIdType == null){
            userIdType = "open_id";
        }
        do {
            hasMore = false;
            Map datas = feishuHttpConfigClientProxy.sendBodyForObject( this.baseFeishuConfig,
                    FeishuHelper.buildSearchUrl(tableAppToken,tableId,pageToken,pageSize,userIdType),queryDslName,params,(Map)null,Map.class);
            if (datas != null) {
                int code = (Integer) datas.get("code");
                if (code != 0) {
                    throw new FeishuException(JsonUtil.object2json(datas));
                }
                Map data = (Map) datas.get("data");
                if (data != null) {
                    List<Map> items = (List<Map>) data.get("items");

                    if (items != null && items.size() > 0) {

                        for (Map item : items) {
                            try {
                                feishuRowHandler.handle(item);
                            } catch (Exception e) {
                                throw new FeishuException(e);
                            }
                        }

                    }
                    hasMore = (boolean) data.get("has_more");
                    pageToken = (String) data.get("page_token");
                }

            }
        }while (hasMore);
         

 
    }
   

    public void searchDataConfigable(String tableAppToken, String tableId,int pageSize,String queryDslName,Map params,FeishuRowHandler feishuRowHandler){
//        Map headers = buildHeaders(accessToken);

//        sendBodyForObject(HttpConfigInf httpConfigInf,  String searchUrl, String queryDslName,Map params,
//                Map headers,
//                Class<T> resultType)
        String userIdType = "open_id";
        searchDataConfigable(    tableAppToken,
                tableId,  pageSize,  userIdType,  queryDslName,  params,  feishuRowHandler);
    }
 

    @Override
    public void init() {
        super.init();
        if(baseFeishuConfig instanceof FeishuTableInputConfig) {
            FeishuTableInputConfig feishuTableInputConfig = (FeishuTableInputConfig)baseFeishuConfig;
            if (SimpleStringUtil.isNotEmpty(feishuTableInputConfig.getRequestBody())) {
                feishuHttpConfigClientProxy = getHttpConfigClientProxy(new BaseDslTemplateContainerImpl(feishuTableInputConfig.getDslNamespace()) {
                    @Override
                    protected Map<String, DslTemplateMeta> loadTemplateMetas(String namespace) {
                        try {
                            BaseDslTemplateMeta baseTemplateMeta = new BaseDslTemplateMeta();
                            baseTemplateMeta.setName(feishuTableInputConfig.getQueryDslName());
                            baseTemplateMeta.setNamespace(namespace);
                            baseTemplateMeta.setDslTemplate(feishuTableInputConfig.getRequestBody());
                            baseTemplateMeta.setMultiparser(true);
                            Map<String, DslTemplateMeta> templateMetaMap = new LinkedHashMap<>();
                            templateMetaMap.put(baseTemplateMeta.getName(), baseTemplateMeta);
                            return templateMetaMap;
                        } catch (Exception e) {
                            throw new DslConfigException(e);
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
            }  
            else if (feishuTableInputConfig.getDslFile() != null) {
                feishuHttpConfigClientProxy = getHttpConfigClientProxy(feishuTableInputConfig.getDslFile());
            }
        }
        else {
             
            BaseFeishuTableConfig baseFeishuTableConfig = (BaseFeishuTableConfig) baseFeishuConfig;
            if (baseFeishuTableConfig.getDslFile() != null) {
                feishuHttpConfigClientProxy = getHttpConfigClientProxy(baseFeishuTableConfig.getDslFile());
            }
             
        }
    }
}
