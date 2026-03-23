package org.frameworkset.tran.plugin.feishu.output;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.feishu.FeishuHelper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public class FeishuTableOutputConfig extends BaseConfig<FeishuTableOutputConfig> implements OutputConfig<FeishuTableOutputConfig> {

    private String feishuDataSource;
    
    private String feishuTableId;
    private String feishuTableAppToken;
    
    private String feishuAppId;
    private String feishAppSecret;
    private String batchInsertUrl;
    private String batchUpdateUrl;
    private String listFieldsUrl;
    private String accessTokenKey = "accessToken";

    private String batchDeleteUrl;

    private Map<String,Object> httpConfigs;
    @JsonIgnore
    private FeishuHelper feishuHelper;

    public Map<String, Object> getHttpConfigs() {
        return httpConfigs;
    }

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        if(SimpleStringUtil.isEmpty(feishuTableAppToken) ){
            throw new IllegalArgumentException("feishuTableAppToken is empty!");
        }
        if(SimpleStringUtil.isEmpty(feishuTableId) ){
            throw new IllegalArgumentException("feishuTableId is empty!");
        }
        if(SimpleStringUtil.isEmpty(feishuDataSource) ){
            if(this.httpConfigs != null){
                String name = (String) httpConfigs.get("http.poolNames");
                int index = name.indexOf(",");
                if(index > 0){//声明多个数据源时，使用逗号分隔，取第一个
                    this.feishuDataSource = name.substring(0,index);
                }
                else{
                    this.feishuDataSource = name;
                }
            }
            else {
                throw new IllegalArgumentException("feishuDataSource is empty!");
            }
        }
        if(this.cellMappingList == null || this.cellMappingList.size() == 0){
            throw new IllegalArgumentException("未配置飞书多维表格字段映射：cellMappingList is empty!");
        }
        feishuHelper = new FeishuHelper(feishuDataSource,feishuAppId,feishAppSecret);
        batchInsertUrl = "/open-apis/bitable/v1/apps/"+feishuTableAppToken+"/tables/"+feishuTableId+"/records/batch_create";
        batchUpdateUrl = "/open-apis/bitable/v1/apps/"+feishuTableAppToken+"/tables/"+feishuTableId+"/records/batch_update";
        batchDeleteUrl = "/open-apis/bitable/v1/apps/"+feishuTableAppToken+"/tables/"+feishuTableId+"/records/batch_delete";
        listFieldsUrl = "/open-apis/bitable/v1/apps/"+feishuTableAppToken+"/tables/"+feishuTableId+"/fields";
    }

    public FeishuHelper getFeishuHelper() {
        return feishuHelper;
    }

    public String getBatchInsertUrl() {
        return batchInsertUrl;
    }

    public String getBatchUpdateUrl() {
        return batchUpdateUrl;
    }

    public String getBatchDeleteUrl() {
        return batchDeleteUrl;
    }

    public String getListFieldsUrl() {
        return listFieldsUrl;
    }

 
    private void checkConfigs(){
        if(httpConfigs == null)
            httpConfigs = new LinkedHashMap<>();
    }
    public FeishuTableOutputConfig addTargetHttpPoolName(String nameProperty, String httpPoolName){
        checkConfigs();
        this.httpConfigs.put(nameProperty,httpPoolName);
        this.feishuDataSource = httpPoolName;
        return this;
    }

    public FeishuTableOutputConfig addHttpOutputConfig(String property,Object value){
        checkConfigs();
        this.httpConfigs.put(property,value);
        return this;
    }
    /**
     * 根据上下文配置创建OutputPlugin
     *
     * @param importContext
     * @return
     */
    @Override
    public OutputPlugin getOutputPlugin(ImportContext importContext) {
        return new FeishuOutputDataTranPlugin(this,importContext);
    }

    public String getFeishuDataSource() {
        return feishuDataSource;
    }

    public FeishuTableOutputConfig setFeishuDataSource(String feishuDataSource) {
        this.feishuDataSource = feishuDataSource;
        return this;
    }

    public String getFeishuTableId() {
        return feishuTableId;
    }

    public FeishuTableOutputConfig setFeishuTableId(String feishuTableId) {
        this.feishuTableId = feishuTableId;
        return this;
    }

    public String getFeishuTableAppToken() {
        return feishuTableAppToken;
    }

    public FeishuTableOutputConfig setFeishuTableAppToken(String feishuTableAppToken) {
        this.feishuTableAppToken = feishuTableAppToken;
        return this;
    }

    public String getFeishuAppId() {
        return feishuAppId;
    }

    public FeishuTableOutputConfig setFeishuAppId(String feishuAppId) {
        this.feishuAppId = feishuAppId;
        return this;
    }

    public String getFeishAppSecret() {
        return feishAppSecret;
    }

    public FeishuTableOutputConfig setFeishAppSecret(String feishAppSecret) {
        this.feishAppSecret = feishAppSecret;
        return this;
    }

    public String getAccessTokenKey() {
        return accessTokenKey;
    }

    public FeishuTableOutputConfig setAccessTokenKey(String accessTokenKey) {
        this.accessTokenKey = accessTokenKey;
        return this;
    }
}
