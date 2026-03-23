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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public abstract class BaseFeishuTableConfig<T extends BaseFeishuTableConfig> extends BaseConfig<T>   {

    protected String feishuDataSource;

    protected String feishuTableId;
    protected String feishuTableAppToken;

    protected String feishuAppId;
    protected String feishAppSecret;

    protected String accessTokenKey = "accessToken";



    protected Map<String,Object> httpConfigs;
    @JsonIgnore
    protected FeishuHelper feishuHelper;

    public Map<String, Object> getHttpConfigs() {
        return httpConfigs;
    }

     
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
        
        feishuHelper = new FeishuHelper(feishuDataSource,feishuAppId,feishAppSecret);
       
    }

    public FeishuHelper getFeishuHelper() {
        return feishuHelper;
    }
 
 
    private void checkConfigs(){
        if(httpConfigs == null)
            httpConfigs = new LinkedHashMap<>();
    }
     

    public T addHttpConfig(String property, Object value){
        checkConfigs();
        this.httpConfigs.put(property,value);
        return (T)this;
    }
    

    public String getFeishuDataSource() {
        return feishuDataSource;
    }

    public T setFeishuDataSource(String feishuDataSource) {
        this.feishuDataSource = feishuDataSource;
        return (T)this;
    }

    public String getFeishuTableId() {
        return feishuTableId;
    }

    public T setFeishuTableId(String feishuTableId) {
        this.feishuTableId = feishuTableId;
        return (T)this;
    }

    public String getFeishuTableAppToken() {
        return feishuTableAppToken;
    }

    public T setFeishuTableAppToken(String feishuTableAppToken) {
        this.feishuTableAppToken = feishuTableAppToken;
        return (T)this;
    }

    public String getFeishuAppId() {
        return feishuAppId;
    }

    public T setFeishuAppId(String feishuAppId) {
        this.feishuAppId = feishuAppId;
        return (T)this;
    }

    public String getFeishAppSecret() {
        return feishAppSecret;
    }

    public T setFeishAppSecret(String feishAppSecret) {
        this.feishAppSecret = feishAppSecret;
        return (T)this;
    }

    public String getAccessTokenKey() {
        return accessTokenKey;
    }

    public T setAccessTokenKey(String accessTokenKey) {
        this.accessTokenKey = accessTokenKey;
        return (T)this;
    }
}
