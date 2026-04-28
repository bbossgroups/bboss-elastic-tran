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
import org.frameworkset.spi.feishu.BaseFeishuConfigInf;
import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.spi.remote.http.HttpConfigInf;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author biaoping.yin
 * @Date 2026/3/23
 */
public abstract class BaseFeishuTableConfig<T extends BaseFeishuTableConfig> extends BaseConfig<T>  implements BaseFeishuConfigInf<T> , HttpConfigInf {

    protected String feishuDataSource;

    protected String feishuTableId;
    protected String feishuTableAppToken;

    protected String feishuAppId;
    protected String feishAppSecret;




    private String recordIdFieldName = "record_id";

    protected String feishuViewId;

    protected String accessTokenKey = "accessToken";

    protected String searchUrl;
    protected String userIdType = "open_id";



    protected Map<String,Object> httpConfigs;
    protected boolean showDsl;
    @JsonIgnore
    protected ConfigFeishuHelper feishuHelper;

    public Map<String, Object> getHttpConfigs() {
        return httpConfigs;
    }

    public String getSearchUrl() {
        return searchUrl;
    }
    
    public T setShowDsl(boolean showDsl) {
        this.showDsl = showDsl;
        return (T)this;
    }

    public boolean isShowDsl() {
        return showDsl;
    }

    public String getUserIdType() {
        return userIdType;
    }

    public T setUserIdType(String userIdType) {
        this.userIdType = userIdType;
        return (T)this;
    }

    public String getAccessToken(TaskContext taskContext, String accessTokenKey){
        String accessToken = null;
        if(taskContext.getJobFlowNodeExecuteContext() != null) {
//		String accessToken = customOutPutContext.getTaskContext().getTaskStringData("accessToken");
            accessToken = (String) taskContext.getJobFlowNodeExecuteContext().getJobFlowContextData(accessTokenKey);
        }
        else{
            accessToken = taskContext.getTaskStringData(accessTokenKey);
        }
        return accessToken;
    }
    /**
     * access_token expire time:默认值2小时，刷新时间提前10分钟
     */
    protected long accessTokenExpireTime = 2L * 50L * 60L * 1000L;
    public long getAccessTokenExpireTime() {
        return accessTokenExpireTime;
    }
    public T setAccessTokenExpireTime(long accessTokenExpireTime) {
        this.accessTokenExpireTime = accessTokenExpireTime;
        return (T)this;
    }
    public String getAccessToken(TaskContext taskContext){
        String accessToken = null;
        if(taskContext.getJobFlowNodeExecuteContext() != null) {
//		String accessToken = customOutPutContext.getTaskContext().getTaskStringData("accessToken");
            accessToken = (String) taskContext.getJobFlowNodeExecuteContext().getJobFlowContextData(accessTokenKey);
        }
        else{
            accessToken = taskContext.getTaskStringData(accessTokenKey);
        }
        return accessToken;
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
                if(!this.httpConfigs.containsKey(feishuDataSource+ ".http.authorTokenFunction")) {
                    addHttpConfig(feishuDataSource + ".http.authorTokenFunction", "org.frameworkset.spi.feishu.FeishuAuthorTokenFunction");
                }
                if(!this.httpConfigs.containsKey(feishuDataSource+ ".http.authorTokenExpiredTime")) {
                    addHttpConfig(feishuDataSource + ".http.authorTokenExpiredTime", accessTokenExpireTime);
                }
                if(!this.httpConfigs.containsKey(feishuDataSource+ ".http.extendConfigs.appId")) {
                    addHttpConfig(feishuDataSource + ".http.extendConfigs.appId", this.getFeishuAppId());
                }
                if(!this.httpConfigs.containsKey(feishuDataSource+ ".http.extendConfigs.appSecret")) {
                    addHttpConfig(feishuDataSource + ".http.extendConfigs.appSecret", this.getFeishAppSecret());
                }
            }
            else {
                String feishuDataSource = SimpleStringUtil.getUUID32();
                addHttpConfig("http.poolNames", feishuDataSource)
                        .addHttpConfig(feishuDataSource+ ".http.hosts", "https://open.feishu.cn")
                        .addHttpConfig(feishuDataSource+ ".http.maxTotal", 100)
                        .addHttpConfig(feishuDataSource+ ".http.defaultMaxPerRoute", 100)
                        .addHttpConfig(feishuDataSource+ ".http.timeoutConnection", 15000)
                        .addHttpConfig(feishuDataSource+ ".http.connectionRequestTimeout", 10000)
                        
//                    #socket通讯超时时间，如果在通讯过程中出现sockertimeout异常，可以适当调整timeoutSocket参数值，单位：毫秒
                        .addHttpConfig(feishuDataSource+ ".http.timeoutSocket", 120000)
                        .addHttpConfig(feishuDataSource+ ".http.authorTokenFunction","org.frameworkset.spi.feishu.FeishuAuthorTokenFunction")
                        .addHttpConfig(feishuDataSource+ ".http.authorTokenExpiredTime",accessTokenExpireTime)
                        .addHttpConfig(feishuDataSource+ ".http.extendConfigs.appId",this.getFeishuAppId())
                        .addHttpConfig(feishuDataSource+ ".http.extendConfigs.appSecret", this.getFeishAppSecret());
                this.feishuDataSource = feishuDataSource;        
            }
        }     
        
        
       
    }
    public void destroy(){
        if(feishuHelper != null) {
            feishuHelper.destroy();
        }
    }
    public void initFeishHelper(){
        ConfigFeishuHelper feishuHelper = new ConfigFeishuHelper(this);
        feishuHelper.init();
        this.feishuHelper = feishuHelper;
    }


    public ConfigFeishuHelper getFeishuHelper() {
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

    public String getFeishuViewId() {
        return feishuViewId;
    }

    public T setFeishuViewId(String feishuViewId) {
        this.feishuViewId = feishuViewId;
        return (T)this;
    }

    public String getRecordIdFieldName() {
        return recordIdFieldName;
    }

    public T setRecordIdFieldName(String recordIdFieldName) {
        this.recordIdFieldName = recordIdFieldName;
        return (T)this;
    }

    @Override
    public String getDatasource() {
        return this.feishuDataSource;
    }
}
