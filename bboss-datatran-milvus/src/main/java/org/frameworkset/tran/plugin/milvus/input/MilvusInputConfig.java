package org.frameworkset.tran.plugin.milvus.input;
/**
 * Copyright 2024 bboss
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

import com.frameworkset.util.SimpleStringUtil;
import io.milvus.v2.common.ConsistencyLevel;
import org.frameworkset.nosql.milvus.CustomConnectConfigBuilder;
import org.frameworkset.tran.IllegementConfigException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.milvus.MilvusConfigInf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: Milvus query插件</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/1
 */
public class MilvusInputConfig<T extends MilvusInputConfig> extends BaseConfig<MilvusInputConfig> implements InputConfig<MilvusInputConfig>, MilvusConfigInf<MilvusInputConfig> {
    private static Logger logger = LoggerFactory.getLogger(MilvusInputConfig.class);
    private String name;
    private String dbName;

    private String collectionName;
    private String uri;
    private String token;
    
    /**
     * 指定要返回的字段
     */
    private List<String> outputFields;


    /**
     * 指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
     */
    private String expr;

    private ConsistencyLevel consistencyLevel;
     
    
    
    private Integer maxIdlePerKey;
    private Integer minIdlePerKey;
    private Integer maxTotalPerKey;
    private Integer maxTotal;
    private Boolean blockWhenExhausted;
    private Long maxBlockWaitDuration;
    private Long minEvictableIdleDuration;
    private Long evictionPollingInterval;
    private Boolean testOnBorrow;
    private Boolean testOnReturn;
  

    private Long connectTimeoutMs;
    private Long idleTimeoutMs;
    private CustomConnectConfigBuilder customConnectConfigBuilder;



    
    public String getName() {
        return name;
    }
 

    public List<String> getOutputFields() {
        return outputFields;
    }

    public T setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
        return (T)this;
    }

    public T setName(String name) {
        this.name = name;
        return (T)this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public T setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return (T)this;
    }
 
    public String getUri() {
        return uri;
    }

    public T setUri(String uri) {
        this.uri = uri;
        return (T)this;
    }

    public String getToken() {
        return token;
    }

    public T setToken(String token) {
        this.token = token;
        return (T)this;
    }

    public Integer getMaxIdlePerKey() {
        return maxIdlePerKey;
    }

    public T setMaxIdlePerKey(Integer maxIdlePerKey) {
        this.maxIdlePerKey = maxIdlePerKey;
        return (T)this;
    }

    public Integer getMinIdlePerKey() {
        return minIdlePerKey;
    }

    public T setMinIdlePerKey(Integer minIdlePerKey) {
        this.minIdlePerKey = minIdlePerKey;
        return (T)this;
    }

    public Integer getMaxTotalPerKey() {
        return maxTotalPerKey;
    }

    public T setMaxTotalPerKey(Integer maxTotalPerKey) {
        this.maxTotalPerKey = maxTotalPerKey;
        return (T)this;
    }

    public Integer getMaxTotal() {
        return maxTotal;
    }

    public T setMaxTotal(Integer maxTotal) {
        this.maxTotal = maxTotal;
        return (T)this;
    }

    public Boolean getBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public T setBlockWhenExhausted(Boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
        return (T)this;
    }

    public Long getMaxBlockWaitDuration() {
        return maxBlockWaitDuration;
    }

    public T setMaxBlockWaitDuration(Long maxBlockWaitDuration) {
        this.maxBlockWaitDuration = maxBlockWaitDuration;
        return (T)this;
    }

    public Long getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    public T setMinEvictableIdleDuration(Long minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
        return (T)this;
    }

    public Long getEvictionPollingInterval() {
        return evictionPollingInterval;
    }

    public T setEvictionPollingInterval(Long evictionPollingInterval) {
        this.evictionPollingInterval = evictionPollingInterval;
        return (T)this;
    }

    public Boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public T setTestOnBorrow(Boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return (T)this;
    }

    public Boolean getTestOnReturn() {
        return testOnReturn;
    }

    public T setTestOnReturn(Boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return (T)this;
    }
    public String getDbName() {
        return dbName;
    }

    public T setDbName(String dbName) {
        this.dbName = dbName;
        return (T)this;
    }

    public Long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public T setConnectTimeoutMs(Long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        return (T)this;
    }

    public Long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public T setIdleTimeoutMs(Long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
        return (T)this;
    }

    public CustomConnectConfigBuilder getCustomConnectConfigBuilder() {
        return customConnectConfigBuilder;
    }

    public T setCustomConnectConfigBuilder(CustomConnectConfigBuilder customConnectConfigBuilder) {
        this.customConnectConfigBuilder = customConnectConfigBuilder;
        return (T)this;
    }

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        if(SimpleStringUtil.isEmpty(this.name)){
            throw new IllegementConfigException("milvus datasource name is empty.");
        }
        
        if(SimpleStringUtil.isEmpty(this.uri)){
            logger.info("milvus uri is empty,will use outner milvus datasource "+this.name);
        }
        else{
            if(SimpleStringUtil.isEmpty(this.dbName)){
                throw new IllegementConfigException("milvus dbName is empty.");
            }
        }
        if(SimpleStringUtil.isEmpty(this.collectionName)){
            throw new IllegementConfigException("milvus collectionName is empty.");
        }
        if(SimpleStringUtil.isEmpty(outputFields) ){
            throw new IllegementConfigException("milvus outputFields is empty.");
        }
        
    }

    @Override
    public InputPlugin getInputPlugin(ImportContext importContext) {
        return new MilvusInputDatatranPlugin(importContext);
    }
    public String getExpr() {
        return expr;
    }

    public T setExpr(String expr) {
        this.expr = expr;
        return (T)this;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public T setConsistencyLevel(ConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
        return (T)this;
    }

}
