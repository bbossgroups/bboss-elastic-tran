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
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/1
 */
public class MilvusInputConfig extends BaseConfig implements InputConfig, MilvusConfigInf {
    private static Logger logger = LoggerFactory.getLogger(MilvusInputConfig.class);
    private String name;
    private String dbName;

    private String collectionName;
    private String uri;
    private String token;
    
    /**
     * 指定要返回的字段
     */
    List<String> outputFields;


    /**
     * 指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
     */
    private String expr;
    
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

    public MilvusInputConfig setOutputFields(List<String> outputFields) {
        this.outputFields = outputFields;
        return this;
    }

    public MilvusInputConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public MilvusInputConfig setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }
 
    public String getUri() {
        return uri;
    }

    public MilvusInputConfig setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getToken() {
        return token;
    }

    public MilvusInputConfig setToken(String token) {
        this.token = token;
        return this;
    }

    public Integer getMaxIdlePerKey() {
        return maxIdlePerKey;
    }

    public MilvusInputConfig setMaxIdlePerKey(Integer maxIdlePerKey) {
        this.maxIdlePerKey = maxIdlePerKey;
        return this;
    }

    public Integer getMinIdlePerKey() {
        return minIdlePerKey;
    }

    public MilvusInputConfig setMinIdlePerKey(Integer minIdlePerKey) {
        this.minIdlePerKey = minIdlePerKey;
        return this;
    }

    public Integer getMaxTotalPerKey() {
        return maxTotalPerKey;
    }

    public MilvusInputConfig setMaxTotalPerKey(Integer maxTotalPerKey) {
        this.maxTotalPerKey = maxTotalPerKey;
        return this;
    }

    public Integer getMaxTotal() {
        return maxTotal;
    }

    public MilvusInputConfig setMaxTotal(Integer maxTotal) {
        this.maxTotal = maxTotal;
        return this;
    }

    public Boolean getBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public MilvusInputConfig setBlockWhenExhausted(Boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
        return this;
    }

    public Long getMaxBlockWaitDuration() {
        return maxBlockWaitDuration;
    }

    public MilvusInputConfig setMaxBlockWaitDuration(Long maxBlockWaitDuration) {
        this.maxBlockWaitDuration = maxBlockWaitDuration;
        return this;
    }

    public Long getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    public MilvusInputConfig setMinEvictableIdleDuration(Long minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
        return this;
    }

    public Long getEvictionPollingInterval() {
        return evictionPollingInterval;
    }

    public MilvusInputConfig setEvictionPollingInterval(Long evictionPollingInterval) {
        this.evictionPollingInterval = evictionPollingInterval;
        return this;
    }

    public Boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public MilvusInputConfig setTestOnBorrow(Boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return this;
    }

    public Boolean getTestOnReturn() {
        return testOnReturn;
    }

    public MilvusInputConfig setTestOnReturn(Boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return this;
    }
    public String getDbName() {
        return dbName;
    }

    public MilvusInputConfig setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public Long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public MilvusInputConfig setConnectTimeoutMs(Long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        return this;
    }

    public Long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public MilvusInputConfig setIdleTimeoutMs(Long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
        return this;
    }

    public CustomConnectConfigBuilder getCustomConnectConfigBuilder() {
        return customConnectConfigBuilder;
    }

    public MilvusInputConfig setCustomConnectConfigBuilder(CustomConnectConfigBuilder customConnectConfigBuilder) {
        this.customConnectConfigBuilder = customConnectConfigBuilder;
        return this;
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

    public MilvusInputConfig setExpr(String expr) {
        this.expr = expr;
        return this;
    }

 
}
