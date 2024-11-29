package org.frameworkset.tran.plugin.milvus.output;
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
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.IllegementConfigException;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.milvus.MilvusConfigInf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/1
 */
public class MilvusOutputConfig extends BaseConfig implements OutputConfig, MilvusConfigInf {
    private static Logger logger = LoggerFactory.getLogger(MilvusOutputConfig.class);
    private String name;
    private String uri;
    private String token;
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
    private String dbName;

    private Long connectTimeoutMs;
    private Long idleTimeoutMs;
    private CustomConnectConfigBuilder customConnectConfigBuilder;

    /**
     * 标记是否采用upsert写入数据：This operation inserts or updates data in a specific collection.
     */
    private boolean upsert;
    private String collectionName;
    private String partitionName;
    private Map<String, Object> collectionSchemaIdx;
    List<String> fields;
    
    private boolean loadCollectionSchema = true;
    public String getName() {
        return name;
    }

    public Map<String, Object> getCollectionSchemaIdx() {
        return collectionSchemaIdx;
    }

    public MilvusOutputConfig setCollectionSchemaIdx(Map<String, Object> collectionSchemaIdx) {
        this.collectionSchemaIdx = collectionSchemaIdx;
        return this;
    }

    public List<String> getFields() {
        return fields;
    }

    public MilvusOutputConfig setFields(List<String> fields) {
        this.fields = fields;
        return this;
    }

    public MilvusOutputConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public MilvusOutputConfig setCollectionName(String collectionName) {
        this.collectionName = collectionName;
        return this;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public MilvusOutputConfig setPartitionName(String partitionName) {
        this.partitionName = partitionName;
        return this;
    }

    public String getUri() {
        return uri;
    }

    public MilvusOutputConfig setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getToken() {
        return token;
    }

    public MilvusOutputConfig setToken(String token) {
        this.token = token;
        return this;
    }

    public Integer getMaxIdlePerKey() {
        return maxIdlePerKey;
    }

    public MilvusOutputConfig setMaxIdlePerKey(Integer maxIdlePerKey) {
        this.maxIdlePerKey = maxIdlePerKey;
        return this;
    }

    public Integer getMinIdlePerKey() {
        return minIdlePerKey;
    }

    public MilvusOutputConfig setMinIdlePerKey(Integer minIdlePerKey) {
        this.minIdlePerKey = minIdlePerKey;
        return this;
    }

    public Integer getMaxTotalPerKey() {
        return maxTotalPerKey;
    }

    public MilvusOutputConfig setMaxTotalPerKey(Integer maxTotalPerKey) {
        this.maxTotalPerKey = maxTotalPerKey;
        return this;
    }

    public Integer getMaxTotal() {
        return maxTotal;
    }

    public MilvusOutputConfig setMaxTotal(Integer maxTotal) {
        this.maxTotal = maxTotal;
        return this;
    }

    public Boolean getBlockWhenExhausted() {
        return blockWhenExhausted;
    }

    public MilvusOutputConfig setBlockWhenExhausted(Boolean blockWhenExhausted) {
        this.blockWhenExhausted = blockWhenExhausted;
        return this;
    }

    public Long getMaxBlockWaitDuration() {
        return maxBlockWaitDuration;
    }

    public MilvusOutputConfig setMaxBlockWaitDuration(Long maxBlockWaitDuration) {
        this.maxBlockWaitDuration = maxBlockWaitDuration;
        return this;
    }

    public Long getMinEvictableIdleDuration() {
        return minEvictableIdleDuration;
    }

    public MilvusOutputConfig setMinEvictableIdleDuration(Long minEvictableIdleDuration) {
        this.minEvictableIdleDuration = minEvictableIdleDuration;
        return this;
    }

    public Long getEvictionPollingInterval() {
        return evictionPollingInterval;
    }

    public MilvusOutputConfig setEvictionPollingInterval(Long evictionPollingInterval) {
        this.evictionPollingInterval = evictionPollingInterval;
        return this;
    }

    public Boolean getTestOnBorrow() {
        return testOnBorrow;
    }

    public MilvusOutputConfig setTestOnBorrow(Boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return this;
    }

    public Boolean getTestOnReturn() {
        return testOnReturn;
    }

    public MilvusOutputConfig setTestOnReturn(Boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return this;
    }
    public boolean isUpsert() {
        return upsert;
    }
    /**
     * 标记是否采用upsert写入数据：This operation inserts or updates data in a specific collection.
     */
    public MilvusOutputConfig setUpsert(boolean upsert) {
        this.upsert = upsert;
        return this;
    }

    public String getDbName() {
        return dbName;
    }

    public MilvusOutputConfig setDbName(String dbName) {
        this.dbName = dbName;
        return this;
    }

    public Long getConnectTimeoutMs() {
        return connectTimeoutMs;
    }

    public MilvusOutputConfig setConnectTimeoutMs(Long connectTimeoutMs) {
        this.connectTimeoutMs = connectTimeoutMs;
        return this;
    }

    public Long getIdleTimeoutMs() {
        return idleTimeoutMs;
    }

    public MilvusOutputConfig setIdleTimeoutMs(Long idleTimeoutMs) {
        this.idleTimeoutMs = idleTimeoutMs;
        return this;
    }

    public CustomConnectConfigBuilder getCustomConnectConfigBuilder() {
        return customConnectConfigBuilder;
    }

    public MilvusOutputConfig setCustomConnectConfigBuilder(CustomConnectConfigBuilder customConnectConfigBuilder) {
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
        
    }

    @Override
    public OutputPlugin getOutputPlugin(ImportContext importContext) {
        return new MilvusOutputDataTranPlugin(importContext);
    }

    public boolean isLoadCollectionSchema() {
        return loadCollectionSchema;
    }

    public MilvusOutputConfig setLoadCollectionSchema(boolean loadCollectionSchema) {
        this.loadCollectionSchema = loadCollectionSchema;
        return this;
    }
}
