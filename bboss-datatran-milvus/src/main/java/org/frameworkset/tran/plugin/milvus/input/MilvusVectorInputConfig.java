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
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.vector.request.data.BaseVector;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.IllegementConfigException;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: Milvus向量search插件</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/11/1
 */
public class MilvusVectorInputConfig extends MilvusInputConfig<MilvusVectorInputConfig> {
    private static Logger logger = LoggerFactory.getLogger(MilvusVectorInputConfig.class);
   
     
     // 向量检索参数--开始
    private String vectorFieldName;
    //构建检索参数
    private String searchParams;
    private List<BaseVector> vectorData;
    private BuildMilvusVectorDataFunction buildMilvusVectorDataFunction;
    
    private IndexParam.MetricType metricType;
    // 向量检索参数--结束

    @Override
    public InputPlugin getInputPlugin(ImportContext importContext) {
        return new MilvusVectorInputDatatranPlugin(importContext);
    }

    public MilvusVectorInputConfig setBuildMilvusVectorDataFunction(BuildMilvusVectorDataFunction buildMilvusVectorDataFunction) {
        this.buildMilvusVectorDataFunction = buildMilvusVectorDataFunction;
        return this;
    }

    @Override
    public void build(ImportContext importContext, ImportBuilder importBuilder) {
        super.build(importContext,importBuilder);
        if(SimpleStringUtil.isEmpty(this.vectorFieldName)){
            throw new IllegementConfigException("milvus vectorFieldName is not set.");
        }
        
        if(SimpleStringUtil.isEmpty(searchParams)){
                throw new IllegementConfigException("milvus Vector searchParams is not set.");
        }
        if(SimpleStringUtil.isEmpty(this.vectorData) && buildMilvusVectorDataFunction == null){
            throw new IllegementConfigException("milvus vectorData is empty and buildMilvusVectorDataFunction is null,please set vectorData or buildMilvusVectorDataFunction.");
        }
        if(SimpleStringUtil.isEmpty(metricType) ){
            throw new IllegementConfigException("milvus metricType is not set.");
        }
        
    }
 

    public String getVectorFieldName() {
        return vectorFieldName;
    }

    public MilvusVectorInputConfig setVectorFieldName(String vectorFieldName) {
        this.vectorFieldName = vectorFieldName;
        return this;
    }

    public String getSearchParams() {
        return searchParams;
    }

    public MilvusVectorInputConfig setSearchParams(String searchParams) {
        this.searchParams = searchParams;
        return this;
    }

    public List<BaseVector> getVectorData() {
        return vectorData;
    }
    
    public void buildVectorData(){
        if(vectorData != null){
            return;
        }
        vectorData = this.buildMilvusVectorDataFunction.buildMilvusVectorData();
        if(vectorData == null){
            throw new DataImportException("buildMilvusVectorDataFunction failed: return null,");
        }
    }

    public MilvusVectorInputConfig setVectorData(List<BaseVector> vectorData) {
        this.vectorData = vectorData;
        return this;
    }

    public IndexParam.MetricType getMetricType() {
        return metricType;
    }

    /**
     * public enum MetricType {
     *         INVALID,
     *         // Only for float vectors
     *         L2,
     *         IP,
     *         COSINE,
     *
     *         // Only for binary vectors
     *         HAMMING,
     *         JACCARD,
     *
     *         // Only for sparse vector with BM25
     *         BM25,
     *         ;
     *     }
     * @param metricType
     * @return
     */
    public MilvusVectorInputConfig setMetricType(IndexParam.MetricType metricType) {
        this.metricType = metricType;
        return this;
    }
}
