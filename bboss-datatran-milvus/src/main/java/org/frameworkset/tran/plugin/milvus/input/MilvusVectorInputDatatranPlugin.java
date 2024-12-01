package org.frameworkset.tran.plugin.milvus.input;
/**
 * Copyright 2008 biaoping.yin
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

import io.milvus.orm.iterator.SearchIterator;
import io.milvus.v2.service.vector.request.SearchIteratorReq;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusVectorInputDatatranPlugin extends MilvusInputDatatranPlugin {
	private static Logger logger = LoggerFactory.getLogger(MilvusVectorInputDatatranPlugin.class);
    private MilvusVectorInputConfig milvusVectorInputConfig; 


	public MilvusVectorInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
        milvusVectorInputConfig = (MilvusVectorInputConfig) importContext.getInputConfig();
		this.jobType = "MilvusVectorInputDatatranPlugin";
	}
 
  

	@Override
	public void initStatusTableId() {
		if(importContext.isIncreamentImport()) {
			//计算增量记录id

			String statusTableId = milvusVectorInputConfig.getDbName()+"|"+milvusVectorInputConfig.getCollectionName()+"|"+milvusVectorInputConfig.getUri() + "|"+milvusVectorInputConfig.getVectorFieldName();
			if(milvusVectorInputConfig.getExpr() != null){
				statusTableId = statusTableId +"|" + milvusVectorInputConfig.getExpr();
			}
			importContext.setStatusTableId(statusTableId.hashCode());
		}

	}

    @Override
    public void afterInit() {
        if(milvusVectorInputConfig.getVectorData() == null){
            milvusVectorInputConfig.buildVectorData();
        }
    }

    private SearchIteratorReq.SearchIteratorReqBuilder getSearchIteratorReqBuilder(){
        SearchIteratorReq.SearchIteratorReqBuilder searchIteratorReqBuilder = SearchIteratorReq.builder()
                .collectionName(milvusVectorInputConfig.getCollectionName())
                .outputFields(milvusVectorInputConfig.getOutputFields())
                .batchSize(importContext.getFetchSize());
        if(milvusVectorInputConfig.getConsistencyLevel() != null){
            searchIteratorReqBuilder.consistencyLevel(milvusVectorInputConfig.getConsistencyLevel());
        } 
        searchIteratorReqBuilder.vectorFieldName(milvusVectorInputConfig.getVectorFieldName())
                .vectors(milvusVectorInputConfig.getVectorData())
                .params(milvusVectorInputConfig.getSearchParams()) //返回content与查询条件相似度为0.85以上的记录
                .metricType(milvusVectorInputConfig.getMetricType()); //采用余弦相似度算法
        return searchIteratorReqBuilder;
    }
    @Override
	protected void commonImportData( TaskContext taskContext) throws Exception {       

        exportData(  getSearchIteratorReqBuilder().build(), taskContext);
 
	}

	private void exportData(SearchIteratorReq searchIteratorReq, TaskContext taskContext){

        MilvusHelper.executeRequest(milvusVectorInputConfig.getName(), milvusClientV2 -> {

            SearchIterator searchIterator = milvusClientV2.searchIterator(searchIteratorReq);
            doTran(() -> {
                MilvusResultSet milvusResultSet = new MilvusVectorResultSet(importContext,   searchIterator);
                return milvusResultSet;
            }, taskContext);
             
            return null;

        });
		

	}
 
    @Override
	protected void increamentImportData( TaskContext taskContext) throws Exception {

        SearchIteratorReq.SearchIteratorReqBuilder searchIteratorReqBuilder = getSearchIteratorReqBuilder();       
         
		putLastParamValue(searchIteratorReqBuilder);
		exportData(  searchIteratorReqBuilder.build(), taskContext);
	}
	public void putLastParamValue(SearchIteratorReq.SearchIteratorReqBuilder searchIteratorReqBuilder){
         
        String ex = buildExpr();
        searchIteratorReqBuilder.expr(ex);//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
		 
	}
 


}
