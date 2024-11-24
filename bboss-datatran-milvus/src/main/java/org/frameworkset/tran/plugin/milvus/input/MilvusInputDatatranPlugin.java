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

import com.mongodb.BasicDBObject;
import io.milvus.orm.iterator.QueryIterator;
import io.milvus.v2.service.vector.request.QueryIteratorReq;
import org.frameworkset.nosql.milvus.MilvusConfig;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.nosql.milvus.MilvusStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.BaseInputPlugin;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusInputDatatranPlugin extends BaseInputPlugin {
	private static Logger logger = LoggerFactory.getLogger(MilvusInputDatatranPlugin.class);
	private MilvusInputConfig milvusInputConfig;
	private MilvusStartResult milvusStartResult = new MilvusStartResult();
	@Override
	public void init(){
		

	}

	@Override
	public void destroy(boolean waitTranStop) {
        MilvusHelper.shutdown(milvusStartResult); 
	}


	public MilvusInputDatatranPlugin(ImportContext importContext){
		super(  importContext);
        milvusInputConfig = (MilvusInputConfig) importContext.getInputConfig();
		this.jobType = "MilvusInputDatatranPlugin";

	}

	@Override
	public void beforeInit() {
		initMilvus();


	}

	protected void initMilvus(){
        MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName(milvusInputConfig.getName());//数据源名称
        milvusConfig.setDbName(milvusInputConfig.getDbName());//Milvus数据库名称
        milvusConfig.setUri(milvusInputConfig.getUri());//Milvus数据库地址
        milvusConfig.setToken(milvusInputConfig.getToken());//认证token：root:xxxx
        milvusConfig.setMaxIdlePerKey(milvusInputConfig.getMaxIdlePerKey());
        milvusConfig.setMinIdlePerKey(milvusInputConfig.getMinIdlePerKey());
        milvusConfig.setMaxTotalPerKey(milvusInputConfig.getMaxTotalPerKey());
        milvusConfig.setMaxTotal(milvusInputConfig.getMaxTotal());
        milvusConfig.setBlockWhenExhausted(milvusInputConfig.getBlockWhenExhausted());
        milvusConfig.setMaxBlockWaitDuration(milvusInputConfig.getMaxBlockWaitDuration());
        milvusConfig.setMinEvictableIdleDuration(milvusInputConfig.getMinEvictableIdleDuration());
        milvusConfig.setEvictionPollingInterval(milvusInputConfig.getEvictionPollingInterval());
        milvusConfig.setTestOnBorrow(milvusInputConfig.getTestOnBorrow());
        milvusConfig.setTestOnReturn(milvusInputConfig.getTestOnReturn());

        milvusConfig.setConnectTimeoutMs(milvusInputConfig.getConnectTimeoutMs());
        milvusConfig.setIdleTimeoutMs(milvusInputConfig.getIdleTimeoutMs());
        milvusConfig.setCustomConnectConfigBuilder(milvusInputConfig.getCustomConnectConfigBuilder());
        this.milvusStartResult = MilvusHelper.init(milvusConfig);//启动初始化Milvus数据源
        
	}
	@Override
	public void afterInit(){

	}

	@Override
	public void initStatusTableId() {
		if(importContext.isIncreamentImport()) {
			//计算增量记录id

			String statusTableId = milvusInputConfig.getDbName()+"|"+milvusInputConfig.getCollectionName()+"|"+milvusInputConfig.getUri();
			if(milvusInputConfig.getExpr() != null){
				statusTableId = statusTableId +"|" + milvusInputConfig.getExpr();
			}
			importContext.setStatusTableId(statusTableId.hashCode());
		}

	}

    private QueryIteratorReq.QueryIteratorReqBuilder getQueryIteratorReqBuilder(){
        QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder = QueryIteratorReq.builder()
                .collectionName(milvusInputConfig.getCollectionName())
                .outputFields(milvusInputConfig.getOutputFields())
                .batchSize(importContext.getFetchSize());
        return queryIteratorReqBuilder;
    }
	private void commonImportData( TaskContext taskContext) throws Exception {

        

        exportData(  getQueryIteratorReqBuilder().build(), taskContext);
 
	}

	private void exportData(QueryIteratorReq queryIteratorReq, TaskContext taskContext){

        MilvusHelper.executeRequest(milvusInputConfig.getName(), milvusClientV2 -> {
           
            QueryIterator queryIterator = milvusClientV2.queryIterator(queryIteratorReq);
            doTran(queryIterator, taskContext);
             
            return null;

        });
		

	}
	protected  void doTran(QueryIterator queryIterator, TaskContext taskContext){
        MilvusResultSet milvusResultSet = new MilvusResultSet(importContext,   queryIterator);
		BaseDataTran baseDataTran = dataTranPlugin.createBaseDataTran(taskContext,milvusResultSet,null,dataTranPlugin.getCurrentStatus());//new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
        baseDataTran.initTran();
        dataTranPlugin.callTran( baseDataTran);
	}
	private void increamentImportData( TaskContext taskContext) throws Exception {

        QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder = getQueryIteratorReqBuilder();       
         
		putLastParamValue(queryIteratorReqBuilder);
		exportData(  queryIteratorReqBuilder.build(), taskContext);
	}
	public void putLastParamValue(QueryIteratorReq.QueryIteratorReqBuilder queryIteratorReqBuilder){
        StringBuilder expr = new StringBuilder();
        if(milvusInputConfig.getExpr() != null)
            expr.append("(").append(milvusInputConfig.getExpr()).append(")");
            
		int lastValueType = dataTranPlugin.getLastValueType();
		Status currentStatus = dataTranPlugin.getCurrentStatus();
        LastValueWrapper currentLastValueWrapper = currentStatus.getCurrentLastValueWrapper();
        Object lastValue = currentLastValueWrapper.getLastValue();
		if(lastValueType == ImportIncreamentConfig.NUMBER_TYPE) {
            expr.append(" and ").append(getLastValueVarName()).append(" > ").append(lastValue);
		}
		else{
            Date lv = null;
			if(lastValue instanceof Date) {
				lv = (Date) lastValue;
			}
			else {
				if(lastValue instanceof Long) {
					lv =   new Date((Long)lastValue);
				}
				else if(lastValue instanceof Integer){
					lv =  new Date(((Integer) lastValue).longValue());
				}
				else if(lastValue instanceof Short){
					lv =  new Date(((Short) lastValue).longValue());
				}
				else{
					lv =  new Date(((Number) lastValue).longValue());
				}
			}

			if(importContext.increamentEndOffset() != null){
				Date lastOffsetValue = TimeUtil.addDateSeconds(new Date(),0-importContext.increamentEndOffset());
				BasicDBObject basicDBObject = new BasicDBObject();
				basicDBObject.put("$gt", lv);
				basicDBObject.put("$lte",lastOffsetValue);
                expr.append(" and ").append(getLastValueVarName()).append(" > ").append(lv.getTime());
                expr.append(" and ").append(getLastValueVarName()).append(" <= ").append(lastOffsetValue.getTime());
			}
			else{
                expr.append(" and ").append(getLastValueVarName()).append(" > ").append(lv.getTime());
			}
		}
        String ex = expr.toString();
        queryIteratorReqBuilder.expr(ex);//指定过滤条件，可以进行条件组合，具体参考文档：https://milvus.io/api-reference/java/v2.4.x/v2/Vector/search.md
		if(importContext.isPrintTaskLog()){
			logger.info(new StringBuilder().append("Current values: ").append(ex).toString());
		}
	}

	public void doImportData( TaskContext taskContext)  throws DataImportException {


			try {
				if (!importContext.isIncreamentImport()) {

					commonImportData(   taskContext );

				} else {

					increamentImportData(   taskContext );

				}
			} catch (DataImportException e) {
				throw e;
			} catch (Exception e) {
				throw ImportExceptionUtil.buildDataImportException(importContext,e);
			}

	}


}
