package org.frameworkset.tran.es.input;
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

import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.elasticsearch.entity.MetaMap;
import org.frameworkset.elasticsearch.template.ESInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.BaseESExporterScrollHandler;
import org.frameworkset.tran.es.ES2TranResultSet;
import org.frameworkset.tran.es.input.db.ESDirectExporterScrollHandler;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class ESInputPlugin extends BaseDataTranPlugin implements DataTranPlugin {

	protected ESInputContext esInputContext;
	protected void init(ImportContext importContext, ImportContext targetImportContext){
		super.init(importContext,   targetImportContext);
		esInputContext = (ESInputContext)importContext;

	}


	public ESInputPlugin(ImportContext importContext, ImportContext targetImportContext){
		super(  importContext,  targetImportContext);


	}

	@Override
	public void beforeInit() {
		this.initES(importContext.getApplicationPropertiesFile());


	}
	@Override
	public void afterInit(){

	}
	public void initStatusTableId(){
		if(isIncreamentImport() && esInputContext.getDslFile() != null && !esInputContext.getDslFile().equals("")) {
			try {
				ClientInterface clientInterface = ElasticSearchHelper.getConfigRestClientUtil(importContext.getSourceElasticsearch(),esInputContext.getDslFile());
				ESInfo esInfo = clientInterface.getESInfo(esInputContext.getDslName());
				importContext.setStatusTableId(esInfo.getTemplate().hashCode());
			} catch (Exception e) {
				throw new ESDataImportException(e);
			}
		}
	}



	protected void commonImportData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = esInputContext.getParams() != null ?esInputContext.getParams():new HashMap();
		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(esInputContext.isSliceQuery()){
			params.put("sliceMax",esInputContext.getSliceSize());
		}
		Date date = new Date();
		exportESData(  taskContext,  esExporterScrollHandler,  params,date,date);
	}

	protected String getQueryUrl(TaskContext taskContext,Date lastStartValue,Date lastEndValue){
		if(esInputContext.getQueryUrl() != null){
			return esInputContext.getQueryUrl();
		}
		else if(esInputContext.getQueryUrlFunction() != null){
			return esInputContext.getQueryUrlFunction().queryUrl(  taskContext,  lastStartValue,  lastEndValue);
		}
		throw new DataImportException("query url or query url function not setted.");
	}

	protected void exportESData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler,Map params,Date lastStartValue,Date lastEndValue){

		//采用自定义handler函数处理每个scroll的结果集后，response中只会包含总记录数，不会包含记录集合
		//scroll上下文有效期1分钟；大数据量时可以采用handler函数来处理每次scroll检索的结果，规避数据量大时存在的oom内存溢出风险

		ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil(importContext.getSourceElasticsearch(),esInputContext.getDslFile());

		ESDatas<MetaMap> response = null;
		if(!esInputContext.isSliceQuery()) {

			if(importContext.isParallel() && esExporterScrollHandler instanceof ESDirectExporterScrollHandler) {
				response = clientUtil.scrollParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						esInputContext.getDslName(), esInputContext.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
			else
			{
				response = clientUtil.scroll(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						esInputContext.getDslName(), esInputContext.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
		}
		else{
			response = clientUtil.scrollSliceParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue), esInputContext.getDslName(),
					params, esInputContext.getScrollLiveTime(),MetaMap.class, esExporterScrollHandler);
		}
		if(logger.isInfoEnabled()) {
			if(response != null) {
				logger.info("Export compoleted and export total {} records.", response.getTotalSize());
			}
			else{
				logger.info("Export compoleted and export no records or failed.");
			}
		}
	}
	protected void increamentImportData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = esInputContext.getParams() != null ?esInputContext.getParams():new HashMap();
		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(esInputContext.isSliceQuery()){
			params.put("sliceMax",esInputContext.getSliceSize());
		}
		Object lastValue = putLastParamValue(params);

		if(lastValue instanceof Date) {
			Date lastEndValue = null;
			if(importContext.increamentEndOffset() != null){
				lastEndValue = (Date)params.get(getLastValueVarName()+"__endTime");
			}
			else
				lastEndValue = new Date();
			exportESData(  taskContext,esExporterScrollHandler, params, (Date)lastValue,lastEndValue);
		}
		else{
			Date date = new Date();
			exportESData(  taskContext,esExporterScrollHandler, params, date,date);
		}

	}
	protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, CountDownLatch countDownLatch, Status currentStatus);
	protected void doBatchHandler(TaskContext taskContext){

	}
	public void doImportData(TaskContext taskContext)  throws ESDataImportException {


		AsynBaseTranResultSet jdbcResultSet = new ES2TranResultSet(importContext);
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final BaseDataTran es2DBDataTran = createBaseDataTran(  taskContext,jdbcResultSet,countDownLatch,  currentStatus);//new AsynDBOutPutDataTran(jdbcResultSet,importContext,   targetImportContext,countDownLatch);
		ESExporterScrollHandler<MetaMap> esExporterScrollHandler = new ESExporterScrollHandler<MetaMap>(importContext,
				targetImportContext,es2DBDataTran);
		try {
			Thread tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					es2DBDataTran.tran();
				}
			},"Elasticsearch-DB-Tran");
			tranThread.start();
			if (!isIncreamentImport()) {

				commonImportData(  taskContext,esExporterScrollHandler);

			} else {

				increamentImportData(  taskContext,esExporterScrollHandler);

			}
		} catch (ESDataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new ESDataImportException(e);
		}
		finally {
			jdbcResultSet.reachEend();
			try {
				countDownLatch.await();
			} catch (InterruptedException e) {
				if(logger.isErrorEnabled())
					logger.error("",e);
			}
		}

	}


}
