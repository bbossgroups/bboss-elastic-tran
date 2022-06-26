package org.frameworkset.tran.plugin.es.input;
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
import org.frameworkset.tran.AsynBaseTranResultSet;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.BaseESExporterScrollHandler;
import org.frameworkset.tran.es.ES2TranResultSet;
import org.frameworkset.tran.es.input.ESExporterScrollHandler;
import org.frameworkset.tran.es.input.db.ESDirectExporterScrollHandler;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.es.BaseESPlugin;
import org.frameworkset.tran.schedule.TaskContext;

import java.text.SimpleDateFormat;
import java.util.Date;
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
public class ElasticsearchInputDataTranPlugin extends BaseESPlugin implements InputPlugin {
	public ElasticsearchInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
		elasticsearchInputConfig = (ElasticsearchInputConfig) importContext.getInputConfig();
	}
	protected ElasticsearchInputConfig elasticsearchInputConfig;
	@Override
	public void init(){


	}



	@Override
	public void beforeInit() {
		this.esConfig = elasticsearchInputConfig.getEsConfig();
		this.applicationPropertiesFile = importContext.getApplicationPropertiesFile();
		this.initES();


	}
	@Override
	public void afterInit(){

	}
	public void initStatusTableId(){
		if(dataTranPlugin.isIncreamentImport() && elasticsearchInputConfig.getDslFile() != null && !elasticsearchInputConfig.getDslFile().equals("")) {
			try {
				ClientInterface clientInterface = ElasticSearchHelper.getConfigRestClientUtil(elasticsearchInputConfig.getSourceElasticsearch(),elasticsearchInputConfig.getDslFile());
				ESInfo esInfo = clientInterface.getESInfo(elasticsearchInputConfig.getDslName());
				importContext.setStatusTableId(esInfo.getTemplate().hashCode());
			} catch (Exception e) {
				throw new DataImportException(e);
			}
		}
	}



	protected void commonImportData(TaskContext taskContext, BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = dataTranPlugin.getJobParams();

		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(elasticsearchInputConfig.isSliceQuery()){
			params.put("sliceMax",elasticsearchInputConfig.getSliceSize());
		}
		Date date = new Date();
		exportESData(  taskContext,  esExporterScrollHandler,  params,date,date);
	}

	protected String getQueryUrl(TaskContext taskContext,Date lastStartValue,Date lastEndValue){
		if(elasticsearchInputConfig.getQueryUrl() != null){
			return elasticsearchInputConfig.getQueryUrl();
		}
		else if(elasticsearchInputConfig.getQueryUrlFunction() != null){
			return elasticsearchInputConfig.getQueryUrlFunction().queryUrl(  taskContext,  lastStartValue,  lastEndValue);
		}
		throw new DataImportException("query url or query url function not setted.");
	}

	protected void exportESData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler,Map params,Date lastStartValue,Date lastEndValue){

		//采用自定义handler函数处理每个scroll的结果集后，response中只会包含总记录数，不会包含记录集合
		//scroll上下文有效期1分钟；大数据量时可以采用handler函数来处理每次scroll检索的结果，规避数据量大时存在的oom内存溢出风险

		ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil(elasticsearchInputConfig.getSourceElasticsearch(),elasticsearchInputConfig.getDslFile());

		ESDatas<MetaMap> response = null;
		if(!elasticsearchInputConfig.isSliceQuery()) {

			if(importContext.isParallel() && esExporterScrollHandler instanceof ESDirectExporterScrollHandler) {
				response = clientUtil.scrollParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						elasticsearchInputConfig.getDslName(), elasticsearchInputConfig.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
			else
			{
				response = clientUtil.scroll(getQueryUrl(  taskContext,lastStartValue,lastEndValue),
						elasticsearchInputConfig.getDslName(), elasticsearchInputConfig.getScrollLiveTime(),
						params, MetaMap.class, esExporterScrollHandler);
			}
		}
		else{
			response = clientUtil.scrollSliceParallel(getQueryUrl(  taskContext,lastStartValue,lastEndValue), elasticsearchInputConfig.getDslName(),
					params, elasticsearchInputConfig.getScrollLiveTime(),MetaMap.class, esExporterScrollHandler);
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

	/**
	 * 需要根据增量日期字段格式对date进行格式转换，避免采用默认utc格式检索出错
	 * @param date
	 * @return
	 */
	@Override
	protected Object formatLastDateValue(Date date){
		String lastValueDateformat = importContext.getLastValueDateformat();
		if(lastValueDateformat != null && !lastValueDateformat.equals("")){
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat(lastValueDateformat);
			return simpleDateFormat.format(date);
		}
		return date;


	}
	protected void increamentImportData(TaskContext taskContext,BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = dataTranPlugin.getJobParams();
		params.put("size", importContext.getFetchSize());//每页fetchSize条记录
		if(elasticsearchInputConfig.isSliceQuery()){
			params.put("sliceMax",elasticsearchInputConfig.getSliceSize());
		}
		Object[] lastValues = dataTranPlugin.putLastParamValue(params);
		Object lastValue = lastValues[0];
		if(lastValue instanceof Date) {
			Date lastEndValue = null;
			if(importContext.increamentEndOffset() != null){
//				lastEndValue = (Date)params.get(getLastValueVarName()+"__endTime");
				lastEndValue = (Date)lastValues[1];
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
//	protected abstract BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, CountDownLatch countDownLatch, Status currentStatus);

	public void doImportData(TaskContext taskContext)  throws DataImportException {


		AsynBaseTranResultSet jdbcResultSet = new ES2TranResultSet(importContext);
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		dataTranPlugin.createBaseDataTran( taskContext,jdbcResultSet,null,dataTranPlugin.getCurrentStatus());
		final BaseDataTran es2DBDataTran = dataTranPlugin.createBaseDataTran( taskContext,jdbcResultSet,countDownLatch,dataTranPlugin.getCurrentStatus());
//		final BaseDataTran es2DBDataTran = createBaseDataTran(  taskContext,jdbcResultSet,countDownLatch,  currentStatus);//new AsynDBOutPutDataTran(jdbcResultSet,importContext,   targetImportContext,countDownLatch);
		ESExporterScrollHandler<MetaMap> esExporterScrollHandler = new ESExporterScrollHandler<MetaMap>(importContext,
				es2DBDataTran);
		try {
			Thread tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					es2DBDataTran.tran();
				}
			},"Elasticsearch-DB-Tran");
			tranThread.start();
			if (!importContext.isIncreamentImport()) {

				commonImportData(  taskContext,esExporterScrollHandler);

			} else {

				increamentImportData(  taskContext,esExporterScrollHandler);

			}
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new DataImportException(e);
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
