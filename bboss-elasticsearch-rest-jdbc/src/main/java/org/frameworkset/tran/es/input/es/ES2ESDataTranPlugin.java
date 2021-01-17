package org.frameworkset.tran.es.input.es;
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
import org.frameworkset.tran.BaseDataTranPlugin;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.BaseESExporterScrollHandler;
import org.frameworkset.tran.es.ES2TranResultSet;
import org.frameworkset.tran.es.output.AsynESOutPutDataTran;

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
public class ES2ESDataTranPlugin  extends BaseDataTranPlugin implements DataTranPlugin {

	private ES2ESContext es2esContext;
	protected void init(ImportContext importContext){
		super.init(importContext);
		es2esContext = (ES2ESContext)importContext;

	}


	public ES2ESDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		super(  importContext,  targetImportContext);


	}

	@Override
	public void beforeInit() {
		this.initES(importContext.getApplicationPropertiesFile());
//		this.initDS(importContext.getDbConfig());
		initOtherDSes(importContext.getConfigs());

	}
	@Override
	public void afterInit(){
//		TranUtil.initTargetSQLInfo(es2DBContext,importContext.getDbConfig(),es2DBContext.getSqlName());

	}
	public void initStatusTableId(){
		if(isIncreamentImport() && es2esContext.getDslFile() != null && !es2esContext.getDslFile().equals("")) {
			try {
				ClientInterface clientInterface = ElasticSearchHelper.getConfigRestClientUtil(this.importContext.getSourceElasticsearch(),es2esContext.getDslFile());
				ESInfo esInfo = clientInterface.getESInfo(es2esContext.getDslName());
				importContext.setStatusTableId(esInfo.getTemplate().hashCode());
			} catch (Exception e) {
				throw new ESDataImportException(e);
			}
		}
	}



	private void commonImportData(BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = es2esContext.getParams() != null ?es2esContext.getParams():new HashMap();
		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(es2esContext.isSliceQuery()){
			params.put("sliceMax",es2esContext.getSliceSize());
		}
		exportESData(  esExporterScrollHandler,  params);
	}

	private void exportESData(BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler,Map params){

		//采用自定义handler函数处理每个scroll的结果集后，response中只会包含总记录数，不会包含记录集合
		//scroll上下文有效期1分钟；大数据量时可以采用handler函数来处理每次scroll检索的结果，规避数据量大时存在的oom内存溢出风险

		ClientInterface clientUtil = ElasticSearchHelper.getConfigRestClientUtil(this.importContext.getSourceElasticsearch(),es2esContext.getDslFile());

		ESDatas<MetaMap> response = null;
		if(!es2esContext.isSliceQuery()) {
			response = clientUtil.scroll(es2esContext.getQueryUrl(),
					es2esContext.getDslName(), es2esContext.getScrollLiveTime(),
					params, MetaMap.class, esExporterScrollHandler);
		}
		else{
			response = clientUtil.scrollSliceParallel(es2esContext.getQueryUrl(), es2esContext.getDslName(),
					params, es2esContext.getScrollLiveTime(), MetaMap.class, esExporterScrollHandler);
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
	private void increamentImportData(BaseESExporterScrollHandler<MetaMap> esExporterScrollHandler) throws Exception {
		Map params = es2esContext.getParams() != null ?es2esContext.getParams():new HashMap();
		params.put("size", importContext.getFetchSize());//每页5000条记录
		if(es2esContext.isSliceQuery()){
			params.put("sliceMax",es2esContext.getSliceSize());
		}
		putLastParamValue(params);
		exportESData(  esExporterScrollHandler,  params);

	}

	public void doImportData()  throws ESDataImportException {
		AsynBaseTranResultSet jdbcResultSet = new ES2TranResultSet(importContext);
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		final AsynESOutPutDataTran es2DBDataTran = new AsynESOutPutDataTran(jdbcResultSet,importContext,   targetImportContext,es2esContext.getTargetElasticsearch(),countDownLatch);
		ES2ESExporterScrollHandler<MetaMap> esExporterScrollHandler = new ES2ESExporterScrollHandler<MetaMap>(importContext,
				es2DBDataTran);
		try {
			Thread tranThread = new Thread(new Runnable() {
				@Override
				public void run() {
					es2DBDataTran.tran();
				}
			},"Elasticsearch-Elasticsearch-Tran");
			tranThread.start();
			if (!isIncreamentImport()) {

				commonImportData(esExporterScrollHandler);

			} else {

				increamentImportData(esExporterScrollHandler);

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
