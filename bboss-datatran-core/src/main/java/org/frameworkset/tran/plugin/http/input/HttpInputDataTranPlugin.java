package org.frameworkset.tran.plugin.http.input;
/**
 * Copyright 2022 bboss
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
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.elasticsearch.template.BaseTemplateMeta;
import org.frameworkset.elasticsearch.template.DSLParserException;
import org.frameworkset.elasticsearch.template.TemplateMeta;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.http.HttpConfigClientProxy;
import org.frameworkset.tran.plugin.http.HttpProxyHelper;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpInputDataTranPlugin extends BasePlugin implements InputPlugin {
	private HttpInputConfig httpInputConfig;
	private ResourceStartResult resourceStartResult ;
	private HttpConfigClientProxy httpConfigClientProxy ;

	public HttpInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
		httpInputConfig = (HttpInputConfig) importContext.getInputConfig();
	}

	@Override
	public void initStatusTableId() {
		if(dataTranPlugin.isIncreamentImport()) {
			if(SimpleStringUtil.isNotEmpty(httpInputConfig.getQueryDsl())){
				//计算增量记录id
				importContext.setStatusTableId((httpInputConfig.getQueryDsl()+"$$"+httpInputConfig.getQueryUrl()).hashCode());
			}

			else{
				//计算增量记录id
				importContext.setStatusTableId((httpInputConfig.getDslFile()+"$$"+httpInputConfig.getQueryDslName() +"$$"+httpInputConfig.getQueryUrl() ).hashCode());
			}
		}

	}

	@Override
	public String getLastValueVarName(){
		return super.getLastValueVarName();
	}

	@Override
	public void doImportData(TaskContext taskContext) throws DataImportException {
		try {
			if (!importContext.isIncreamentImport()) {

				commonImportData(   taskContext );

			} else {

				increamentImportData(   taskContext );

			}
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw new DataImportException(e);
		}
	}

	interface QueryAction{
		public HttpResult<Map> execute();
		public boolean hasMore();

	}

	private void exportData(final Map params, TaskContext taskContext){
		QueryAction queryAction = new QueryAction(){
			private int from;
			private boolean hasMore ;
			@Override
			public HttpResult<Map> execute() {
				if(httpInputConfig.isPagine()){
					params.put(HttpInputConfig.pagineFromKey,from);//从0开始
					int pageSize = httpInputConfig.getPageSize();
					params.put(HttpInputConfig.pagineSizeKey,pageSize);
				}
				HttpResult<Map> httpResult =  null;
				if(httpInputConfig.getHttpMethod() == null || httpInputConfig.getHttpMethod().equals("post")) {
					httpResult = httpConfigClientProxy.sendBodyForList(httpInputConfig, httpInputConfig.getSourceHttpPool(),
							httpInputConfig.getQueryUrl(), httpInputConfig.getQueryDslName(), params, Map.class);
				}
				else if( httpInputConfig.getHttpMethod().equals("put")) {
					httpResult = httpConfigClientProxy.putBodyForList(httpInputConfig, httpInputConfig.getSourceHttpPool(),
							httpInputConfig.getQueryUrl(), httpInputConfig.getQueryDslName(), params, Map.class);
				}
				if(httpInputConfig.isPagine()) {
					if(httpResult.size() == httpInputConfig.getPageSize()) {
						hasMore = true;
						from = from + httpInputConfig.getPageSize();
					}
					else{
						hasMore = false;
					}

				}

				return httpResult;
			}

			@Override
			public boolean hasMore() {
				return hasMore;
			}

		};


		doTran( queryAction,  taskContext);


	}
	private void commonImportData( TaskContext taskContext) throws Exception {

		Map params = dataTranPlugin.getJobParams();

		exportData(  params, taskContext);
		/**
		 * JDBCResultSet jdbcResultSet = new JDBCResultSet();
		 * 		jdbcResultSet.setResultSet(resultSet);
		 * 		jdbcResultSet.setMetaData(statementInfo.getMeta());
		 * 		jdbcResultSet.setDbadapter(statementInfo.getDbadapter());
		 * 		DB2ESDataTran db2ESDataTran = new DB2ESDataTran(jdbcResultSet,importContext);
		 *
		 * 		db2ESDataTran.tran(  );
		 */
	}

	protected  void doTran(QueryAction queryAction, TaskContext taskContext){

		HttpTranResultset httpTranResultset = new HttpTranResultset(queryAction,importContext);
		httpTranResultset.init();
		BaseDataTran mongoDB2ESDataTran = dataTranPlugin.createBaseDataTran(taskContext,httpTranResultset,null,dataTranPlugin.getCurrentStatus());//new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
		mongoDB2ESDataTran.initTran();
		mongoDB2ESDataTran.tran();
	}
	private void increamentImportData( TaskContext taskContext) throws Exception {
		Map params = dataTranPlugin.getJobParams();
		params = dataTranPlugin.getParamValue(params);

		exportData(  params, taskContext);
	}

	@Override
	public void afterInit() {
		if(SimpleStringUtil.isNotEmpty(httpInputConfig.getQueryDsl())) {
			httpConfigClientProxy = HttpProxyHelper.getHttpConfigClientProxy(new BaseTemplateContainerImpl(httpInputConfig.getDslNamespace()) {
				@Override
				protected Map<String, TemplateMeta> loadTemplateMetas(String namespace) {
					try {
						BaseTemplateMeta baseTemplateMeta = new BaseTemplateMeta();
						baseTemplateMeta.setName(httpInputConfig.getQueryDslName());
						baseTemplateMeta.setNamespace(namespace);
						baseTemplateMeta.setDslTemplate(httpInputConfig.getQueryDsl());
						baseTemplateMeta.setMultiparser(true);
						Map<String, TemplateMeta> templateMetaMap = new LinkedHashMap<>();
						templateMetaMap.put(baseTemplateMeta.getName(), baseTemplateMeta);
						return templateMetaMap;
					} catch (Exception e) {
						throw new DSLParserException(e);
					}
				}

				@Override
				protected long getLastModifyTime(String namespace) {
					return -1;
				}
			});
		}
		else{
			httpConfigClientProxy = HttpProxyHelper.getHttpConfigClientProxy(httpInputConfig.getDslFile());
		}
	}

	@Override
	public void beforeInit() {

	}

	@Override
	public void init() {
		if(httpInputConfig != null && httpInputConfig.getHttpConfigs() != null){
			resourceStartResult = HttpRequestProxy.startHttpPools(httpInputConfig.getHttpConfigs());
		}
	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(resourceStartResult != null){
			HttpRequestProxy.stopHttpClients(resourceStartResult);


		}
	}
}
