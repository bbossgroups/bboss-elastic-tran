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
import org.frameworkset.tran.plugin.http.DynamicHeaderContext;
import org.frameworkset.tran.plugin.http.HttpConfigClientProxy;
import org.frameworkset.tran.plugin.http.HttpProxyHelper;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;
import org.frameworkset.util.concurrent.ThreadPoolFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpInputDataTranPlugin extends BasePlugin implements InputPlugin {
	protected String jobType ;
	private HttpInputConfig httpInputConfig;
	private ResourceStartResult resourceStartResult ;
	private HttpConfigClientProxy httpConfigClientProxy ;
    private ExecutorService blockedExecutor;

	public HttpInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
		httpInputConfig = (HttpInputConfig) importContext.getInputConfig();
		this.jobType = "HttpInputDataTranPlugin";
	}

	@Override
	public String getJobType() {
		return jobType;
	}
	public HttpInputConfig getHttpInputConfig() {
		return httpInputConfig;
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


	private void exportData(final Map params, final TaskContext taskContext){
		QueryAction queryAction = buildQueryAction( params, taskContext);


		doTran( queryAction,  taskContext);


	}

    private QueryAction buildQueryAction(Map params,TaskContext taskContext){
        QueryAction queryAction = new QueryAction() {
            private int from;
            private boolean hasMore;

            @Override
            public HttpResult<Map> execute() {
                if (httpInputConfig.isPagine()) {
                    params.put(httpInputConfig.getPagineFromKey(), from);//从0开始
                    int pageSize = httpInputConfig.getPageSize();
                    params.put(httpInputConfig.getPagineSizeKey(), pageSize);
                }
                HttpResult<Map> httpResult = null;
                HttpResultParserContext httpResultParserContext = null;
                if (httpInputConfig.getHttpResultParser() != null) {
                    httpResultParserContext = new HttpResultParserContext();
                    httpResultParserContext.setImportContext(importContext);
                    httpResultParserContext.setTaskContext(taskContext);
                }
                DynamicHeaderContext dynamicHeaderContext = null;
                if (httpInputConfig.getDynamicHeaders() != null) {
                    dynamicHeaderContext = new DynamicHeaderContext();
//					dynamicHeaderContext.setDatas(datas);
                    dynamicHeaderContext.setTaskContext(taskContext);
                    dynamicHeaderContext.setImportContext(importContext);
                }
                if (httpInputConfig.isPostMethod()) {
                    if (httpInputConfig.isDslSetted()) {
                        httpResult = httpConfigClientProxy.sendBodyForList(HttpInputDataTranPlugin.this, httpResultParserContext, dynamicHeaderContext,
                                params, Map.class);
                    } else {
                        httpResult = httpConfigClientProxy.postForList(HttpInputDataTranPlugin.this, httpResultParserContext, dynamicHeaderContext,
                                params, Map.class);
                    }
                } else if (httpInputConfig.isPutMethod()) {
                    if (httpInputConfig.isDslSetted()) {
                        httpResult = httpConfigClientProxy.putBodyForList(HttpInputDataTranPlugin.this, httpResultParserContext, dynamicHeaderContext, params, Map.class);
                    } else {
                        httpResult = httpConfigClientProxy.putForList(HttpInputDataTranPlugin.this, httpResultParserContext, dynamicHeaderContext, params, Map.class);
                    }
                } else if (httpInputConfig.isGetMethod()) {
                    httpResult = httpConfigClientProxy.httpGetforList(HttpInputDataTranPlugin.this, httpResultParserContext, dynamicHeaderContext, params, Map.class);
                }
                if (httpInputConfig.isPagine()) {
                    if (httpResult.size() == httpInputConfig.getPageSize()) {
                        hasMore = true;
                        from = from + httpInputConfig.getPageSize();
                    } else {
                        hasMore = false;
                    }

                }
                if(httpResult != null){
                    httpResult.setQueryAction(this);
                }
                return httpResult;
            }

            @Override
            public boolean hasMore() {
                return hasMore;
            }

        };
        return queryAction;
    }

    private void parrelExportData(final List<Map> paramGroups, final TaskContext taskContext){
        List<QueryAction> queryActions = new ArrayList<>(paramGroups.size());
        for(Map params:paramGroups) {

            QueryAction queryAction = buildQueryAction( params, taskContext);

            queryActions.add(queryAction);
        }
        doParrelTran( queryActions, taskContext);
    }
	private void commonImportData( TaskContext taskContext) throws Exception {
        //单次请求
        if (!dataTranPlugin.hasJobInputParamGroups()){
            Map params = dataTranPlugin.getJobInputParams(taskContext);
            exportData(params, taskContext);
        }
        else{ //并行查询
            List<Map> paramGroups = dataTranPlugin.getJobInputParamGroups(taskContext);
            parrelExportData(paramGroups,taskContext);
        }


	}
    protected  void doParrelTran(List<QueryAction> queryActions, TaskContext taskContext){

        ParrelHttpTranResultset httpTranResultset = new ParrelHttpTranResultset(queryActions,importContext,blockedExecutor);
        httpTranResultset.init();
        BaseDataTran httpDataTran = dataTranPlugin.createBaseDataTran(taskContext,httpTranResultset,null,dataTranPlugin.getCurrentStatus());
        httpDataTran.initTran();
        dataTranPlugin.callTran( httpDataTran);
    }
	protected  void doTran(QueryAction queryAction, TaskContext taskContext){

		HttpTranResultset httpTranResultset = new HttpTranResultset(queryAction,importContext);
		httpTranResultset.init();
		BaseDataTran httpDataTran = dataTranPlugin.createBaseDataTran(taskContext,httpTranResultset,null,dataTranPlugin.getCurrentStatus());//new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
		httpDataTran.initTran();
        dataTranPlugin.callTran( httpDataTran);
	}
	private void increamentImportData( TaskContext taskContext) throws Exception {


        //单次请求
        if (!dataTranPlugin.hasJobInputParamGroups()){
            Map params = dataTranPlugin.getJobInputParams(taskContext);
            params = dataTranPlugin.getParamValue(params);
            exportData(  params, taskContext);
        }
        else{ //并行查询
            List<Map> paramGroups = dataTranPlugin.getJobInputParamGroups(taskContext);
            for(Map params:paramGroups){
                dataTranPlugin.getParamValue(params);
            }
            parrelExportData(paramGroups,taskContext);
        }
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

        if(dataTranPlugin.hasJobInputParamGroups()){
            blockedExecutor = ThreadPoolFactory.buildThreadPool("HttpInputQueryThread","HttpInputQueryThread",
                    httpInputConfig.getQueryThread(),httpInputConfig.getQueryThreadQueue(),
                    -1l
                    ,1000);
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
        if(blockedExecutor != null){
            ThreadPoolFactory.shutdownExecutor(blockedExecutor);
        }
	}
}
