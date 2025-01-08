package org.frameworkset.tran.plugin.http.output;
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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.template.BaseTemplateContainerImpl;
import org.frameworkset.elasticsearch.template.BaseTemplateMeta;
import org.frameworkset.elasticsearch.template.DSLParserException;
import org.frameworkset.elasticsearch.template.TemplateMeta;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.http.HttpConfigClientProxy;
import org.frameworkset.tran.plugin.http.HttpProxyHelper;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class HttpOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	protected HttpOutputConfig httpOutputConfig ;
	private ResourceStartResult resourceStartResult ;

	private HttpConfigClientProxy httpConfigClientProxy ;
	public HttpOutputDataTranPlugin(OutputConfig pluginOutputConfig, ImportContext importContext){
		super(  pluginOutputConfig,importContext);
		httpOutputConfig = (HttpOutputConfig)   pluginOutputConfig;

	}
    @Override
    public String getJobType(){
        return "HttpOutputDataTranPlugin";
    }
	public HttpOutputConfig getHttpOutputConfig() {
		return httpOutputConfig;
	}


	public HttpConfigClientProxy getHttpConfigClientProxy() {
		return httpConfigClientProxy;
	}

	@Override
	public void afterInit() {
		if(!httpOutputConfig.isDirectSendData()){
			if(SimpleStringUtil.isNotEmpty(httpOutputConfig.getDataDsl())) {
				httpConfigClientProxy = HttpProxyHelper.getHttpConfigClientProxy(new BaseTemplateContainerImpl(httpOutputConfig.getDslNamespace()) {
					@Override
					protected Map<String, TemplateMeta> loadTemplateMetas(String namespace) {
						try {
							BaseTemplateMeta baseTemplateMeta = new BaseTemplateMeta();
							baseTemplateMeta.setName(httpOutputConfig.getDataDslName());
							baseTemplateMeta.setNamespace(namespace);
							baseTemplateMeta.setDslTemplate(httpOutputConfig.getDataDsl());
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
				httpConfigClientProxy = HttpProxyHelper.getHttpConfigClientProxy(httpOutputConfig.getDslFile());
			}
		}
	}

	@Override
	public void beforeInit() {

	}


	@Override
	public void init() {
		if(httpOutputConfig != null && httpOutputConfig.getHttpConfigs() != null){
			resourceStartResult = HttpRequestProxy.startHttpPools(httpOutputConfig.getHttpConfigs());
		}
	}

	@Override
	public void destroy(boolean waitTranStop) {
		if(resourceStartResult != null){
			HttpRequestProxy.stopHttpClients(resourceStartResult);


		}
	}






	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		BaseDataTran db2ESDataTran = new HttpOutPutDataTran(  taskContext,   tranResultSet,   importContext,   countDownLatch,   currentStatus);
		db2ESDataTran.initTran();
		return db2ESDataTran;
	}

    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    @Override
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        BaseDataTran db2ESDataTran = new HttpOutPutDataTran(  baseDataTran);
        return db2ESDataTran;
    }
}
