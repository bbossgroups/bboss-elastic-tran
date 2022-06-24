package org.frameworkset.tran.plugin.es;
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
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.boot.ElasticSearchBoot;
import org.frameworkset.elasticsearch.boot.ElasticsearchBootResult;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.ESConfig;
import org.frameworkset.tran.plugin.BasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/20
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseESPlugin extends BasePlugin {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());

	protected ESConfig esConfig;
	public BaseESPlugin(ImportContext importContext) {
		super(importContext);
	}
	private ElasticsearchBootResult elasticsearchBootResult ;
	protected void initES(String applicationPropertiesFile){
		if(SimpleStringUtil.isNotEmpty(applicationPropertiesFile ))
			elasticsearchBootResult = ElasticSearchBoot.boot(applicationPropertiesFile);
		if(esConfig != null){
			ElasticsearchBootResult _elasticsearchBootResult = ElasticSearchBoot.boot(esConfig.getConfigs());
			if(_elasticsearchBootResult != null){
				if(this.elasticsearchBootResult == null)
					this.elasticsearchBootResult = _elasticsearchBootResult;
				else{
					this.elasticsearchBootResult.addInitedElasticsearchs(_elasticsearchBootResult.getInitedElasticsearchs());
				}
			}
		}
	}

	/**
	 * 停止采集作业内部启动的Elasticsearch数据源
	 */
	protected void stopES(){
		if(elasticsearchBootResult != null && elasticsearchBootResult.getInitedElasticsearchs() != null){
			List<String> initedElasticsearchs = elasticsearchBootResult.getInitedElasticsearchs();
			if(initedElasticsearchs != null && initedElasticsearchs.size() > 0 ){
				ElasticSearchHelper.stopElasticsearchs(initedElasticsearchs);
			}
		}
	}
	public void destroy(boolean waitTranStop) {
		this.stopES();
	}

}
