package org.frameworkset.tran.plugin.es.output;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.es.BaseESPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class ElasticsearchOutputDataTranPlugin extends BaseESPlugin implements OutputPlugin {
	protected ElasticsearchOutputConfig elasticsearchOutputConfig ;
	public ElasticsearchOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
		elasticsearchOutputConfig = (ElasticsearchOutputConfig) importContext.getOutputConfig();

	}

	@Override
	public void afterInit() {

	}


	@Override
	public void beforeInit() {
		this.esConfig = elasticsearchOutputConfig.getEsConfig();
		this.applicationPropertiesFile = importContext.getApplicationPropertiesFile();
		this.initES();


	}

	@Override
	public void init() {

	}




	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		BaseDataTran db2ESDataTran = null;
		if(countDownLatch == null) {
			db2ESDataTran = new BaseElasticsearchDataTran(taskContext, tranResultSet, importContext, currentStatus);
		}
		else {
			db2ESDataTran = new AsynESOutPutDataTran(taskContext, tranResultSet, importContext,
					elasticsearchOutputConfig.getTargetElasticsearch(), countDownLatch, currentStatus);
		}
		db2ESDataTran.initTran();

		return db2ESDataTran;
	}


}
