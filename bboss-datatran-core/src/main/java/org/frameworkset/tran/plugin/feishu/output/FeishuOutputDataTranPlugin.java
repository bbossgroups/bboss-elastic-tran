package org.frameworkset.tran.plugin.feishu.output;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
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
public class FeishuOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	protected FeishuTableOutputConfig feishuTableOutputConfig ;
   

	public FeishuOutputDataTranPlugin(OutputConfig pluginOutputConfig, ImportContext importContext){
		super(  pluginOutputConfig,importContext);
        feishuTableOutputConfig = (FeishuTableOutputConfig)   pluginOutputConfig;
        

	}

    @Override
    public void afterInit() {
        
    }

    @Override
    public String getJobType(){
        return "FeishuOutputDataTranPlugin";
    }

    public FeishuTableOutputConfig getFeishuTableOutputConfig() {
        return feishuTableOutputConfig;
    }

 
    
     

	@Override
	public void beforeInit() {

	}


	@Override
	public void init() {
//		if(feishuTableOutputConfig != null && feishuTableOutputConfig.getHttpConfigs() != null){
//            FeishuHelper feishuHelper = new FeishuHelper(feishuTableOutputConfig
//                    );
//			resourceStartResult = HttpRequestProxy.startHttpPools(feishuTableOutputConfig.getHttpConfigs());
//             
//		}
        feishuTableOutputConfig.initFeishHelper();
	}

	@Override
	public void destroy(boolean waitTranStop) {
        feishuTableOutputConfig.destroy();
       
	}






	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		BaseDataTran feishuOutPutDataTran = new FeishuOutPutDataTran(  taskContext,   tranResultSet,   importContext,   countDownLatch,   currentStatus);
		feishuOutPutDataTran.initTran();
		return feishuOutPutDataTran;
	}

    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    @Override
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        BaseDataTran db2ESDataTran = new FeishuOutPutDataTran(  baseDataTran);
        return db2ESDataTran;
    }
}
