package org.frameworkset.tran.plugin.milvus.output;
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
import org.frameworkset.nosql.milvus.MilvusConfig;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.nosql.milvus.MilvusStartResult;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
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
public class MilvusOutputDataTranPlugin extends BasePlugin implements OutputPlugin {
	/**
	 * 包含所有启动成功的db数据源
	 */
    protected MilvusOutputConfig milvusOutputConfig;

	private MilvusStartResult milvusStartResult = new MilvusStartResult();
	public MilvusOutputDataTranPlugin(ImportContext importContext){
		super(importContext);
        milvusOutputConfig = (MilvusOutputConfig) importContext.getOutputConfig();

	}

	@Override
	public void afterInit() {

	}

	@Override
	public void beforeInit(){
        initMilvus();

	}


	protected void initMilvus(){
		MilvusConfig milvusConfig = new MilvusConfig();
        milvusConfig.setName(milvusOutputConfig.getName());
        if(SimpleStringUtil.isNotEmpty(milvusOutputConfig.getUri())) {
            milvusConfig.setUri(milvusOutputConfig.getUri());
            milvusConfig.setToken(milvusOutputConfig.getToken());
            milvusConfig.setMaxIdlePerKey(milvusOutputConfig.getMaxIdlePerKey());
            milvusConfig.setMinIdlePerKey(milvusOutputConfig.getMinIdlePerKey());
            milvusConfig.setMaxTotalPerKey(milvusOutputConfig.getMaxTotalPerKey());
            milvusConfig.setMaxTotal(milvusOutputConfig.getMaxTotal());
            milvusConfig.setBlockWhenExhausted(milvusOutputConfig.getBlockWhenExhausted());
            milvusConfig.setMaxBlockWaitDuration(milvusOutputConfig.getMaxBlockWaitDuration());
            milvusConfig.setMinEvictableIdleDuration(milvusOutputConfig.getMinEvictableIdleDuration());
            milvusConfig.setEvictionPollingInterval(milvusOutputConfig.getEvictionPollingInterval());
            milvusConfig.setTestOnBorrow(milvusOutputConfig.getTestOnBorrow());
            milvusConfig.setTestOnReturn(milvusOutputConfig.getTestOnReturn());
            milvusConfig.setDbName(milvusOutputConfig.getDbName());

            milvusConfig.setConnectTimeoutMs(milvusOutputConfig.getConnectTimeoutMs());
            milvusConfig.setIdleTimeoutMs(milvusOutputConfig.getIdleTimeoutMs());
            milvusConfig.setCustomConnectConfigBuilder(milvusOutputConfig.getCustomConnectConfigBuilder());


            milvusStartResult = MilvusHelper.init(milvusConfig);
            
        }
	}


	@Override
	public void init() {

	}

	@Override

	public void destroy(boolean waitTranStop) {
        if(milvusStartResult != null)
            MilvusHelper.shutdown(milvusStartResult) ;
	}

	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		if(countDownLatch == null) {
			MilvusOutPutDataTran milvusOutPutDataTran = new MilvusOutPutDataTran(taskContext, tranResultSet, importContext, currentStatus);
            milvusOutPutDataTran.initTran();
			return milvusOutPutDataTran;
		}
		else{
            MilvusOutPutDataTran milvusOutPutDataTran = new MilvusOutPutDataTran(  taskContext,tranResultSet,importContext,    currentStatus,countDownLatch);
            milvusOutPutDataTran.initTran();
			return milvusOutPutDataTran;
		}
	}




}
