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
import org.frameworkset.nosql.milvus.*;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.metrics.output.MetricsOutputDataTran;
import org.frameworkset.tran.plugin.milvus.InitMilvusUtil;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusOutputDataTranPlugin extends BasePlugin implements OutputPlugin {

    private static Logger log = LoggerFactory.getLogger(MilvusOutputDataTranPlugin.class);
	/**
	 * 包含所有启动成功的db数据源
	 */
    protected MilvusOutputConfig milvusOutputConfig;

	private MilvusStartResult milvusStartResult ;
	public MilvusOutputDataTranPlugin(ImportContext importContext, OutputConfig outputConfig){
		super(outputConfig,importContext);
        milvusOutputConfig = (MilvusOutputConfig) outputConfig;

	}
    @Override
    public String getJobType(){
        return "MilvusOutputDataTranPlugin";
    }
	@Override
	public void afterInit() {

	}

	@Override
	public void beforeInit(){
        initMilvus();

	}


	protected void initMilvus(){
		milvusStartResult = InitMilvusUtil.initMilvus(milvusOutputConfig);
        if(milvusOutputConfig.isLoadCollectionSchema()){
            loadCollectionSchema();
        }
	}
    
    private void loadCollectionSchema(){
        
        try {
            List<String> fields = MilvusHelper.loadCollectionSchema(milvusOutputConfig.getName(),
                    milvusOutputConfig.getCollectionName());
            Map<String, Object> collectionSchemaIdx = new LinkedHashMap<>();
            for (String f : fields) {
                collectionSchemaIdx.put(f, 1);
            }
            milvusOutputConfig.setFields(fields);
            milvusOutputConfig.setCollectionSchemaIdx(collectionSchemaIdx);
            if(log.isInfoEnabled()) {
                log.info("collection {} collectionSchema {}", milvusOutputConfig.getCollectionName(), SimpleStringUtil.object2json(fields));
            }
            
        }
        catch (DataImportException dataImportException){
            throw dataImportException;
        }
        catch (Exception exception){
            throw new DataImportException("loadCollectionSchema failed:",exception);
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

    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    @Override
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        MilvusOutPutDataTran milvusOutPutDataTran = new MilvusOutPutDataTran(baseDataTran);
        return milvusOutPutDataTran;
    }


}
