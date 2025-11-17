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
import com.google.gson.*;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.service.vector.request.InsertReq;
import io.milvus.v2.service.vector.request.UpsertReq;
import io.milvus.v2.service.vector.response.InsertResp;
import io.milvus.v2.service.vector.response.UpsertResp;
import org.frameworkset.nosql.milvus.MilvusFunction;
import org.frameworkset.nosql.milvus.MilvusHelper;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.task.CommonBaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusTaskCommandImpl  extends CommonBaseTaskCommand<Object>  {
	protected MilvusOutputConfig milvusOutputConfig;
	
	private static final Logger logger = LoggerFactory.getLogger(MilvusTaskCommandImpl.class);
	public MilvusTaskCommandImpl(TaskCommandContext taskCommandContext, OutputConfig outputConfig) {
		super( taskCommandContext,outputConfig);
        milvusOutputConfig = (MilvusOutputConfig) outputConfig;
	}


    public boolean isArray(Object obj) {
        return obj != null && obj.getClass().isArray();
    }
    private List<JsonObject> _buildDatas(Map<String,Object> fidx){
        Gson gson = new Gson();
        if(SimpleStringUtil.isNotEmpty(records)){
            List<JsonObject> jsonObjects = new ArrayList<>(records.size());
            for(CommonRecord record:records) {

                Map<String, Object> datas = record.getDatas();
                Iterator<Map.Entry<String, Object>> iterator = datas.entrySet().iterator();
                JsonObject dict1 = new JsonObject();
                while (iterator.hasNext()){
                    Map.Entry<String, Object> entry = iterator.next();
                    String key  = entry.getKey();
                    if(!fidx.containsKey(key)){
                        continue;
                    }
                    Object value = entry.getValue();
                   
                    if(value == null){
                        dict1.add(key, JsonNull.INSTANCE);
                    }
                    else if(isArray(value)){

                        dict1.add(key, gson.toJsonTree(value));
                    }
                    else{
                        if(value instanceof String) {
                            dict1.addProperty(key,
                                    (String) value);
                        }
                        else if(value instanceof Number) {
                            dict1.addProperty(key,
                                    (Number) value);
                        }
                        else if(value instanceof Boolean) {
                            dict1.addProperty(key,
                                    (Boolean) value);
                        }
                        else if(value instanceof Character) {
                            dict1.addProperty(key,
                                    (Character) value);
                        }
                        else{
                            dict1.add(key, gson.toJsonTree(value));
                        }
                        
                    }
                   
                }
                jsonObjects.add(dict1);
//                JsonObject dict1 = new JsonObject();
//                List<Float> vectorArray1 = new ArrayList<>();
//                vectorArray1.add(0.37417449965222693);
//                vectorArray1.add(-0.9401784221711342);
//                vectorArray1.add(0.9197526367693833);
//                vectorArray1.add(0.49519396415367245);
//                vectorArray1.add(-0.558567588166478);
//
//                dict1.addProperty("id", 1L);
//                dict1.add("vector", gson.toJsonTree(vectorArray1));
            }
            return jsonObjects;
        }
        return null;
    }
    protected  Map<String,Object> getCollectionSchemaIdx(MilvusClientV2 milvusClientV2){
        if(milvusOutputConfig.isLoadCollectionSchema()){
            return milvusOutputConfig.getCollectionSchemaIdx();
        }
        else{
             
            List<String> fields = MilvusHelper.getCollectionSchemaIdx(milvusOutputConfig.getCollectionName(),milvusClientV2);
            Map<String,Object> fidx = new HashMap<>();
            for(String f:fields){
                fidx.put(f,1);
            }
            if(logger.isInfoEnabled()) {
                logger.info("collection {} collectionSchema {}", milvusOutputConfig.getCollectionName(), SimpleStringUtil.object2json(fields));
            }
            return fidx;
        }
    }
    
    @Override
    protected Object _execute(){
        return MilvusHelper.executeRequest(milvusOutputConfig.getName(), new MilvusFunction<Object>() {
            @Override
            public Object execute(MilvusClientV2 milvusClientV2) {
                // get the collection detail
               
                 
                Map<String,Object> collectionSchemaIdx = getCollectionSchemaIdx(  milvusClientV2);
                
                if (milvusOutputConfig.isUpsert()) {
                    UpsertReq.UpsertReqBuilder upsertReqBuilder = UpsertReq.builder();
                    upsertReqBuilder.collectionName(milvusOutputConfig.getCollectionName());
                    if (SimpleStringUtil.isNotEmpty(milvusOutputConfig.getPartitionName())) {
                        upsertReqBuilder.partitionName(milvusOutputConfig.getPartitionName());
                    }
                    upsertReqBuilder.data(_buildDatas(collectionSchemaIdx));
                    UpsertResp upsertResp = milvusClientV2.upsert(upsertReqBuilder.build());
                    return upsertResp;
                } else {
                    InsertReq.InsertReqBuilder insertReqBuilder = InsertReq.builder();
                    insertReqBuilder.collectionName(milvusOutputConfig.getCollectionName());
                    if (SimpleStringUtil.isNotEmpty(milvusOutputConfig.getPartitionName())) {
                        insertReqBuilder.partitionName(milvusOutputConfig.getPartitionName());
                    }
                    insertReqBuilder.data(_buildDatas(collectionSchemaIdx));
                    InsertResp insertResp = milvusClientV2.insert(insertReqBuilder.build());
                    return insertResp;
                }
            }
        });       
        
	}


	


}
