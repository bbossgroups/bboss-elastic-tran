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

import org.frameworkset.spi.feishu.FeishuHelper;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class FeishuTaskCommandImpl extends BaseTaskCommand< String> {
	private FeishuTableOutputConfig feishuTableOutputConfig ;


    private Logger logger = LoggerFactory.getLogger(FeishuTaskCommandImpl.class);

    public FeishuTaskCommandImpl(TaskCommandContext taskCommandContext, OutputConfig customOutputConfig) {
        super(  customOutputConfig,taskCommandContext);
        this.feishuTableOutputConfig = (FeishuTableOutputConfig) customOutputConfig;
        if(this.taskContext == null) {
            this.taskContext = new TaskContext(importContext);
            taskCommandContext.setTaskContext(taskContext);
        }
    }

    public String execute(){
        if(records != null && records.size() > 0) {
            FeishuHelper feishuHelper = feishuTableOutputConfig.getFeishuHelper();
            String batchInsertUrl = feishuTableOutputConfig.getBatchInsertUrl();//"/open-apis/bitable/v1/apps/F1JHb8MOjaZAYPsvhNkcLS3Kn7b/tables/tbl7ATXDjC6yF7JY/records/batch_create";
            String batchUpdateUrl = feishuTableOutputConfig.getBatchUpdateUrl();//"/open-apis/bitable/v1/apps/F1JHb8MOjaZAYPsvhNkcLS3Kn7b/tables/tbl7ATXDjC6yF7JY/records/batch_update";
            String batchDeleteUrl = feishuTableOutputConfig.getBatchDeleteUrl();//"/open-apis/bitable/v1/apps/F1JHb8MOjaZAYPsvhNkcLS3Kn7b/tables/tbl7ATXDjC6yF7JY/records/batch_delete";
            List<Map<String,Object>> insertRecords = null;
            List<Map<String,Object>> updateRecords = null;
            List<String> deleteRecords = null;
           
            
//            String accessToken = feishuTableOutputConfig.getAccessToken(taskContext, feishuTableOutputConfig.getAccessTokenKey());
//            if(accessToken == null){
//                accessToken = feishuHelper.getTenantAccessToken();
//            }
            String recordIdFieldName = feishuTableOutputConfig.getRecordIdFieldName();
            for(CommonRecord record:records){
               
                Map<String,Object> data = record.getDatas();
                if(!record.isDelete()) {
                    
                    Map feishuDataItem = new LinkedHashMap();
                    List<CellMapping> cellMappings = feishuTableOutputConfig.getSimpleCellMappingList();
                    //组装飞书表格的数据格式
                    for(CellMapping cellMapping : cellMappings){
                        feishuDataItem.put(cellMapping.getTargetField(), data.get(cellMapping.getFieldName()));
                    }


                    if (!record.isUpdate()) {
                        Map feishuData = new LinkedHashMap();
                        feishuData.put("fields", feishuDataItem);
                        if(insertRecords == null){
                            insertRecords = new ArrayList<>();
                        }
                        insertRecords.add(feishuData);
                    } else {
                        
                        
                        
                        if(updateRecords == null){
                            updateRecords = new ArrayList<>();
                        }
                        Object recordIds = data.get(recordIdFieldName);
                        if(recordIds != null){
                            if(recordIds instanceof String){
                                Map feishuData = new LinkedHashMap();
                                feishuData.put("fields", feishuDataItem);
                                feishuData.put("record_id", recordIds);
                                updateRecords.add(feishuData);
                            }
                            else if (recordIds instanceof List){
                                List<String> ids = (List<String>) recordIds;
                                String id = null;
                                Map feishuData = null;
                                for(int i = 0; i < ids.size(); i ++){
                                    feishuData = new LinkedHashMap();
                                    feishuData.put("fields", feishuDataItem);
                                    id = ids.get(i);
                                    feishuData.put("record_id", id);
                                    updateRecords.add(feishuData);                                        
                                }

                            }
                        }
                        
                    }
                               
                }
                else {
                    if(deleteRecords == null){
                        deleteRecords = new ArrayList<>();
                    }
                    Object recordIds = data.get(recordIdFieldName);
                    if(recordIds instanceof String){
                        deleteRecords.add((String) recordIds);
                    }
                    else if (recordIds instanceof List){
                        List<String> ids = (List<String>) recordIds;
                        deleteRecords.addAll(ids);

                    }
                }


            }
            if(updateRecords != null && updateRecords.size() > 0) {
                Map<String,Object> updateRequestData = new LinkedHashMap<>();
                updateRequestData.put("records",updateRecords);
                feishuHelper.sendRequest(  updateRequestData,batchUpdateUrl);
            }
            if(insertRecords != null && insertRecords.size() > 0) {
                Map<String, Object> requestData = new LinkedHashMap<>();
                requestData.put("records", insertRecords);
                feishuHelper.sendRequest(  requestData, batchInsertUrl);
            }
            if(deleteRecords != null && deleteRecords.size() > 0) {
                Map<String, Object> deleteRequestData = new LinkedHashMap<>();
                deleteRequestData.put("records", deleteRecords);
                feishuHelper.sendRequest(  deleteRequestData, batchDeleteUrl);
            }
        }
        else{

            logNodatas( logger);
        }
        finishTask();
        return null;
    }



}
