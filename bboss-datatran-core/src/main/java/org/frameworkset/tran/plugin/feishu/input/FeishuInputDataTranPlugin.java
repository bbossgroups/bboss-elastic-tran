package org.frameworkset.tran.plugin.feishu.input;
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

import org.frameworkset.spi.ai.mcp.feishu.FeishuHelper;
import org.frameworkset.spi.remote.http.HttpRequestProxy;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.BasePlugin;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.ResourceStartResult;

import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class FeishuInputDataTranPlugin extends BasePlugin implements InputPlugin {
	protected String jobType ;
	private FeishuTableInputConfig feishuTableInputConfig;

	public FeishuInputDataTranPlugin(ImportContext importContext) {
		super(importContext);
        feishuTableInputConfig = (FeishuTableInputConfig) importContext.getInputConfig();
		this.jobType = "FeishuInputDataTranPlugin";
	}

	@Override
	public String getJobType() {
		return jobType;
	}
	 

	@Override
	public void initStatusTableId() {
		

	}



	@Override
	public void doImportData(TaskContext taskContext) throws DataImportException {
		try {			

			commonImportData(   taskContext );
			
		} catch (DataImportException e) {
			throw e;
		} catch (Exception e) {
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
	}


	private void exportData(final Map params, final TaskContext taskContext){
        FeishuQueryAction queryAction = buildQueryAction( params, taskContext);


		doTran( queryAction,  taskContext);


	}

    private FeishuQueryAction buildQueryAction(Map params,TaskContext taskContext){
        FeishuQueryAction queryAction = new FeishuQueryAction() {
             
            private boolean hasMore;
            private String pageToken;
            
            

            private FeishuData handleItem(Map<String,Object> item){
                FieldValueConvertor  fieldValueConvertor = null;
                AllFieldValueConvertor allFieldValueConvertor = feishuTableInputConfig.getAllFieldValueConvertor();
                FeishuData feishuData = new FeishuData();
                String record_id = (String)item.get("record_id");
                feishuData.setRecordId(record_id);
                Map<String,Object> fields = (Map<String,Object>)item.get("fields");
                Map<String,Object> newfields = new LinkedHashMap<>(fields.size());
                Iterator<Map.Entry<String, Object>> iterator = fields.entrySet().iterator();
                while (iterator.hasNext()){
                    Map.Entry<String, Object> entry =  iterator.next();
                    String key =   entry.getKey();
                    Object value = entry.getValue();
                    fieldValueConvertor = feishuTableInputConfig.fieldValueConvertor(key);
                    if(fieldValueConvertor != null){
                        newfields.put(key, fieldValueConvertor.handleItem(fields, key,value));
                    }
                    else if(allFieldValueConvertor != null){
                        newfields.put(key, allFieldValueConvertor.handleItem(fields, key,value));
                    }
                    else {
                        if (value instanceof List) {
                            List v = (List) value;
                            int size = v.size();
                            if (v != null && size > 0) {
                                Object o = v.get(0);
                                
                                if(o instanceof Map){
                                    if(size == 1) {
                                        newfields.put(key, ((Map) o).get("text"));
                                    }
                                    else{
                                        List texts = new ArrayList(size);
                                        for(Object o1:v){
                                            texts.add(((Map) o1).get("text"));
                                        }
                                        newfields.put(key, texts);
                                    }
                                }
                                else{
                                    newfields.put(key, v);
                                }
                                
                            }
                        }
                        else if(value instanceof Map){
                            Map map = (Map)value;
                            Integer type = (Integer) map.get("type");
                            if(type != null){
                                //公式值处理：{"type":1,"value":[{"text":"[100%]","type":"text"}]}
                                List<Map> functionValue = (List<Map>) map.get("value");
                                if(functionValue != null && functionValue.size() > 0){
                                    newfields.put(key, functionValue.get(0).get("text"));
                                }
                                else{
                                    newfields.put(key, value);
                                }
                            }
                            else{
                                newfields.put(key, value);
                            }
                        }
                        else {
                            newfields.put(key, value);
                        }
                    }
                }
                feishuData.setFields(newfields);
                return feishuData;
            }
            @Override
            public List<FeishuData> execute() {
                String requestBody = feishuTableInputConfig.getRequestBody();
                String searchUrl = feishuTableInputConfig.getSearchUrl();
                FeishuHelper feishuHelper = feishuTableInputConfig.getFeishuHelper();
                String accessToken = feishuTableInputConfig.getAccessToken(taskContext,feishuTableInputConfig.getAccessTokenKey());
                if(accessToken == null){
                    accessToken = feishuHelper.getTenantAccessToken();
                }
                if(pageToken != null){
                    searchUrl = searchUrl + "&page_token=" + pageToken;
                }
                List<FeishuData> feishuTableResult = null;
                Map datas = feishuHelper.searchData(accessToken,searchUrl,  requestBody);
                if(datas != null ){
                    int code = (Integer)datas.get("code");
                    if(code != 0){
                        String msg = (String)datas.get("msg");
                        throw new DataImportException(msg);
                    }
                    Map data = (Map)datas.get("data");
                    if(data != null) {
                        List<Map> items = (List<Map>) data.get("items");
                        
                        if (items != null && items.size() > 0) {
                            List<FeishuData> feishuDataList = new ArrayList<FeishuData>();
                            for(Map item:items){
                                FeishuData feishuData = handleItem(item);
                                feishuDataList.add(feishuData);
                            }
                            feishuTableResult = feishuDataList;
                        }
                        hasMore = (boolean) data.get("has_more");
                        pageToken = (String) data.get("page_token");
                    }
                    else{
                        
                    }
                }
                
                
                return feishuTableResult;
            }

            @Override
            public boolean hasMore() {
                return hasMore;
            }

        };
        return queryAction;
    }

    
	private void commonImportData( TaskContext taskContext) throws Exception {
        //单次请求
         
        Map params = dataTranPlugin.getJobInputParams(taskContext);
        exportData(params, taskContext);
        
        


	}
 
	protected  void doTran(FeishuQueryAction queryAction, TaskContext taskContext){

        FeishuTranResultset feishuTranResultset = new FeishuTranResultset(queryAction,importContext);
		feishuTranResultset.init();
		BaseDataTran httpDataTran = dataTranPlugin.createBaseDataTran(taskContext,feishuTranResultset,null,dataTranPlugin.getCurrentStatus());//new BaseElasticsearchDataTran( taskContext,mongoDB2ESResultSet,importContext,targetImportContext,this.currentStatus);
		httpDataTran.initTran();
        dataTranPlugin.callTran( httpDataTran);
	}
	 

	@Override
	public void afterInit() {
		 
	}

	@Override
	public void beforeInit() {

	}

	@Override
	public void init() {
        feishuTableInputConfig.initFeishHelper();
//		if(feishuTableInputConfig != null && feishuTableInputConfig.getHttpConfigs() != null){
//			resourceStartResult = HttpRequestProxy.startHttpPools(feishuTableInputConfig.getHttpConfigs());
//		}
	}

	@Override
	public void destroy(boolean waitTranStop) {
        feishuTableInputConfig.destroy();
        
	}
}
