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

import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.db.output.JDBCGetVariableValue;
import org.frameworkset.tran.plugin.es.BaseESPlugin;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.annotations.DateFormateMeta;

import java.util.Date;

import static org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig.SPECIALCONFIG_JDBCGETVARIABLEVALUE_NAME;


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
	public ElasticsearchOutputDataTranPlugin(OutputConfig pluginOutputConfig, ImportContext importContext){
		super(  pluginOutputConfig, importContext);
		elasticsearchOutputConfig = (ElasticsearchOutputConfig) pluginOutputConfig;

	}
    @Override
    public String getJobType(){
        return "ElasticsearchOutputDataTranPlugin";
    }
	@Override
	public void afterInit() {
        initClientInterfaces( );
	}
    @Override
    public RecordColumnInfo resolveRecordColumnInfo(Object value, FieldMeta fieldMeta, Context context){
        RecordColumnInfo recordColumnInfo = null;
        if (value != null && value instanceof Date){
            DateFormateMeta dateFormateMeta = null;
            if(fieldMeta != null){
                dateFormateMeta = fieldMeta.getDateFormateMeta();

            }
            if(dateFormateMeta == null)
                dateFormateMeta = context.getDateFormateMeta();
            recordColumnInfo = new RecordColumnInfo();
            recordColumnInfo.setDateTag(true);
            recordColumnInfo.setDateFormateMeta(dateFormateMeta);
        }
        return recordColumnInfo;
    }

    @Override
    public void buildRecordOutpluginSpecialConfig(CommonRecord dataRecord, Context context) throws Exception {
        
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = context.getRecordSpecialConfigsContext().getRecordOutpluginSpecialConfig(this);
        if(recordOutpluginSpecialConfig != null) {
            JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(dataRecord, context.getBatchContext());
            recordOutpluginSpecialConfig.addRecordSpecialConfigOnly(SPECIALCONFIG_JDBCGETVARIABLEVALUE_NAME, jdbcGetVariableValue);
        }
         
    }
//    @Override
//    public CommonRecord buildRecord(Context context) throws Exception {
//        ElasticsearchCommonRecord elasticsearchCommonRecord = new ElasticsearchCommonRecord();
//
//
//        super.buildRecord(elasticsearchCommonRecord,context);
//        elasticsearchCommonRecord.setEsId(context.getEsId());
//        elasticsearchCommonRecord.setParentId(context.getParentId());
//        elasticsearchCommonRecord.setRouting(context.getRouting());
//        ClientOptions clientOptions = context.getClientOptions();
//        elasticsearchCommonRecord.setClientOptions(clientOptions);
//        elasticsearchCommonRecord.setEsIndexWrapper(context.getESIndexWrapper());
//        JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(elasticsearchCommonRecord,context.getBatchContext());
//        elasticsearchCommonRecord.setJdbcGetVariableValue(jdbcGetVariableValue);
//        String operation = null;
//        if(context.isInsert() ){
//            operation = "index";
//        }
//        else if(context.isUpdate()){
//            operation = "update";
//        }
//        else if(context.isDelete()){
//            operation = "delete";
//        }
//        else{
//            operation = "index";
//        }
//        elasticsearchCommonRecord.setOperation(operation);
//        return elasticsearchCommonRecord;
//
//    }
//    

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
			db2ESDataTran = new AsynESOutPutDataTran(taskContext, tranResultSet, importContext, countDownLatch, currentStatus);
		}
		db2ESDataTran.initTran();

		return db2ESDataTran;
	}
    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        BaseCommonRecordDataTran dummyOutPutDataTran = new AsynESOutPutDataTran(baseDataTran);
        return dummyOutPutDataTran;
    }

    private ClientInterface[] clientInterfaces;
    private void initClientInterfaces( ){
        if(clientInterfaces == null) {
            String targetElasticsearch = elasticsearchOutputConfig.getTargetElasticsearch();
            if (targetElasticsearch != null) {
                String[] _elasticsearchs = targetElasticsearch.split(",");
                clientInterfaces = new ClientInterface[_elasticsearchs.length];
                for (int i = 0; i < _elasticsearchs.length; i++) {
                    clientInterfaces[i] = ElasticSearchHelper.getRestClientUtil(_elasticsearchs[i]);
                }

            } else {
                clientInterfaces = new ClientInterface[1];
                clientInterfaces[0] = ElasticSearchHelper.getRestClientUtil("default");
            }
        }

    }

    public ClientInterface[] getClientInterfaces() {
//        initClientInterfaces();
        return clientInterfaces;
    }


}
