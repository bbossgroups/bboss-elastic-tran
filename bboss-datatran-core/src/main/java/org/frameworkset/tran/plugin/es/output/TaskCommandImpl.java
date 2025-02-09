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

import com.frameworkset.common.poolman.handle.ValueExchange;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.bulk.BulkConfig;
import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.client.ClientUtil;
import org.frameworkset.elasticsearch.handler.ESVoidResponseHandler;
import org.frameworkset.elasticsearch.serial.CharEscapeUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.elasticsearch.template.ConfigDSLUtil;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.db.output.JDBCGetVariableValue;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputDataTranPlugin;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskCommandImpl extends BaseTaskCommand< String> {
	private ElasticsearchOutputConfig elasticsearchOutputConfig;
	public TaskCommandImpl(TaskCommandContext taskCommandContext, ElasticsearchOutputConfig elasticsearchOutputConfig 
                           ) {
		super(  elasticsearchOutputConfig,taskCommandContext);
		this.elasticsearchOutputConfig = elasticsearchOutputConfig;
        ElasticsearchOutputDataTranPlugin elasticsearchOutputDataTranPlugin = (ElasticsearchOutputDataTranPlugin) elasticsearchOutputConfig.getOutputPlugin();
        this.clientInterfaces = elasticsearchOutputDataTranPlugin.getClientInterfaces();
        if(clientInterfaces != null && clientInterfaces.length > 0)
            versionUpper7 = clientInterfaces[0].isVersionUpper7();
	}




	private ClientInterface[] clientInterfaces;



	public ClientInterface[] getClientInterfaces() {
		return clientInterfaces;
	}

 

	private int tryCount;
    private String datas;
    private boolean versionUpper7;
    public Object getDatas(){
        return datas;
    }
 

 


	private static Logger logger = LoggerFactory.getLogger(TaskCommand.class);
    public  void buildMeta(CommonRecord elasticsearchCommonRecord, Writer writer , boolean upper7) throws Exception {
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = elasticsearchCommonRecord.getRecordOutpluginSpecialConfig(outputConfig);
        Object id = recordOutpluginSpecialConfig.getSpecialConfig(ElasticsearchOutputConfig.SPECIALCONFIG_ESID_NAME);
        Object parentId = recordOutpluginSpecialConfig.getSpecialConfig(ElasticsearchOutputConfig.SPECIALCONFIG_PARENTID_NAME);
        Object routing = recordOutpluginSpecialConfig.getSpecialConfig(ElasticsearchOutputConfig.SPECIALCONFIG_ROUTING_NAME);
        ClientOptions clientOptions = (ClientOptions) recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_CLIENTOPTIONS_NAME);
        Object esRetryOnConflict = clientOptions != null?clientOptions.getEsRetryOnConflict():null;
        ESIndexWrapper esIndexWrapper = (ESIndexWrapper) recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_ESINDEXWRAPPER_NAME);

        JDBCGetVariableValue jdbcGetVariableValue = (JDBCGetVariableValue) recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_JDBCGETVARIABLEVALUE_NAME);
        writer.write("{ \"");
        writer.write((String)recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_OPERATION_NAME));
        writer.write("\" : { \"_index\" : \"");

        if (esIndexWrapper == null ) {
            throw ImportExceptionUtil.buildDataImportException(importContext," ESIndex not seted." );
        }
        BuildTool.buildIndiceName(esIndexWrapper,writer,jdbcGetVariableValue);

        writer.write("\"");
        if(!upper7) {
            writer.write(", \"_type\" : \"");
            if (esIndexWrapper == null ) {
                throw ImportExceptionUtil.buildDataImportException(importContext," ESIndex type not seted." );
            }
            String indexType = BuildTool.buildIndiceType(esIndexWrapper,jdbcGetVariableValue);
            if(indexType == null || indexType.equals("")){
                throw ImportExceptionUtil.buildDataImportException(importContext," ESIndex type not seted." );
            }
            writer.write(indexType);
            writer.write("\"");
        }
        if(id != null) {
            writer.write(", \"_id\" : ");
            BuildTool.buildId(id, writer, true);
        }
        if(parentId != null){
            writer.write(", \"parent\" : ");
            BuildTool.buildId(parentId,writer,true);
        }
        if(routing != null){
            if(!upper7) {
                writer.write(", \"_routing\" : ");
            }
            else{
                writer.write(", \"routing\" : ");
            }
            BuildTool.buildRouting(routing,writer,true);
        }

//			if(action.equals("update"))
//			{
        if (esRetryOnConflict != null) {
            if(!upper7) {
                writer.write(",\"_retry_on_conflict\":");
            }
            else{
                writer.write(",\"retry_on_conflict\":");
            }
            writer.write(String.valueOf(esRetryOnConflict));
        }
        Object version = recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_VERSION_NAME);

        if (version != null) {
            if(!upper7) {
                writer.write(",\"_version\":");
            }
            else{
                writer.write(",\"version\":");
            }
            writer.write(String.valueOf(version));

        }
        Object versionType = clientOptions!= null?clientOptions.getVersionType():null;
        if(versionType != null) {
            if(!upper7) {
                writer.write(",\"_version_type\":");
            }
            else{
                writer.write(",\"version_type\":");
            }
            writer.write(String.valueOf(versionType));
            writer.write("\"");
        }

        if(upper7) {
            Long if_seq_no = clientOptions != null ? clientOptions.getIfSeqNo() : null;

            if (if_seq_no != null) {

//					if(!upper7) {
//						writer.write(",\"_if_seq_no\":");
//					}
//					else{
//						writer.write(",\"if_seq_no\":");
//					}

                writer.write(",\"if_seq_no\":");

                writer.write(String.valueOf(if_seq_no));
            }

            Long if_primary_term = clientOptions != null ? clientOptions.getIfPrimaryTerm() : null;

            if (if_primary_term != null) {
//					if (!upper7) {
//						writer.write(",\"_if_primary_term\":");
//					} else {
//						writer.write(",\"if_primary_term\":");
//					}
                writer.write(",\"if_primary_term\":");
                writer.write(String.valueOf(if_primary_term));
            }
        }
//			}
        String pipeline = clientOptions!= null?clientOptions.getPipeline():null;

        if (pipeline != null) {

            writer.write(",\"pipeline\":\"");

            writer.write(pipeline);
            writer.write("\"");
        }
//		}
        if(elasticsearchCommonRecord.isInsert()){

            String op_type = clientOptions!= null?clientOptions.getOpType():null;

            if (op_type != null) {

                writer.write(",\"op_type\":\"");

                writer.write(op_type);
                writer.write("\"");
            }
        }
        writer.write(" } }\n");

    }

    public  void evalBuilk( Writer writer, CommonRecord elasticsearchCommonRecord, boolean upper7) throws Exception {

        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = elasticsearchCommonRecord.getRecordOutpluginSpecialConfig(outputConfig);
        if(elasticsearchCommonRecord.isInsert()) {
//				SerialUtil.object2json(param,writer);
            buildMeta( elasticsearchCommonRecord, writer ,       upper7);
            serialResult(  writer,elasticsearchCommonRecord);
            writer.write("\n");
        }
        else if(elasticsearchCommonRecord.isUpdate())
        {
            buildMeta( elasticsearchCommonRecord, writer ,       upper7);
            writer.write("{\"doc\":");
            serialResult(  writer,elasticsearchCommonRecord);
            ClientOptions clientOptions = (ClientOptions) recordOutpluginSpecialConfig.getSpecialConfig(SPECIALCONFIG_CLIENTOPTIONS_NAME);
            Object esDocAsUpsert = clientOptions != null?clientOptions.getDocasupsert():null;
            if(esDocAsUpsert != null){
                writer.write(",\"doc_as_upsert\":");
                writer.write(String.valueOf(esDocAsUpsert));
            }
            Object detect_noop = clientOptions != null?clientOptions.getDetectNoop():null;
            if(detect_noop != null){
                writer.write(",\"detect_noop\":");
                writer.write(detect_noop.toString());
            }
            Object esReturnSource = clientOptions != null?clientOptions.getReturnSource():null;
            if(esReturnSource != null){
                writer.write(",\"_source\":");
                writer.write(String.valueOf(esReturnSource));
            }
            List<String> sourceUpdateExcludes  = clientOptions!= null?clientOptions.getSourceUpdateExcludes():null;

            if (sourceUpdateExcludes != null) {
                /**
                 if(!upper7) {
                 writer.write(",\"_source_excludes\":");
                 }
                 else{
                 writer.write(",\"source_excludes\":");
                 }
                 */
                if(!upper7) {
                    writer.write(",\"_source_excludes\":");
                    SerialUtil.object2json(sourceUpdateExcludes,writer);
                }

            }
            List<String> sourceUpdateIncludes  = clientOptions!= null?clientOptions.getSourceUpdateIncludes():null;

            if (sourceUpdateIncludes != null) {
                /**
                 if(!upper7) {
                 writer.write(",\"_source_includes\":");
                 }
                 else{
                 writer.write(",\"source_includes\":");
                 }
                 */
                if(!upper7) {
                    writer.write(",\"_source_includes\":");
                    SerialUtil.object2json(sourceUpdateIncludes,writer);
                }


            }
            writer.write("}\n");
        }
        else if(elasticsearchCommonRecord.isDelete()){
            evalDeleteBuilk(  writer, elasticsearchCommonRecord,upper7);
        }
        else{
            buildMeta( elasticsearchCommonRecord, writer ,       upper7);
            serialResult(  writer,elasticsearchCommonRecord);
            writer.write("\n");
        }


    }

    public void evalDeleteBuilk(Writer writer,  CommonRecord elasticsearchCommonRecord,boolean isUpper7)  throws Exception{

        buildMeta(  elasticsearchCommonRecord,writer,isUpper7);

    }

    private  void serialResult( Writer writer,  CommonRecord dataRecord) throws Exception {


        writer.write("{");

        boolean hasSeted = false;
//		CommonRecord dataRecord = super.buildRecord(context);
//		CommonRecord dataRecord = context.getCommonRecord();
        Map<String,Object> datas = dataRecord.getDatas();
        Iterator<Map.Entry<String,Object>> iterator = datas.entrySet().iterator();
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = dataRecord.getRecordOutpluginSpecialConfig(outputConfig);
        while(iterator.hasNext())
        {
            Map.Entry<String,Object> entry = iterator.next();
            String colName = entry.getKey();
            if(colName.equals("_id")){
                if(logger.isDebugEnabled()){
                    logger.debug("Field [_id] is a metadata field and cannot be added inside a document. Use the index API request parameters.");
                }
                continue;
            }
//			if("ROWNUM__".equals(colName))//去掉oracle的行伪列
//				continue;

            Object value = entry.getValue();
            if(value == null && importContext.isIgnoreNullValueField()){
                continue;
            }
            if(hasSeted )
                writer.write(",");
            else
                hasSeted = true;

            writer.write("\"");
            writer.write(colName);
            writer.write("\":");
//			int colType = metaData.getColumnTypeByIndex(i);

            if(value != null) {
                
                if (value instanceof String) {
                    writer.write("\"");
                    CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
                    charEscapeUtil.writeString((String) value, true);
                    writer.write("\"");
                } else if (value instanceof Date) {
                    RecordColumnInfo recordColumnInfo = recordOutpluginSpecialConfig.getRecordColumnInfo(colName);
                    DateFormat dateFormat = recordColumnInfo.getDateFormat();

                    String dataStr = ConfigDSLUtil.getDate((Date) value,dateFormat);
                    writer.write("\"");
                    writer.write(dataStr);
                    writer.write("\"");
                }
                else if(value instanceof Clob)
                {
                    String dataStr = ValueExchange.getStringFromClob((Clob)value);
                    writer.write("\"");
                    CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
                    charEscapeUtil.writeString(dataStr, true);
                    writer.write("\"");

                }
                else if(value instanceof Blob){
                    String dataStr = ValueExchange.getStringFromBlob((Blob)value);
                    writer.write("\"");
                    CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
                    charEscapeUtil.writeString(dataStr, true);
                    writer.write("\"");
                }
                else {
                    SimpleStringUtil.object2json(value,writer);//					writer.write(String.valueOf(value));
                }
            }
            else{
                writer.write("null");
            }

        }

        writer.write("}");
    }
    private void buildDatas() throws Exception {
        if(records.size() == 0){
            return;
        }
        StringBuilder builder = new StringBuilder();
        BBossStringWriter writer = new BBossStringWriter(builder);
        CommonRecord elasticsearchCommonRecord = null;
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(int i = 0; i < records.size(); i ++){
            elasticsearchCommonRecord = records.get(i);
//            Date c = (Date)recordOutpluginSpecialConfig.getSpecialConfig("Data("collecttime");
//            logger.error("collecttime:"+dateFormat.format(c)+",logId:"+recordOutpluginSpecialConfig.getSpecialConfig("Data("logId"));
            evalBuilk(   writer, elasticsearchCommonRecord, this.versionUpper7);
        }
        this.datas = writer.toString();
        builder = null;
    }
	public String execute() throws Exception {
		String data = null;
        if(records.size() > 0){
            if(this.importContext.getMaxRetry() > 0){
                if(this.tryCount >= this.importContext.getMaxRetry())
                    throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
            }
            if(datas == null){
                this.buildDatas();
            }
            this.tryCount ++;
            String actionUrl = BuildTool.buildActionUrl(elasticsearchOutputConfig.getClientOptions(), BulkConfig.ERROR_FILTER_PATH);
            if (elasticsearchOutputConfig.isDebugResponse()) {

                for (ClientInterface clientInterface : clientInterfaces) {
                    data = clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST);
                }
                if (logger.isInfoEnabled())
                    logger.info(data);

            } else {
                if (elasticsearchOutputConfig.isDiscardBulkResponse() && elasticsearchOutputConfig.getExportResultHandler() == null) {
                    for (ClientInterface clientInterface : clientInterfaces) {
                        ESVoidResponseHandler esVoidResponseHandler = new ESVoidResponseHandler();
                        clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST, esVoidResponseHandler);

                        if (esVoidResponseHandler.getElasticSearchException() != null)
                            throw ImportExceptionUtil.buildDataImportException(importContext, esVoidResponseHandler.getElasticSearchException());
                    }
                    return null;
                } else {
                    for (ClientInterface clientInterface : clientInterfaces) {
                        data = clientInterface.executeHttp(actionUrl, datas, ClientUtil.HTTP_POST);
                    }
                }
            }
        }
        else{
            logNodatas( logger);           
        }
        finishTask();
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
