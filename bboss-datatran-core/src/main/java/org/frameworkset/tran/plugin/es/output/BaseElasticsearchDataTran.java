package org.frameworkset.tran.plugin.es.output;

import com.frameworkset.common.poolman.Param;
import com.frameworkset.common.poolman.handle.ValueExchange;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import com.frameworkset.util.SimpleStringUtil;
import com.frameworkset.util.VariableHandler;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.serial.CharEscapeUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.elasticsearch.template.ConfigDSLUtil;
import org.frameworkset.persitent.util.PersistentSQLVariable;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.db.input.DBRecord;
import org.frameworkset.tran.plugin.db.output.JDBCGetVariableValue;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;
import org.frameworkset.util.annotations.DateFormateMeta;

import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.text.DateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BaseElasticsearchDataTran extends BaseCommonRecordDataTran {
	private ClientInterface[] clientInterfaces;
	
	private String elasticsearch;
	protected ElasticsearchOutputConfig elasticsearchOutputConfig ;
//	protected CommonRecord buildStringRecord(ElasticsearchCommonRecord elasticsearchCommonRecord, Writer writer) throws Exception {
//		evalBuilk(writer, elasticsearchCommonRecord, versionUpper7);
//		return null;
//	}
    @Override
    protected RecordColumnInfo resolveRecordColumnInfo(Object value,FieldMeta fieldMeta,Context context){
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
	private void initClientInterfaces(String elasticsearchs){
		if(elasticsearchs != null) {
			String[] _elasticsearchs = elasticsearchs.split(",");
			clientInterfaces = new ClientInterface[_elasticsearchs.length];
			for(int i = 0; i < _elasticsearchs.length; i ++) {
				clientInterfaces[i] = ElasticSearchHelper.getRestClientUtil(_elasticsearchs[i]);
			}

		}
		else{
			clientInterfaces = new ClientInterface[1];
			clientInterfaces[0] = ElasticSearchHelper.getRestClientUtil("default");
		}
		
	}
    
	public BaseElasticsearchDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
		elasticsearchOutputConfig = (ElasticsearchOutputConfig) importContext.getOutputConfig();
		String elasticsearch =  elasticsearchOutputConfig.getTargetElasticsearch();
		if(elasticsearch == null)
			elasticsearch = "default";
		this.elasticsearch = elasticsearch;

//		clientInterface = ElasticSearchHelper.getRestClientUtil(elasticsearch);
	}
	@Override
	protected void initTranJob(){
//		tranJob = new StringTranJob();
        tranJob = new CommonRecordTranJob();
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas,  ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				if(datas != null) {
//					for (ClientInterface clientInterface : clientInterfaces) {
						taskNo++;
                        List<CommonRecord> records = convertDatas( datas);
						TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext,  elasticsearchOutputConfig,
								dataSize, taskNo, taskContext.getJobNo(), lastValue, currentStatus,  taskContext);
//						count = 0;
						taskCommand.setClientInterfaces(clientInterfaces);
						taskCommand.setRecords(records);
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//					}
				}
				return taskNo;
			}

//			@Override
//			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//				return BaseElasticsearchDataTran.this.buildStringRecord(context,writer);
//			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
				return processDataSerial(  totalCount,  dataSize,  taskNo,   lastValue,  datas);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
				return processDataSerial(  totalCount,  dataSize,  taskNo,   lastValue,  datas);
			}



//			@Override
//			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//				return BaseElasticsearchDataTran.this.buildStringRecord(context,writer);
//			}
		};
	}
	@Override
	public void init() {
		super.init();
		initClientInterfaces(elasticsearch);
		if(elasticsearchOutputConfig.getEsIndexWrapper() == null){
			throw ImportExceptionUtil.buildDataImportException(importContext,"Global Elasticsearch index must be setted, please check your import job builder config.");
		}

		taskInfo = new StringBuilder().append("import data to elasticsearch[").append(elasticsearch).append("] ")
				.append(" IndexName[").append(elasticsearchOutputConfig.getEsIndexWrapper().getIndex())
				.append("] IndexType[").append(elasticsearchOutputConfig.getEsIndexWrapper().getType())
				.append("] start.").toString();
	}

	public BaseElasticsearchDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext,  String esCluster,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,     currentStatus);
		elasticsearchOutputConfig = (ElasticsearchOutputConfig) importContext.getOutputConfig();
		this.elasticsearch = esCluster;
//		clientInterface = ElasticSearchHelper.getRestClientUtil(esCluster);
	}




	protected int processDataSerial(ImportCount totalCount,long dataSize,int taskNo, LastValueWrapper lastValue,Object datas){
		if(datas != null) {
//			for (ClientInterface clientInterface : clientInterfaces) {
				taskNo++;
                List<CommonRecord> records = convertDatas( datas);
				TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext, elasticsearchOutputConfig,
						dataSize, taskNo, taskContext.getJobNo(), lastValue, currentStatus, taskContext);
//						count = 0;
				taskCommand.setClientInterfaces(clientInterfaces);
				taskCommand.setRecords(records);
				TaskCall.call(taskCommand);
//			}
		}
		return taskNo;
	}


//	public String tran(String indexName,String indexType) throws DataImportException{
//		ESIndexWrapper esIndexWrapper = new ESIndexWrapper(indexName,indexType);
//		elasticsearchOutputConfig.setEsIndexWrapper(esIndexWrapper);
//		return tran();
//	}


    @Override
    public CommonRecord buildRecord(Context context) throws Exception {
        ElasticsearchCommonRecord elasticsearchCommonRecord = new ElasticsearchCommonRecord();


        super.buildRecord(elasticsearchCommonRecord,context);
        elasticsearchCommonRecord.setEsId(context.getEsId());
        elasticsearchCommonRecord.setParentId(context.getParentId());
        elasticsearchCommonRecord.setRouting(context.getRouting());
        ClientOptions clientOptions = context.getClientOptions();
        elasticsearchCommonRecord.setClientOptions(clientOptions);
        elasticsearchCommonRecord.setEsIndexWrapper(context.getESIndexWrapper());
        JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(elasticsearchCommonRecord,context.getBatchContext());
        elasticsearchCommonRecord.setJdbcGetVariableValue(jdbcGetVariableValue);
        String operation = null;
            if(context.isInsert() ){
                operation = "index";
            }
            else if(context.isUpdate()){
                operation = "update";
            }
            else if(context.isDelete()){
                operation = "delete";
            }
            else{
                operation = "index";
            }
        elasticsearchCommonRecord.setOperation(operation);
        return elasticsearchCommonRecord;

    }





}
