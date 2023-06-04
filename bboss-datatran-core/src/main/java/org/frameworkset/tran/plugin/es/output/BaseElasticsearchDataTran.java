package org.frameworkset.tran.plugin.es.output;

import com.frameworkset.common.poolman.handle.ValueExchange;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.BuildTool;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.elasticsearch.serial.CharEscapeUtil;
import org.frameworkset.elasticsearch.serial.SerialUtil;
import org.frameworkset.elasticsearch.template.ConfigDSLUtil;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.db.output.JDBCGetVariableValue;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;

import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.text.DateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class BaseElasticsearchDataTran extends BaseCommonRecordDataTran {
	private ClientInterface[] clientInterfaces;
	private boolean versionUpper7;;
	private String elasticsearch;
	protected ElasticsearchOutputConfig elasticsearchOutputConfig ;
	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
		evalBuilk(writer, context, versionUpper7);
		return null;
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
		if(clientInterfaces != null && clientInterfaces.length > 0)
			versionUpper7 = clientInterfaces[0].isVersionUpper7();
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
		tranJob = new StringTranJob();
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record, ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				if(datas != null) {
//					for (ClientInterface clientInterface : clientInterfaces) {
						taskNo++;
						TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext,  elasticsearchOutputConfig,
								dataSize, taskNo, taskContext.getJobNo(), lastValue, currentStatus, reachEOFClosed, taskContext);
//						count = 0;
						taskCommand.setClientInterfaces(clientInterfaces);
						taskCommand.setDatas((String) datas);
						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//					}
				}
				return taskNo;
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return BaseElasticsearchDataTran.this.buildStringRecord(context,writer);
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				return processDataSerial(  totalCount,  dataSize,  taskNo,   lastValue,  datas,  reachEOFClosed,  record);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				return processDataSerial(  totalCount,  dataSize,  taskNo,   lastValue,  datas,  reachEOFClosed,  record);
			}



			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return BaseElasticsearchDataTran.this.buildStringRecord(context,writer);
			}
		};
	}
	@Override
	public void init() {
		super.init();
		initClientInterfaces(elasticsearch);
		if(elasticsearchOutputConfig.getEsIndexWrapper() == null){
			throw new DataImportException("Global Elasticsearch index must be setted, please check your import job builder config.");
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




	protected int processDataSerial(ImportCount totalCount,long dataSize,int taskNo, LastValueWrapper lastValue,Object datas,boolean reachEOFClosed,CommonRecord record){
		if(datas != null) {
//			for (ClientInterface clientInterface : clientInterfaces) {
				taskNo++;
				TaskCommandImpl taskCommand = new TaskCommandImpl(totalCount, importContext, elasticsearchOutputConfig,
						dataSize, taskNo, taskContext.getJobNo(), lastValue, currentStatus, reachEOFClosed, taskContext);
//						count = 0;
				taskCommand.setClientInterfaces(clientInterfaces);
				taskCommand.setDatas((String) datas);
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




	public static void buildMeta(Context context, Writer writer ,boolean upper7) throws Exception {
		Object id = context.getEsId();
		Object parentId = context.getParentId();
		Object routing = context.getRouting();
		ClientOptions clientOptions = context.getClientOptions();
		Object esRetryOnConflict = clientOptions != null?clientOptions.getEsRetryOnConflict():null;
		ESIndexWrapper esIndexWrapper = context.getESIndexWrapper();

		JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(context);
		writer.write("{ \"");
		writer.write(context.getOperation());
		writer.write("\" : { \"_index\" : \"");

		if (esIndexWrapper == null ) {
			throw new DataImportException(" ESIndex not seted." );
		}
		BuildTool.buildIndiceName(esIndexWrapper,writer,jdbcGetVariableValue);

		writer.write("\"");
		if(!upper7) {
			writer.write(", \"_type\" : \"");
			if (esIndexWrapper == null ) {
				throw new DataImportException(" ESIndex type not seted." );
			}
			String indexType = BuildTool.buildIndiceType(esIndexWrapper,jdbcGetVariableValue);
			if(indexType == null || indexType.equals("")){
				throw new DataImportException(" ESIndex type not seted." );
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
		Object version = context.getVersion();

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
		/**
		String refresh = clientOptions!= null?clientOptions.getRefresh():null;

		if (refresh != null) {

			writer.write(",\"refresh\":\"");

			writer.write(refresh);
			writer.write("\"");
		}*/
		/**
		String timeout = clientOptions!= null?clientOptions.getTimeout():null;

		if (timeout != null) {

			writer.write(",\"timeout\":\"");

			writer.write(timeout);
			writer.write("\"");
		}
		 */
		/**
		Integer waitForActiveShards = clientOptions!= null?clientOptions.getWaitForActiveShards():null;

		if (waitForActiveShards != null) {

			writer.write(",\"wait_for_active_shards\":");

			writer.write(String.valueOf(waitForActiveShards));
		}
		 */
		/**
		Long if_seq_no = clientOptions!= null?clientOptions.getIfSeqNo():null;

		if (if_seq_no != null) {

			writer.write(",\"if_seq_no\":");

			writer.write(String.valueOf(if_seq_no));
		}
		Long if_primary_term = clientOptions!= null?clientOptions.getIfPrimaryTerm():null;

		if (if_primary_term != null) {

			writer.write(",\"if_primary_term\":");

			writer.write(String.valueOf(if_primary_term));
		}
		 */
//		if(!context.isUpdate()){
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
		if(context.isInsert()){

			String op_type = clientOptions!= null?clientOptions.getOpType():null;

			if (op_type != null) {

				writer.write(",\"op_type\":\"");

				writer.write(op_type);
				writer.write("\"");
			}
		}
		writer.write(" } }\n");

	}

	public  void evalBuilk( Writer writer, Context context, boolean upper7) throws Exception {


		if(context.isInsert()) {
//				SerialUtil.object2json(param,writer);
			buildMeta( context, writer ,       upper7);
			serialResult(  writer,context);
			writer.write("\n");
		}
		else if(context.isUpdate())
		{
			buildMeta( context, writer ,       upper7);
			writer.write("{\"doc\":");
			serialResult(  writer,context);
			ClientOptions clientOptions = context.getClientOptions();
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
		else if(context.isDelete()){
			evalDeleteBuilk(  writer, context,upper7);
		}
		else{
			buildMeta( context, writer ,       upper7);
			serialResult(  writer,context);
			writer.write("\n");
		}


	}

	public static void evalDeleteBuilk(Writer writer,  Context context,boolean isUpper7)  throws Exception{
		/**
		try {
			ESIndexWrapper esIndexWrapper = context.getESIndexWrapper();

			JDBCGetVariableValue jdbcGetVariableValue = new JDBCGetVariableValue(context);
			Object id = context.getEsId();


			writer.write("{ \"delete\" : { \"_index\" : \"");

			BuildTool.buildIndiceName(esIndexWrapper,writer,jdbcGetVariableValue);

			writer.write("\"");
			if(!isUpper7) {
				writer.write(", \"_type\" : \"");
				if (esIndexWrapper == null ) {
					throw new DataImportException(" ESIndex type not seted." );
				}
				BuildTool.buildIndiceType(esIndexWrapper,writer,jdbcGetVariableValue);
				writer.write("\"");
			}
			writer.write(", \"_id\" : ");
			BuildTool.buildId(id,writer,true);
			writer.write(" } }\n");


		} catch (Exception e) {
			throw new DataImportException(e);
		}*/
		buildMeta(context,writer,isUpper7);

	}

	private  void serialResult( Writer writer,  Context context) throws Exception {


		writer.write("{");

		boolean hasSeted = false;
//		CommonRecord dataRecord = super.buildRecord(context);
		CommonRecord dataRecord = context.getCommonRecord();
		Map<String,Object> datas = dataRecord.getDatas();
		Iterator<Map.Entry<String,Object>> iterator = datas.entrySet().iterator();
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
				RecordColumnInfo recordColumnInfo = dataRecord.getRecordColumnInfo(colName);
				if (value instanceof String) {
					writer.write("\"");
					CharEscapeUtil charEscapeUtil = new CharEscapeUtil(writer);
					charEscapeUtil.writeString((String) value, true);
					writer.write("\"");
				} else if (value instanceof Date) {
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



}
