package org.frameworkset.tran.plugin.es.output;


import org.frameworkset.elasticsearch.ElasticSearchHelper;
import org.frameworkset.elasticsearch.client.ClientInterface;
import org.frameworkset.tran.BaseCommonRecordDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
 

public class BaseElasticsearchDataTran extends BaseCommonRecordDataTran {
	private ClientInterface[] clientInterfaces;
	
	private String elasticsearch;
	protected ElasticsearchOutputConfig elasticsearchOutputConfig ;
//	protected CommonRecord buildStringRecord(ElasticsearchCommonRecord elasticsearchCommonRecord, Writer writer) throws Exception {
//		evalBuilk(writer, elasticsearchCommonRecord, versionUpper7);
//		return null;
//	}
    
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
//	@Override
//	protected void initTranJob(){
////		tranJob = new StringTranJob();
//        tranJob = new CommonRecordTranJob();
//	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand() {
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
				if(taskCommandContext.containData()) {
//					for (ClientInterface clientInterface : clientInterfaces) {
                    taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
//                        List<CommonRecord> records = convertDatas( datas);
						TaskCommandImpl taskCommand = new TaskCommandImpl(taskCommandContext,  elasticsearchOutputConfig);
//						count = 0;
						taskCommand.setClientInterfaces(clientInterfaces);
//						taskCommand.setRecords(records);
                    taskCommandContext.addTask(taskCommand);
//						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//					}
				}
				return taskCommandContext.getTaskNo();
			}

//			@Override
//			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//				return BaseElasticsearchDataTran.this.buildStringRecord(context,writer);
//			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
				return processDataSerial(    taskCommandContext);
			}

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
				return processDataSerial(    taskCommandContext);
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




	protected int processDataSerial(TaskCommandContext taskCommandContext){
		if(taskCommandContext.containData()) {
//			for (ClientInterface clientInterface : clientInterfaces) {
            taskCommandContext.increamentTaskNo();
            initTaskCommandContext(taskCommandContext);
//                List<CommonRecord> records = convertDatas( datas);
				TaskCommandImpl taskCommand = new TaskCommandImpl(taskCommandContext, elasticsearchOutputConfig);
//						count = 0;
				taskCommand.setClientInterfaces(clientInterfaces);
//				taskCommand.setRecords(records);
				TaskCall.call(taskCommand);
//			}
		}
		return taskCommandContext.getTaskNo();
	}


//	public String tran(String indexName,String indexType) throws DataImportException{
//		ESIndexWrapper esIndexWrapper = new ESIndexWrapper(indexName,indexType);
//		elasticsearchOutputConfig.setEsIndexWrapper(esIndexWrapper);
//		return tran();
//	}

 




}
