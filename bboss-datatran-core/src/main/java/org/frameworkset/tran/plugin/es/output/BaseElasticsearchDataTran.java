package org.frameworkset.tran.plugin.es.output;


import org.frameworkset.tran.BaseCommonRecordDataTran;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
 

public class BaseElasticsearchDataTran extends BaseCommonRecordDataTran {
	
	
	protected ElasticsearchOutputConfig elasticsearchOutputConfig ;
//	protected CommonRecord buildStringRecord(ElasticsearchCommonRecord elasticsearchCommonRecord, Writer writer) throws Exception {
//		evalBuilk(writer, elasticsearchCommonRecord, versionUpper7);
//		return null;
//	}
    

    public BaseElasticsearchDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	public BaseElasticsearchDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
		elasticsearchOutputConfig = (ElasticsearchOutputConfig) importContext.getOutputConfig();
 

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
//                        List<CommonRecord> records = convertDatas( datas);
						TaskCommandImpl taskCommand = (TaskCommandImpl)_buildTaskCommand(taskCommandContext);
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
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        return new TaskCommandImpl(taskCommandContext, (ElasticsearchOutputConfig) outputPlugin.getOutputConfig());
    }
    
	@Override
	public void init() {
		super.init();
        if(elasticsearchOutputConfig == null)
            elasticsearchOutputConfig =  (ElasticsearchOutputConfig) outputPlugin.getOutputConfig();

		taskInfo = new StringBuilder().append("import data to elasticsearch[").append(elasticsearchOutputConfig.getTargetElasticsearch()).append("] ")
				.append(" IndexName[").append(elasticsearchOutputConfig.getEsIndexWrapper().getIndex())
				.append("] IndexType[").append(elasticsearchOutputConfig.getEsIndexWrapper().getType())
				.append("] start.").toString();
	}

	protected int processDataSerial(TaskCommandContext taskCommandContext){
		if(taskCommandContext.containData()) {
//			for (ClientInterface clientInterface : clientInterfaces) {
            taskCommandContext.increamentTaskNo();
//                List<CommonRecord> records = convertDatas( datas);
            TaskCommandImpl taskCommand = (TaskCommandImpl)_buildTaskCommand(taskCommandContext);
//						count = 0;
				 
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
