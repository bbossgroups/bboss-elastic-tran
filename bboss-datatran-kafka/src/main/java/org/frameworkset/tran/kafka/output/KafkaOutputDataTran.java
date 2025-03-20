package org.frameworkset.tran.kafka.output;

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;
import org.slf4j.Logger;


import static org.frameworkset.tran.context.Context.KAFKA_TOPIC_KEY;

public class KafkaOutputDataTran extends BaseCommonRecordDataTran {
	protected String taskInfo;

	private KafkaOutputConfig kafkaOutputConfig ;
    public KafkaOutputDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo);
	}

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand() {
//			@Override
//			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue,
//                                          Object datas,
//                                          ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
//				return processDataSerial(  totalCount,   dataSize,   taskNo,   lastValue,   datas,    forceFlush);
//			}

            @Override
            public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
//                    List<CommonRecord> records = convertDatas( datas);
                    KafkaBatchCommand kafkaCommand = (KafkaBatchCommand) _buildTaskCommand(taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    taskCommandContext.addTask(kafkaCommand);


                }
                return taskCommandContext.getTaskNo();
//                if(datas != null) {
//                    taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//                    KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
//                            dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
//                    tasks.add(service.submit(new TaskCall(kafkaCommand, tranErrorWrapper)));
//                   
//                }
//                return taskNo;
               
            }

 

		};
		serialTranCommand = new BaseSerialTranCommand() {
            private int action(TaskCommandContext taskCommandContext){
                if(taskCommandContext.containData() )  {
                    taskCommandContext.increamentTaskNo();
//                    List<CommonRecord> records = convertDatas( datas);
                    KafkaBatchCommand kafkaCommand = (KafkaBatchCommand) _buildTaskCommand(taskCommandContext);
//					taskCommand.setRecords(records);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
                    TaskCall.call(kafkaCommand);


                }
                return taskCommandContext.getTaskNo();
//                if(datas != null) {     
//                    if(datas instanceof List || datas instanceof CommonRecord){
//                        taskNo++;
//                        List<CommonRecord> records = convertDatas( datas);
//                        KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
//                                dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
//                        TaskCall.call(kafkaCommand);
//                        return taskNo;
//                    }
//                    
//                }
//                return taskNo;
            }
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
//                if(datas != null) {
//                    taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//                    KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
//                            dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
//                    TaskCall.call(kafkaCommand);
//                }
//                return taskNo;
                return action(    taskCommandContext);
			}

            

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
//				return processDataSerial(  totalCount, dataSize,
//						taskNo, lastValue, datas, false );
                return action(    taskCommandContext);
			}

 
		};
	}
	@Override
	public void init() {
		super.init();
        if(kafkaOutputConfig == null){
            kafkaOutputConfig = (KafkaOutputConfig) outputPlugin.getOutputConfig();
        }
		//使用importContext.getLogsendTaskMetric()
//		logsendTaskMetric = kafkaOutputContext.getLogsendTaskMetric();
		taskInfo = new StringBuilder().append("Send data to kafka topic[")
				.append(kafkaOutputConfig.getTopic()).append("].").toString();

	}

	public KafkaOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,     currentStatus);
		kafkaOutputConfig = (KafkaOutputConfig) importContext.getOutputConfig();
		this.countDownLatch = countDownLatch;
	}




	public String serialExecute(){
		logger.info("Send data to kafka start.");
		return super.serialExecute();


	}

    @Override
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext) {
        return new KafkaBatchCommand(  taskCommandContext,outputPlugin.getOutputConfig());
    }

    class SerialData{
        String data;
        Object key;
        Object topic;
        public SerialData(Object data,CommonRecord record){
            this.data = (String)data;
            key = record.getRecordKey();
            /**
             * 从临时变量中获取记录对应的kafka主题，如果存在，就用记录级别的kafka主题，否则用全局配置的kafka主题发送数据
             */
            topic = record.getTempData(KAFKA_TOPIC_KEY);
        }
        
    }
    @Override
    public Object buildSerialDatas(Object data,CommonRecord record){
        
        return new SerialData(data,record);
    }
 

//	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
//
//
//		CommonRecord dataRecord = context.getCommonRecord();
//		kafkaOutputConfig.generateReocord(context.getTaskContext(),context.getTaskMetrics(),dataRecord, writer);
//		return dataRecord;
//	}
// 

}
