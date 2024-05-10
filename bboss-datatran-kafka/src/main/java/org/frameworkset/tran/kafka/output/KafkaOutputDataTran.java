package org.frameworkset.tran.kafka.output;

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.output.Base2DBTaskCommandImpl;
import org.frameworkset.tran.plugin.db.output.MultiSQLConf2DBTaskCommandImpl;
import org.frameworkset.tran.plugin.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;
import org.slf4j.Logger;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static org.frameworkset.tran.context.Context.KAFKA_TOPIC_KEY;

public class KafkaOutputDataTran extends BaseCommonRecordDataTran {
	protected String taskInfo;

	private KafkaOutputConfig kafkaOutputConfig ;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo);
	}

	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}
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
            public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas,
                                          ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {

                if(datas != null) {
                    taskNo++;
                    List<CommonRecord> records = convertDatas( datas);
                    KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
                            dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
                    tasks.add(service.submit(new TaskCall(kafkaCommand, tranErrorWrapper)));
                   
                }
                return taskNo;
               
            }

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return KafkaOutputDataTran.this.buildStringRecord(context,writer);
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
            private int action(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas){
                if(datas != null) {     
                    if(datas instanceof List || datas instanceof CommonRecord){
                        taskNo++;
                        List<CommonRecord> records = convertDatas( datas);
                        KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
                                dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
                        TaskCall.call(kafkaCommand);
                        return taskNo;
                    }
                    else{
                        return processDataSerial(  totalCount, dataSize,
                                taskNo, lastValue, datas );
                    }
                }
                return taskNo;
            }
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
//                if(datas != null) {
//                    taskNo++;
//                    List<CommonRecord> records = convertDatas( datas);
//                    KafkaBatchCommand kafkaCommand = new KafkaBatchCommand(totalCount, importContext,records,
//                            dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
//                    TaskCall.call(kafkaCommand);
//                }
//                return taskNo;
                return action(  totalCount,   dataSize,   taskNo,   lastValue,   datas);
			}

            

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
//				return processDataSerial(  totalCount, dataSize,
//						taskNo, lastValue, datas, false );
                return action(  totalCount,   dataSize,   taskNo,   lastValue,   datas);
			}


			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return KafkaOutputDataTran.this.buildStringRecord(context,writer);
			}
		};
	}
	@Override
	public void init() {
		super.init();

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

	protected int processDataSerial(ImportCount importCount, long dataSize,
									int taskNo, LastValueWrapper lastValue, Object datas ) {
		if(datas != null) {
			KafkaCommand kafkaCommand = new KafkaCommand(importCount, importContext,
					dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus);
            SerialData serialData = (SerialData)datas;
			kafkaCommand.setDatas(serialData.data);
			kafkaCommand.setKey(serialData.key);
            /**
             * 从临时变量中获取记录对应的kafka主题，如果存在，就用记录级别的kafka主题，否则用全局配置的kafka主题发送数据
             */
            Object topic = serialData.topic;
            if(topic != null && topic instanceof String) {
                if(!topic.equals(""))
                    kafkaCommand.setTopic((String) topic);
                else{
                    kafkaCommand.setTopic(kafkaOutputConfig.getTopic());
                }
            }
            else {
                kafkaCommand.setTopic(kafkaOutputConfig.getTopic());
            }
			TaskCall.asynCall(kafkaCommand);
		}
		return taskNo;
	}

	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {


		CommonRecord dataRecord = context.getCommonRecord();
		kafkaOutputConfig.generateReocord(context.getTaskContext(),dataRecord, writer);
		return dataRecord;
	}

	/**
	 * 并行批处理导入

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ){
		return super.parallelBatchExecute();
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	@Override
	public String batchExecute(  ){
		return super.batchExecute();
	}


}
