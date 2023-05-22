package org.frameworkset.tran.kafka.output;

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.kafka.output.KafkaOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.StringTranJob;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;

import java.io.Writer;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class KafkaOutputDataTran extends BaseCommonRecordDataTran {
	protected String taskInfo;

	private KafkaOutputConfig kafkaOutputConfig ;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo);
	}

	@Override
	protected void initTranJob(){
		tranJob = new StringTranJob();
	}
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record, ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				return processDataSerial(  totalCount,   dataSize,   taskNo,   lastValue,   datas,   reachEOFClosed,null);
			}

			@Override
			public CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {
				return KafkaOutputDataTran.this.buildStringRecord(context,writer);
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				return processDataSerial(  totalCount, dataSize,
						taskNo, lastValue, datas,  reachEOFClosed,record );
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				return processDataSerial(  totalCount, dataSize,
						taskNo, lastValue, datas,  reachEOFClosed,record );
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





	protected int processDataSerial(ImportCount importCount, long dataSize,
									int taskNo, Object lastValue, Object datas, boolean reachEOFClosed,CommonRecord record ) {
		if(datas != null) {
			KafkaCommand kafkaCommand = new KafkaCommand(importCount, importContext,
					dataSize, taskNo, taskContext.getJobNo(), lastValue, taskContext, currentStatus, reachEOFClosed);
			kafkaCommand.setDatas((String) datas);
			kafkaCommand.setKey(record.getRecordKey());
			TaskCall.asynCall(kafkaCommand);
		}
		return taskNo;
	}

	protected CommonRecord buildStringRecord(Context context, Writer writer) throws Exception {

//		CommonRecord record = buildRecord(  context );
//		if(record.getDatas() == null){
//			Record dataRecord = context.getCurrentRecord();
//			if(dataRecord instanceof CommonStringRecord) {
//				record.setOringeData(dataRecord.getData());
//			}
//		}
		CommonRecord dataRecord = context.getCommonRecord();
		kafkaOutputConfig.generateReocord(context,dataRecord, writer);
		return dataRecord;
	}

	/**
	 * 并行批处理导入

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ){
		return serialExecute();
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	@Override
	public String batchExecute(  ){
		return serialExecute();
	}





//	@Override
//	public void stop(){
//		if(asynTranResultSet != null) {
//
//			asynTranResultSet.stop();
//			asynTranResultSet = null;
//		}
//		super.stop();
//	}
//	/**
//	 * 只停止转换作业
//	 */
//	@Override
//	public void stopTranOnly(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stopTranOnly();
//			asynTranResultSet = null;
//		}
//		super.stopTranOnly();
//	}



}
