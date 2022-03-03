package org.frameworkset.tran.kafka.output;

import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCall;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class KafkaOutputDataTran extends BaseCommonRecordDataTran {
	protected String taskInfo;
	private CountDownLatch countDownLatch;
	private KafkaOutputContext kafkaOutputContext ;
	private long logsendTaskMetric = 10000l;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo);
	}



	@Override
	public void init() {
		super.init();
		kafkaOutputContext = (KafkaOutputContext)targetImportContext;
		logsendTaskMetric = kafkaOutputContext.getLogsendTaskMetric();
		taskInfo = new StringBuilder().append("Send data to kafka topic[")
				.append(kafkaOutputContext.getTopic()).append("].").toString();
	}

	public KafkaOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}




	public String serialExecute(){
		logger.info("Send data to kafka start.");

		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		long lastSend = 0;
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount();
		long totalCount = 0;

		boolean reachEOFClosed = false;
		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;

//			List<DBRecord> records = new ArrayList<DBRecord>();
			//十分钟后打印一次等待日志数据，打印后，就等下次
			long logInterval = 1l * 60l * 1000l;
			boolean printed = false;
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(isPrintTaskLog() && !printed) {
						if (lastSend > 0l) {//等待状态下，需一次打印日志
							long end = System.currentTimeMillis();
							long interval = end - lastSend;
							if (interval >= logInterval) {
								logger.info(new StringBuilder().append("Auto Log Send datas Take time:").append((end - start)).append("ms")
										.append(",Send total ").append(totalCount).append(" records,IgnoreTotalCount ")
										.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
										.append(importCount.getFailedCount()).append(" records.").toString());
								lastSend = 0l;
								printed = true;
							}


						}
						else{
							lastSend = System.currentTimeMillis();
						}
					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				lastSend = 0l;
				printed = false;
				try {
					if (lastValue == null)
						lastValue = importContext.max(currentValue, getLastValue());
					else {
						lastValue = importContext.max(lastValue, getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(taskContext,jdbcResultSet, null);


					if(!reachEOFClosed)
						reachEOFClosed = context.reachEOFClosed();
					if(context.removed()){
						if(!reachEOFClosed)//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
							importCount.increamentIgnoreTotalCount();
						else
							importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
						continue;
					}
					context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
						continue;
					}
					CommonRecord record = buildRecord(  context );
					StringBuilder builder = new StringBuilder();
					BBossStringWriter writer = new BBossStringWriter(builder);
					kafkaOutputContext.generateReocord(context,record, writer);
					KafkaCommand kafkaCommand = new KafkaCommand(importCount, importContext,targetImportContext,
							1, -1, importCount.getJobNo(), lastValue,taskContext,  currentStatus,reachEOFClosed);
					kafkaCommand.setDatas(builder.toString());
					kafkaCommand.setKey(record.getRecordKey());
					TaskCall.asynCall(kafkaCommand);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(totalCount == Long.MAX_VALUE) {
						if(isPrintTaskLog()) {
							long end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Send datas  Take time:").append((end - start)).append("ms")
									.append(",Send total").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
						.append(importCount.getFailedCount()).append(" records. totalCount has reach Long.MAX_VALUE and reset").toString());

						}
						totalCount = 0;
					}
					else{
						if(isPrintTaskLog() && logsendTaskMetric > 0l && (totalCount % logsendTaskMetric) == 0l) {//每一万条记录打印一次日志
							long end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Send datas Take time:").append((end - start)).append("ms")
									.append(",Send total ").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
									.append(importCount.getFailedCount()).append(" records.").toString());

						}
					}
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}

			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Send datas Take time:").append((end - start)).append("ms")
						.append(",Send total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
						.append(importCount.getFailedCount()).append(" records.").toString());

			}
		}
		catch (DataImportException e){
			exception = e;
			throw e;


		}
		catch (Exception e){
			exception = e;
			throw new DataImportException(e);


		} finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					this.stop();
				} else{
					this.stopTranOnly();
				}
			}
			if(importContext.isCurrentStoped()){

					this.stopTranOnly();
			}
			importCount.setJobEndTime(new Date());
		}
		return null;

	}

	/**
	 * 并行批处理导入

	 * @return
	 */
	public String parallelBatchExecute( ){
		return serialExecute();
	}
	/**
	 * 串行批处理导入
	 * @return
	 */
	public String batchExecute(  ){
		return serialExecute();
	}





	@Override
	public void stop(){
		if(esTranResultSet != null) {

			esTranResultSet.stop();
			esTranResultSet = null;
		}
		super.stop();
	}
	/**
	 * 只停止转换作业
	 */
	@Override
	public void stopTranOnly(){
		if(esTranResultSet != null) {
			esTranResultSet.stopTranOnly();
			esTranResultSet = null;
		}
		super.stopTranOnly();
	}

	@Override
	public String tran() throws ESDataImportException {
		try {
			return super.tran();
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}
	}

}
