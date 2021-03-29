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
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo);
	}



	@Override
	public void init() {
		super.init();
		kafkaOutputContext = (KafkaOutputContext)targetImportContext;
		taskInfo = new StringBuilder().append("import data to kafka topic[")
				.append(kafkaOutputContext.getTopic()).append("] start.").toString();
	}

	public KafkaOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}




	public String serialExecute(){
		logger.info("import data to kafka start.");

		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount();
		long totalCount = 0;

		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;
			Param param = null;
//			List<DBRecord> records = new ArrayList<DBRecord>();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){

					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				try {
					if (lastValue == null)
						lastValue = importContext.max(currentValue, getLastValue());
					else {
						lastValue = importContext.max(lastValue, getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(taskContext,jdbcResultSet, null);
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
							1, -1, importCount.getJobNo(), lastValue,taskContext,  currentStatus);
					kafkaCommand.setDatas(builder.toString());
					kafkaCommand.setKey(record.getRecordKey());
					TaskCall.asynCall(kafkaCommand);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;

				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}

			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(importCount.getIgnoreTotalCount()).append(" records.").toString());

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
				stop();
			}
			if(importContext.isCurrentStoped()){
				stop();
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






	public void stop(){
		if(esTranResultSet != null)
			esTranResultSet.stop();
		super.stop();
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
