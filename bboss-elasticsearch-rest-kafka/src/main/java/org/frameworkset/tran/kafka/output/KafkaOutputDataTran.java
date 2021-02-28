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

public class KafkaOutputDataTran extends BaseDataTran {
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

	public KafkaOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext);
		this.countDownLatch = countDownLatch;
	}



	private CommonRecord buildRecord(Context context){
		String[] columns = targetImportContext.getExportColumns();
		//如果采用结果集中的字段集，就需要考虑全局添加或者记录级别添加的字段，通过配置设置导出的字段集不需要考虑全局添加或者记录级别添加的字段
		boolean useResultKeys = false;
		if (columns == null){
			Object  keys = jdbcResultSet.getKeys();
			if(keys != null) {
				useResultKeys = true;
				if(keys instanceof Set) {
					Set<String> _keys = (Set<String>) keys;
					columns = _keys.toArray(new String[_keys.size()]);
				}
				else{
					columns = (String[])keys;
				}
			}
			else{
				throw new DataImportException("Export Columns is null,Please set Export Columns in importconfig.");
			}
		}
		Object temp = null;
		CommonRecord dbRecord = new CommonRecord();

		Map<String,Object> addedFields = new HashMap<String,Object>();
		//计算记录级别字段配置值
		List<FieldMeta> fieldValueMetas = context.getFieldValues();//context优先级高于全局配置，全局配置高于数据源级别字段值

		appendFieldValues( dbRecord, columns,    fieldValueMetas,  addedFields, useResultKeys);
		//计算全局级别字段配置值
		fieldValueMetas = context.getESJDBCFieldValues();//全局配置
		appendFieldValues(  dbRecord, columns,   fieldValueMetas,  addedFields,  useResultKeys);
		//计算数据源级别字段值
		String varName = null;
		for(int i = 0;i < columns.length; i ++)
		{
			varName = columns[i];
			if(addedFields.get(varName) != null)
				continue;
			FieldMeta fieldMeta = context.getMappingName(varName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				varName = fieldMeta.getEsFieldName();
			}
			temp = jdbcResultSet.getValue(varName);
			if(temp == null) {
				if(logger.isWarnEnabled())
					logger.warn("未指定绑定变量的值：{}",varName);
			}
			dbRecord.addData(varName,temp);


		}


		return dbRecord;
	}

	public String serialExecute(){
		logger.info("import data to kafka start.");

		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		Status currentStatus = importContext.getCurrentStatus();
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
							1, -1, importCount.getJobNo(), lastValue,taskContext);
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


	private void appendFieldValues(CommonRecord record,
								   String[] columns ,
								   List<FieldMeta> fieldValueMetas,
								   Map<String, Object> addedFields,boolean useResultKeys) {
		if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
			return;
		}

		for(FieldMeta fieldMeta:fieldValueMetas){
			String fieldName = fieldMeta.getEsFieldName();
			if(addedFields.containsKey(fieldName))
				continue;
			boolean matched = false;
			for(String name:columns){
				if(name.equals(fieldName)){
					record.addData(name,fieldMeta.getValue());
					addedFields.put(name,dummy);
					matched = true;
					break;
				}
			}
			if(useResultKeys && !matched){
				record.addData(fieldName,fieldMeta.getValue());
				addedFields.put(fieldName,dummy);
			}
		}
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
