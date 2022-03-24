package org.frameworkset.tran.task;
/**
 * Copyright 2022 bboss
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.elasticsearch.ElasticSearchException;
import org.frameworkset.soa.BBossStringWriter;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: 文本类型数据处理作业逻辑</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/15
 * @author biaoping.yin
 * @version 1.0
 */
public class StringTranJob extends BaseTranJob{
	private static Logger logger = LoggerFactory.getLogger(StringTranJob.class);
	/**
	 * 串行批处理导入
	 * @return
	 */
	@Override
	public String batchExecute(SerialTranCommand serialTranCommand ,
									  Status currentStatus,
									  ImportContext importContext,
									  ImportContext targetImportContext,
									  TranResultSet jdbcResultSet, BaseDataTran baseDataTran){
		int count = 0;
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
//		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		long start = System.currentTimeMillis();
		long istart = 0;
		long end = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;

		ImportCount importCount = new SerialImportCount(baseDataTran);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {
			istart = start;
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(count > 0) {
						String _dd =  builder.toString();
						builder.setLength(0);
						int _count = count;
						count = 0;
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								_count, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(_dd);
//						ret = TaskCall.call(taskCommand);
//						taskNo ++;
						taskNo = serialTranCommand.hanBatchActionTask(importCount,_count,taskNo,lastValue,_dd,reachEOFClosed,null);

						if (baseDataTran.isPrintTaskLog()) {
							end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
									.append(",import ").append(_count).append(" records.").toString());
							istart = end;
						}


					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				if(lastValue == null)
					lastValue = importContext.max(currentValue,baseDataTran.getLastValue());
				else{
					lastValue = importContext.max(lastValue,baseDataTran.getLastValue());
				}
				Context context = importContext.buildContext(baseDataTran.getTaskContext(),jdbcResultSet, batchContext);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(context.removed()){
					if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
						importCount.increamentIgnoreTotalCount();
					}
					else{
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					}

					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					importCount.increamentIgnoreTotalCount();
					continue;
				}
//				CommonRecord record = buildRecord(  context );
//
//				fileOupputContext.generateReocord(context,record, writer);
//				writer.write(TranUtil.lineSeparator);
				serialTranCommand.buildStringRecord(context,writer);
				count++;
				totalCount ++;
				if (count >= batchsize) {
					writer.flush();
					String datas = builder.toString();
					builder.setLength(0);
					writer.close();
					writer = new BBossStringWriter(builder);

					int _count = count;
					count = 0;
//					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//							_count, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(datas);
//					ret = TaskCall.call(taskCommand);
//					taskNo ++;

					taskNo = serialTranCommand.hanBatchActionTask(importCount,_count,taskNo,lastValue,datas,reachEOFClosed,null);
					if(baseDataTran.isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}


				}

//				if(fileOupputContext.getMaxFileRecordSize() > 0 && totalCount > 0
//						&& (totalCount % fileOupputContext.getMaxFileRecordSize() == 0)){//reached max file record size
				if(serialTranCommand.splitCheck(totalCount)){
					String _dd = null;
					int _count = count;
					if (count > 0 ) {
						_dd = builder.toString();
						builder.setLength(0);
						count = 0;

					}
					serialTranCommand.splitSerialActionTask(importCount,_count,taskNo,lastValue,_dd,reachEOFClosed,null);
//					if(_dd != null) {
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
//								count, taskNo, importCount.getJobNo(), fileTransfer, lastValue, currentStatus, reachEOFClosed, taskContext);
//						taskCommand.setDatas(_dd);
//						TaskCall.call(taskCommand);
//						taskNo++;
//					}
//					sendFile(true);
//					fileTransfer.sendFile();
//					fileTransfer = this.initFileTransfer();
				}

			}
			String datas = null;
			if (count > 0) {
				datas = builder.toString();
				builder.setLength(0);
			}
			int oldTaskNo = taskNo;
			taskNo = serialTranCommand.endSerialActionTask(importCount,count,taskNo,lastValue,datas,reachEOFClosed,null);
			if(taskNo != oldTaskNo){
				if(baseDataTran.isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
			}
//			if (count > 0) {
//				writer.flush();
//				String datas = builder.toString();
//
//				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
//						count, taskNo, importCount.getJobNo(), fileTransfer, lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskCommand.setDatas(datas);
//				TaskCall.call(taskCommand);
//				if(isPrintTaskLog())  {
//					end = System.currentTimeMillis();
//					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
//							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
//							.append(ignoreTotalCount).append(" records.").toString());
//
//				}
//				fileTransfer.sendFile();
//			}
//			else{
//				if(!fileTransfer.isSended()){
//					fileTransfer.sendFile();
//				}
//			}
			if(baseDataTran.isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
		}  catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					baseDataTran.stop();
				} else{
					baseDataTran.stopTranOnly();
				}
			}
			try {
				writer.close();
			} catch (Exception e) {

			}
			baseDataTran.endJob( reachEOFClosed, importCount);
//			Date endTime = new Date();
//			if(baseDataTran.getTaskContext() != null)
//				baseDataTran.getTaskContext().setJobEndTime(endTime);
//			importCount.setJobEndTime(endTime);
		}

		return ret;
	}

	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	@Override
	public String parallelBatchExecute(final ParrelTranCommand serialTranCommand ,
										Status currentStatus,
										ImportContext importContext,
										ImportContext targetImportContext,
										TranResultSet jdbcResultSet, BaseDataTran baseDataTran){

		int count = 0;
		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		String ret = null;
		ExecutorService service = importContext.buildThreadPool();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount(baseDataTran);
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
//		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {

			BatchContext batchContext = new BatchContext();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					jdbcResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){//强制flush操作
					if (count > 0) {
						String datas = builder.toString();
						builder.setLength(0);
						int _count = count;
						count = 0;
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//								_count, taskNo, totalCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(datas);
//						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//						taskNo++;
						taskNo = serialTranCommand.hanBatchActionTask(totalCount,_count,taskNo,lastValue,datas,reachEOFClosed,null,
								service,tasks,tranErrorWrapper);

					}
					continue;
				}
				else if(!hasNext.booleanValue())
					break;

				if(lastValue == null)
					lastValue = importContext.max(currentValue,baseDataTran.getLastValue());
				else{
					lastValue = importContext.max(lastValue,baseDataTran.getLastValue());
				}
				Context context = importContext.buildContext(baseDataTran.getTaskContext(),jdbcResultSet, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, jdbcResultSet, batchContext);
				if(context.removed()){
					if(!reachEOFClosed)//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
						totalCount.increamentIgnoreTotalCount();
					else
						importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
					continue;
				}
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
					continue;
				}
//				CommonRecord record = buildRecord(  context );
//
//				fileOupputContext.generateReocord(context,record, writer);
//				writer.write(TranUtil.lineSeparator);
				serialTranCommand.buildStringRecord(context,writer);
				count++;
				if(count >= batchsize ){
					String datas = builder.toString();
					builder.setLength(0);

					int _count = count;
					count = 0;
//					FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							_count, taskNo, totalCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(datas);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//					taskNo++;
					taskNo = serialTranCommand.hanBatchActionTask(totalCount,_count,taskNo,lastValue,datas,reachEOFClosed,null,
							service,tasks,tranErrorWrapper);

				}
			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
				String datas = builder.toString();
				builder.setLength(0);
				taskNo = serialTranCommand.hanBatchActionTask(totalCount,count,taskNo,lastValue,datas,reachEOFClosed,null,
						service,tasks,tranErrorWrapper);
//				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//						count, taskNo, totalCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskCommand.setDatas(datas);
//				tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));



			}
			if(baseDataTran.isPrintTaskLog())
				logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());



		} catch (ElasticSearchException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new ElasticSearchException(e);
		}
		finally {
			baseDataTran.waitTasksComplete(tasks, service, exception, lastValue, totalCount, tranErrorWrapper, new WaitTasksCompleteCallBack() {
				@Override
				public void call() {
					serialTranCommand.parrelCompleteAction();
//					fileTransfer.sendFile();//传输文件
//					Date endTime = new Date();
//					if(baseDataTran.getTaskContext() != null)
//						baseDataTran.getTaskContext().setJobEndTime(endTime);
//					totalCount.setJobEndTime(endTime);


				}
			},reachEOFClosed);
			try {
				writer.close();
			} catch (Exception e) {

			}

		}

		return ret;
	}


	/**
	 * 串行处理数据，serialAllData为true，一次性将数据加载处理到内存，一次性写入 false 一次处理一条写入一条
	 * @param serialTranCommand
	 * @param currentStatus
	 * @param importContext
	 * @param targetImportContext
	 * @param jdbcResultSet
	 * @param baseDataTran
	 * @return
	 */
	@Override
	public String serialExecute(SerialTranCommand serialTranCommand,
								   Status currentStatus,
								   ImportContext importContext,
								   ImportContext targetImportContext,
								   TranResultSet jdbcResultSet, BaseDataTran baseDataTran){
		if(importContext.serialAllData()){
			return serialExecuteAllRecoreds(serialTranCommand ,
					  currentStatus,
					  importContext,
					  targetImportContext,
					  jdbcResultSet,   baseDataTran );
		}
		else{
			return serialExecuteOneRecord(serialTranCommand,
					  currentStatus,
					  importContext,
					  targetImportContext,
					  jdbcResultSet,   baseDataTran);
		}
	}

	/**
	 * 一次处理一条写入一条
	 * @param serialTranCommand
	 * @param currentStatus
	 * @param importContext
	 * @param targetImportContext
	 * @param jdbcResultSet
	 * @param baseDataTran
	 * @return
	 */
	private String serialExecuteOneRecord(SerialTranCommand serialTranCommand,
										  Status currentStatus,
										  ImportContext importContext,
										  ImportContext targetImportContext,
										  TranResultSet jdbcResultSet, BaseDataTran baseDataTran){

		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		long lastSend = 0;
//		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount(baseDataTran);
		long totalCount = 0;

		boolean reachEOFClosed = false;
		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;

//			List<DBRecord> records = new ArrayList<DBRecord>();
			//十分钟后打印一次等待日志数据，打印后，就等下次
			long logInterval = 1l * 60l * 1000l;
			boolean printed = false;
			BatchContext batchContext =  new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					if(baseDataTran.isPrintTaskLog() && !printed) {
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
						lastValue = importContext.max(currentValue, baseDataTran.getLastValue());
					else {
						lastValue = importContext.max(lastValue,baseDataTran. getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(baseDataTran.getTaskContext(),jdbcResultSet, batchContext);


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
					StringBuilder builder = new StringBuilder();
					BBossStringWriter writer = new BBossStringWriter(builder);
					CommonRecord record = serialTranCommand.buildStringRecord(  context,writer );

//					kafkaOutputContext.generateReocord(context,record, writer);
//					KafkaCommand kafkaCommand = new KafkaCommand(importCount, importContext,targetImportContext,
//							1, -1, importCount.getJobNo(), lastValue,taskContext,  currentStatus,reachEOFClosed);
//					kafkaCommand.setDatas(builder.toString());
//					kafkaCommand.setKey(record.getRecordKey());
//					TaskCall.asynCall(kafkaCommand);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(serialTranCommand.splitCheck( totalCount)){//reached max file record size
						String _dd =  builder.toString();
						builder.setLength(0);
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(_dd);
//						TaskCall.call(taskCommand);
//						taskNo ++;
////						fileTransfer.sendFile();
////						fileTransfer = this.initFileTransfer();
//						sendFile(true);
						serialTranCommand.splitSerialActionTask(importCount,1,-1,lastValue,_dd,reachEOFClosed,null);
					}
					else{
						serialTranCommand.hanBatchActionTask(importCount,1, -1,lastValue,builder.toString(),reachEOFClosed,record);
						builder.setLength(0);
					}
					if(totalCount == Long.MAX_VALUE) {
						if(baseDataTran.isPrintTaskLog()) {
							long end = System.currentTimeMillis();
							logger.info(new StringBuilder().append("Send datas  Take time:").append((end - start)).append("ms")
									.append(",Send total").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
									.append(importCount.getFailedCount()).append(" records. totalCount has reach Long.MAX_VALUE and reset").toString());

						}
						totalCount = 0;
					}
					else{
						if(baseDataTran.isPrintTaskLog() && importContext.getLogsendTaskMetric() > 0l && (totalCount % importContext.getLogsendTaskMetric()) == 0l) {//每一万条记录打印一次日志
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


			serialTranCommand.endSerialActionTask(importCount,-1,-1,lastValue,(String)null,reachEOFClosed,null);
			if(baseDataTran.isPrintTaskLog()) {
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
					baseDataTran.stop();
				} else{
					baseDataTran.stopTranOnly();
				}
			}
			if(importContext.isCurrentStoped()){

				baseDataTran.stopTranOnly();
			}
//			Date endTime = new Date();
//			if(baseDataTran.getTaskContext() != null)
//				baseDataTran.getTaskContext().setJobEndTime(endTime);
//			importCount.setJobEndTime(endTime);
			baseDataTran.endJob( reachEOFClosed, importCount);
		}
		return null;

	}

	/**
	 * 一次性将数据加载处理到内存，一次性写入
	 * @param serialTranCommand
	 * @param currentStatus
	 * @param importContext
	 * @param targetImportContext
	 * @param jdbcResultSet
	 * @param baseDataTran
	 * @return
	 */
	private String serialExecuteAllRecoreds(SerialTranCommand serialTranCommand,
											Status currentStatus,
											ImportContext importContext,
											ImportContext targetImportContext,
											TranResultSet jdbcResultSet, BaseDataTran baseDataTran){

		StringBuilder builder = new StringBuilder();
		BBossStringWriter writer = new BBossStringWriter(builder);
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
//		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount(baseDataTran);
		int taskNo = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;

		try {
			BatchContext batchContext =  new BatchContext();
			while (true) {
				Boolean hasNext = jdbcResultSet.next();
				if(hasNext == null){
					String ret = null;
					if(builder.length() > 0) {
						String _dd =  builder.toString();
						builder.setLength(0);
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(_dd);
//						ret = TaskCall.call(taskCommand);
//						taskNo ++;
						taskNo = serialTranCommand.hanBatchActionTask(importCount,totalCount,taskNo,lastValue,_dd,reachEOFClosed,null);
					}
					else{
						ret = "{\"took\":0,\"errors\":false}";
					}
//					importContext.flushLastValue(lastValue);
					if(baseDataTran.isPrintTaskLog()) {

						long end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Force flush datas Take time:").append((end - start)).append("ms")
								.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
								.append(ignoreTotalCount).append(" records.").toString());

					}
					continue;
				}
				else if(!hasNext.booleanValue()){
					break;
				}
				try {
					if (lastValue == null)
						lastValue = importContext.max(currentValue, baseDataTran.getLastValue());
					else {
						lastValue = importContext.max(lastValue, baseDataTran.getLastValue());
					}
//					Context context = new ContextImpl(importContext, jdbcResultSet, null);
					Context context = importContext.buildContext(baseDataTran.getTaskContext(),jdbcResultSet, batchContext);
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
//					CommonRecord record = buildRecord(  context );
//
//					fileOupputContext.generateReocord(context,record, writer);
//					writer.write(TranUtil.lineSeparator);
					serialTranCommand.buildStringRecord(context,writer);
//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.jdbcResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(serialTranCommand.splitCheck( totalCount)){//reached max file record size
						String _dd =  builder.toString();
						builder.setLength(0);
//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(_dd);
//						TaskCall.call(taskCommand);
//						taskNo ++;
////						fileTransfer.sendFile();
////						fileTransfer = this.initFileTransfer();
//						sendFile(true);
						taskNo = serialTranCommand.splitSerialActionTask(importCount,totalCount,taskNo,lastValue,_dd,reachEOFClosed,null);
					}
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			String datas = null;
			if(builder.length() > 0) {
				datas = builder.toString();
				builder.setLength(0);
			}
			taskNo = serialTranCommand.endSerialActionTask(importCount,totalCount,taskNo,lastValue,datas,reachEOFClosed,null);
//			if(builder.length() > 0) {
//
//				FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//						totalCount, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskNo ++;
//				taskCommand.setDatas(builder.toString());
//				builder.setLength(0);
//				TaskCall.call(taskCommand);
//
////				importContext.flushLastValue(lastValue);
////				fileTransfer.sendFile();//传输文件
//				sendFile(false);
//
//			}
//
//			else{
//				sendFile(false);
////				if(!fileTransfer.isSended()){
////					fileTransfer.sendFile();
////				}
//			}
			if(baseDataTran.isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Serial import Take time:").append((end - start)).append("ms")
						.append(",Total Import  ").append(totalCount).append(" records,Total Ignore Count ")
						.append(importCount.getIgnoreTotalCount()).append(" records,Total Failed Count ")
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
					baseDataTran.stop();
				} else{
					baseDataTran.stopTranOnly();
				}
			}
			if(importContext.isCurrentStoped()){

				baseDataTran.stopTranOnly();

			}
//			Date endTime = new Date();
//			if(baseDataTran.getTaskContext() != null)
//				baseDataTran.getTaskContext().setJobEndTime(endTime);
//			importCount.setJobEndTime(endTime);
			baseDataTran.endJob(  reachEOFClosed, importCount);
		}
		return null;

	}
}
