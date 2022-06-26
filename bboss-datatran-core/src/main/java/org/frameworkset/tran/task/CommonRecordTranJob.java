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
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/15
 * @author biaoping.yin
 * @version 1.0
 */
public class CommonRecordTranJob extends BaseTranJob{
	private static Logger logger = LoggerFactory.getLogger(CommonRecordTranJob.class);
	/**
	 * 串行批处理导入
	 * @return
	 */
	public String batchExecute(SerialTranCommand serialTranCommand ,
									  Status currentStatus,
									  ImportContext importContext,
									  TranResultSet tranResultSet, BaseDataTran baseDataTran){
		int count = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
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
				Boolean hasNext = tranResultSet.next();
				if(hasNext == null){
					if(count > 0) {

						int _count = count;
						count = 0;
//						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								_count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(records);
//
//						ret = TaskCall.call(taskCommand);
//						taskNo ++;
						taskNo = serialTranCommand.hanBatchActionTask(importCount,_count,taskNo,lastValue,records,reachEOFClosed,null);
						records = new ArrayList<>();
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
				Context context = importContext.buildContext(baseDataTran.getTaskContext(),tranResultSet, batchContext);

				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, tranResultSet, batchContext);
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
				CommonRecord record = baseCommonRecordDataTran.buildRecord(  context );

				records.add(record);
				count++;
				totalCount ++;
				if (count >= batchsize) {


					int _count = count;
					count = 0;
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//							_count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//
//					ret = TaskCall.call(taskCommand);
//					taskNo ++;
					taskNo = serialTranCommand.hanBatchActionTask(importCount,_count,taskNo,lastValue,records,reachEOFClosed,null);
					records = new ArrayList<>();


					if(baseDataTran.isPrintTaskLog())  {
						end = System.currentTimeMillis();
						logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
								.append(",import ").append(batchsize).append(" records.").toString());
						istart = end;
					}


				}

				if(serialTranCommand.splitCheck(totalCount)){//reached max file record size

//					if (count > 0 ) {
//
//
//						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
//								count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer, lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(records);
//						records = new ArrayList<>();
//						TaskCall.call(taskCommand);
//						count = 0;
//						taskNo++;
//					}
//					fileTransfer.sendFile();
//					fileTransfer = this.initFileTransfer();

					int _count = count;
					List<CommonRecord> commonRecords = null;
					if (count > 0 ) {
						count = 0;
						commonRecords = records;
						records = new ArrayList<>();
					}
					taskNo = serialTranCommand.splitSerialActionTask(importCount,_count,taskNo,lastValue,commonRecords,reachEOFClosed,null);
				}

			}
			taskNo = serialTranCommand.endSerialActionTask(importCount,count,taskNo,lastValue,records,reachEOFClosed,null);
//			if (count > 0) {
//
//				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext, targetImportContext,
//						count, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer, lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskCommand.setDatas(records);
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
			if(count > 0 ){
				if(baseDataTran.isPrintTaskLog())  {
					end = System.currentTimeMillis();
					logger.info(new StringBuilder().append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
			}
			if(baseDataTran.isPrintTaskLog()) {
				end = System.currentTimeMillis();
				logger.info(new StringBuilder().append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
		} catch (DataImportException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new DataImportException(e);
		}
		finally {

			if(!TranErrorWrapper.assertCondition(exception ,importContext)){
				if(!importContext.getDataTranPlugin().isMultiTran()) {
					baseDataTran.stop();
				} else{
					baseDataTran.stopTranOnly();
				}
			}

//			Date endTime = new Date();
//			if(baseDataTran.getTaskContext() != null)
//				baseDataTran.getTaskContext().setJobEndTime(endTime);
//			importCount.setJobEndTime(endTime);
//			if(reachEOFClosed){
//				baseDataTran.tranStopReadEOFCallback();
//			}
			baseDataTran.endJob( reachEOFClosed, importCount);
		}

		return ret;
	}





	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	public String parallelBatchExecute(final ParrelTranCommand parrelTranCommand ,
										Status currentStatus,
										ImportContext importContext,
										TranResultSet tranResultSet, BaseDataTran baseDataTran){

		int count = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount(baseDataTran);
		Exception exception = null;
//		Status currentStatus = importContext.getCurrentStatus();
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		Object lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {

			BatchContext batchContext = new BatchContext();
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					tranResultSet.stop();
					tranErrorWrapper.throwError();
				}
				Boolean hasNext = tranResultSet.next();
				if(hasNext == null){//强制flush操作
					if (count > 0) {



						int _count = count;
						count = 0;
//						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//								_count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(records);
//
//						tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//
//						taskNo++;
						taskNo = parrelTranCommand.hanBatchActionTask(totalCount,_count,taskNo,lastValue,records,reachEOFClosed,null,service,tasks,tranErrorWrapper);
						records = new ArrayList<>();

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
				Context context = importContext.buildContext(baseDataTran.getTaskContext(),tranResultSet, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = context.reachEOFClosed();
//				Context context = new ContextImpl(importContext, tranResultSet, batchContext);
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
				CommonRecord record = baseCommonRecordDataTran.buildRecord(  context );

				records.add(record);
				count++;
				if(count >= batchsize ){

					int _count = count;
					count = 0;
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							_count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
//					taskNo++;
					taskNo = parrelTranCommand.hanBatchActionTask(totalCount,_count,taskNo,lastValue,records,reachEOFClosed,null,service,tasks,tranErrorWrapper);
					records = new ArrayList<>();

				}
			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
					tranErrorWrapper.throwError();
				}
//				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//						count, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskCommand.setDatas(records);
//				tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));
				taskNo = parrelTranCommand.hanBatchActionTask(totalCount,count,taskNo,lastValue,records,reachEOFClosed,null,service,tasks,tranErrorWrapper);

			}
			if(baseDataTran.isPrintTaskLog())
				logger.info(new StringBuilder().append("Pararrel batch submit tasks:").append(taskNo).toString());


		} catch (DataImportException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw new DataImportException(e);
		}
		finally {
			final boolean _reachEOFClosed = reachEOFClosed;
			baseDataTran.waitTasksComplete(tasks, service, exception, lastValue, totalCount, tranErrorWrapper, new WaitTasksCompleteCallBack() {
				@Override
				public void call() {
//					fileTransfer.sendFile();//传输文件
					parrelTranCommand.parrelCompleteAction();
//					Date endTime = new Date();
//					if(baseDataTran.getTaskContext() != null)
//						baseDataTran.getTaskContext().setJobEndTime(endTime);
//					totalCount.setJobEndTime(endTime);

				}
			},reachEOFClosed);


		}

		return ret;
	}


	public String serialExecute(SerialTranCommand serialTranCommand,
								   Status currentStatus,
								   ImportContext importContext,
								   TranResultSet tranResultSet, BaseDataTran baseDataTran){
		if(importContext.serialAllData()){
			return serialExecuteAllRecoreds(serialTranCommand ,
					  currentStatus,
					  importContext,
					  tranResultSet,   baseDataTran );
		}
		else{
			return serialExecuteOneRecord(serialTranCommand,
					  currentStatus,
					  importContext,
					  tranResultSet,   baseDataTran);
		}
	}
	private String serialExecuteOneRecord(SerialTranCommand serialTranCommand,
										  Status currentStatus,
										  ImportContext importContext,
										  TranResultSet tranResultSet, BaseDataTran baseDataTran){

		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		long lastSend = 0;
//		Status currentStatus = this.currentStatus;
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount(baseDataTran);
		long totalCount = 0;
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
		boolean reachEOFClosed = false;
		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;

//			List<DBRecord> records = new ArrayList<DBRecord>();
			//十分钟后打印一次等待日志数据，打印后，就等下次
			long logInterval = 1l * 60l * 1000l;
			boolean printed = false;
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = tranResultSet.next();
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
//					Context context = new ContextImpl(importContext, tranResultSet, null);
					Context context = importContext.buildContext(baseDataTran.getTaskContext(),tranResultSet, batchContext);


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
					CommonRecord record = baseCommonRecordDataTran.buildRecord(context);

//					kafkaOutputContext.generateReocord(context,record, writer);
//					KafkaCommand kafkaCommand = new KafkaCommand(importCount, importContext,targetImportContext,
//							1, -1, importCount.getJobNo(), lastValue,taskContext,  currentStatus,reachEOFClosed);
//					kafkaCommand.setDatas(builder.toString());
//					kafkaCommand.setKey(record.getRecordKey());
//					TaskCall.asynCall(kafkaCommand);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.tranResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(serialTranCommand.splitCheck( totalCount)){//reached max file record size

//						FileFtpTaskCommandImpl taskCommand = new FileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(_dd);
//						TaskCall.call(taskCommand);
//						taskNo ++;
////						fileTransfer.sendFile();
////						fileTransfer = this.initFileTransfer();
//						sendFile(true);
						serialTranCommand.splitSerialActionTask(importCount,1,-1,lastValue,record,reachEOFClosed,null);
					}
					else{
						serialTranCommand.hanBatchActionTask(importCount,1, -1,lastValue,record,reachEOFClosed,record);

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


			serialTranCommand.endSerialActionTask(importCount,-1,-1,lastValue,(CommonRecord)null,reachEOFClosed,null);
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

	private String serialExecuteAllRecoreds(SerialTranCommand serialTranCommand,
											Status currentStatus,
											ImportContext importContext,
											TranResultSet tranResultSet, BaseDataTran baseDataTran){

//		logger.info("serial import data Execute started.");
		List<CommonRecord> records = new ArrayList<>();
		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
		ImportCount importCount = new SerialImportCount(baseDataTran);
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
		int taskNo = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;

		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;

//			List<DBRecord> records = new ArrayList<DBRecord>();
			BatchContext batchContext = new BatchContext();
			while (true) {
				Boolean hasNext = tranResultSet.next();
				if(hasNext == null){
					String ret = null;
					if(records.size() > 0) {

//						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(records);
//						ret = TaskCall.call(taskCommand);
//						taskNo ++;
						taskNo = serialTranCommand.hanBatchActionTask(importCount,totalCount,taskNo,lastValue,records,reachEOFClosed,null);
						records = new ArrayList<>();
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
//					Context context = new ContextImpl(importContext, tranResultSet, null);
					Context context = importContext.buildContext(baseDataTran.getTaskContext(),tranResultSet, batchContext);
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
					CommonRecord record = baseCommonRecordDataTran.buildRecord(  context );
					records.add(record);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.tranResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					if(serialTranCommand.splitCheck(totalCount)){//reached max file record size

//						ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//								totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer)fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//						taskCommand.setDatas(records);
//						TaskCall.call(taskCommand);
//						taskNo ++;
//
//						fileTransfer.sendFile();
//						fileTransfer = this.initFileTransfer();
						taskNo = serialTranCommand.splitSerialActionTask(importCount,totalCount,taskNo,lastValue,records,reachEOFClosed,null);
						records = new ArrayList<>();
					}
				} catch (Exception e) {
					throw new DataImportException(e);
				}
			}
			taskNo = serialTranCommand.endSerialActionTask(importCount,totalCount,taskNo,lastValue,records,reachEOFClosed,null);
//			if(records.size() > 0) {
//
//				ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(importCount, importContext,targetImportContext,
//						totalCount, taskNo, importCount.getJobNo(), (ExcelFileTransfer)fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//				taskNo ++;
//				taskCommand.setDatas(records);
//				TaskCall.call(taskCommand);
//
////				importContext.flushLastValue(lastValue);
//				fileTransfer.sendFile();//传输文件
//			}
//			else{
//				if(!fileTransfer.isSended()){
//					fileTransfer.sendFile();
//				}
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
