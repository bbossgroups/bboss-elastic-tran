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
import org.frameworkset.tran.Record;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
import org.frameworkset.tran.metrics.SerialImportCount;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.record.WrappedRecord;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.status.LastValueWrapper;
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
        int droped = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		int taskNo = 0;
		Exception exception = null;
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
        LastValueWrapper currentLastValueWrapper = currentStatus != null? currentStatus.getCurrentLastValueWrapper():null;
        LastValueWrapper lastValue = null;


		long start = System.currentTimeMillis();
		long istart = 0;
		long end = 0;
		long totalCount = 0;
		long ignoreTotalCount = 0;
        int ignoreCount = 0 ;
		ImportCount importCount = new SerialImportCount();
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
		try {
			istart = start;
			BatchContext batchContext = new BatchContext();
//			BuildMapDataContext buildMapDataContext = buildMapDataContext( importContext);
			while (true) {

                NextAssert hasNext = tranResultSet.next();
                try{
                    if(hasNext.isNeedFlush()){
                        if(count > 0) {
                            TaskCommandContext taskCommandContext = new TaskCommandContext();
                            taskCommandContext.setTotalCount(importCount);
                            taskCommandContext.setDataSize(count);
                            taskCommandContext.setTaskNo(taskNo);
                            taskCommandContext.setLastValue(lastValue);
                            taskCommandContext.setCommonRecords(records);
                            taskCommandContext.setIgnoreCount(ignoreCount);
                            taskCommandContext.setImportContext(importContext);
                            count = 0;
                            droped = 0;
                            ignoreCount = 0;

                            taskNo = serialTranCommand.hanBatchActionTask(taskCommandContext);
                            records = new ArrayList<>();
                            if (baseDataTran.isPrintTaskLog()) {
                                end = System.currentTimeMillis();
                                StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                                logger.info(builder.append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
                                        .append(",import ").append(taskCommandContext.getDataSize()).append(" records.").append("Force FlushInterval[").append(importContext.getFlushInterval()).append("ms]").toString());
                                istart = end;
                            }


                        }
                        else if(droped > 0){
                            importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
                            droped = 0;
                        }
                        if(!hasNext.isHasNext())
                            continue;
                    }
                    if(!hasNext.isHasNext()){
                        break;
                    }
                    Record resultRecord = new WrappedRecord(tranResultSet);;
                    if(lastValue == null) {
                        lastValue = importContext.max(currentLastValueWrapper, resultRecord);
                    }
                    else{
                        lastValue = importContext.max(lastValue,resultRecord);
                    }
                    if(resultRecord.isRecordDirectIgnore()){
                        droped ++;
                        continue;
                    }
                  
                    if(!reachEOFClosed)
                        reachEOFClosed = resultRecord.reachEOFClosed();
//				Context context = new ContextImpl(importContext, tranResultSet, batchContext);
                    if(resultRecord.removed()){
                        if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
                            importCount.increamentIgnoreTotalCount();
                            ignoreCount ++;
                        }
                        else{
                            importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
                        }

                        continue;
                    }
                    Context context = importContext.buildContext(baseDataTran.getTaskContext(),resultRecord, batchContext);

                    context.refactorData();                    
                    if (context.isDrop()) {
                        importCount.increamentIgnoreTotalCount();
                        ignoreCount ++;
                        droped ++;
                        continue;
                    }
                    context.afterRefactor();
                    CommonRecord record = importContext.getOutputPlugin().buildRecord(  context );
//                    super.metricsMap(record,buildMapDataContext,importContext);
                    records.add(record);
                    count++;
                    totalCount ++;
                    if (count >= batchsize || serialTranCommand.splitCheck(totalCount)) {

                        TaskCommandContext taskCommandContext = new TaskCommandContext();
                        taskCommandContext.setTotalCount(importCount);
                        taskCommandContext.setDataSize(count);
                        taskCommandContext.setTaskNo(taskNo);
                        taskCommandContext.setLastValue(lastValue);
                        taskCommandContext.setCommonRecords(records);
                        taskCommandContext.setIgnoreCount(ignoreCount);
                        taskCommandContext.setImportContext(importContext);
                        
                        count = 0;
                        droped = 0;
                        ignoreCount = 0;
                        taskNo = serialTranCommand.hanBatchActionTask(taskCommandContext);
                        records = new ArrayList<>();


                        if(baseDataTran.isPrintTaskLog())  {
                            end = System.currentTimeMillis();
                            StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                            logger.info(builder.append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
                                    .append(",import ").append(batchsize).append(" records.").toString());
                            istart = end;
                        }


                    }
                }
                catch (Exception e){
                    if(importContext.isContinueOnError() && importContext.getInputPlugin().isEventMsgTypePlugin()){
                        logger.warn("ContinueOnError:true",e);
                        continue;
                    }
                    throw e;
                }
			}
            TaskCommandContext taskCommandContext = new TaskCommandContext();
            taskCommandContext.setTotalCount(importCount);
            taskCommandContext.setDataSize(count);
            taskCommandContext.setTaskNo(taskNo);
            taskCommandContext.setLastValue(lastValue);
            taskCommandContext.setCommonRecords(records);
            taskCommandContext.setIgnoreCount(ignoreCount);
            taskCommandContext.setImportContext(importContext);
			taskNo = serialTranCommand.endSerialActionTask(taskCommandContext);

			if(count > 0 ){
				if(baseDataTran.isPrintTaskLog())  {
					end = System.currentTimeMillis();
                    StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                    logger.info(builder.append("Batch import Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
							.append(",import ").append(count).append(" records,IgnoreTotalCount ")
							.append(ignoreTotalCount).append(" records.").toString());

				}
			}
			if(baseDataTran.isPrintTaskLog()) {
				end = System.currentTimeMillis();
                StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                logger.info(builder.append("Batch import Execute Tasks:").append(taskNo).append(",All Take time:").append((end - start)).append("ms")
						.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
						.append(ignoreTotalCount).append(" records.").toString());

			}
		} catch (DataImportException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
		finally {

            /**
			if(!TranErrorWrapper.assertCondition(exception ,importContext)){

                baseDataTran.stop();
			}*/
            baseDataTran.stop2ndClearResultsetQueue(exception != null);// a{1}


			baseDataTran.endJob( reachEOFClosed, importCount, exception);
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
        int droped = 0;
		long totalSize = 0;
		List<CommonRecord> records = new ArrayList<>();
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
//        ExecutorService buildRecordHandlerExecutor = importContext.buildRecordHandlerExecutor();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount();
        Throwable exception = null;
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
        LastValueWrapper currentLastValueWrapper = currentStatus != null? currentStatus.getCurrentLastValueWrapper():null;
        LastValueWrapper lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
        long istart = 0;
        long end = 0;
        int ignoreCount = 0;
        
		try {

			BatchContext batchContext = new BatchContext();
//			BuildMapDataContext buildMapDataContext = buildMapDataContext( importContext);
			while (true) {
				if(!tranErrorWrapper.assertCondition()) {
					tranResultSet.stop(true);
                    Throwable ex = tranErrorWrapper.throwError();
					if(ex != null)
						throw ex;
					else
					{
						break;
					}
				}
                NextAssert hasNext = tranResultSet.next();
				if(hasNext.isNeedFlush()){//强制flush操作
					if (count > 0) {
                        TaskCommandContext taskCommandContext = new TaskCommandContext();
                        taskCommandContext.setTotalCount(totalCount);
                        taskCommandContext.setDataSize(count);
                        taskCommandContext.setTaskNo(taskNo);
                        taskCommandContext.setLastValue(lastValue);
                        taskCommandContext.setCommonRecords(records);
                        taskCommandContext.setService(service);
                        taskCommandContext.setTasks(tasks);
                        taskCommandContext.setTranErrorWrapper(tranErrorWrapper);
                        taskCommandContext.setIgnoreCount(ignoreCount);
                        taskCommandContext.setImportContext(importContext);
						count = 0;
                        droped = 0;
                        ignoreCount = 0;
						taskNo = parrelTranCommand.hanBatchActionTask(taskCommandContext);
                        if (baseDataTran.isPrintTaskLog()) {
                            end = System.currentTimeMillis();
                            StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                            logger.info(builder.append("Batch import Force flush datas Task[").append(taskNo).append("] complete,take time:").append((end - istart)).append("ms")
                                    .append(",import ").append(taskCommandContext.getDataSize()).append(" records.").append("Force FlushInterval[").append(importContext.getFlushInterval()).append("ms]").toString());
                            istart = end;
                        }
						records = new ArrayList<>();

					}
                    else if(droped > 0){
                        importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
                        droped = 0;
                    }
                    if(!hasNext.isHasNext())
                        continue;
				}
				if(!hasNext.isHasNext())
					break;
                Record resultRecord = new WrappedRecord(tranResultSet);;
				if(lastValue == null)
					lastValue = importContext.max(currentLastValueWrapper,resultRecord);
				else{
					lastValue = importContext.max(lastValue,resultRecord);
				}
                if(resultRecord.isRecordDirectIgnore()){
                    droped ++;
                    continue;
                }
				
				if(!reachEOFClosed)
					reachEOFClosed = resultRecord.reachEOFClosed();
//				Context context = new ContextImpl(importContext, tranResultSet, batchContext);
				if(resultRecord.removed()){
					if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
                        ignoreCount ++;
                        totalCount.increamentIgnoreTotalCount();
                    }
					continue;
				}
                Context context = importContext.buildContext(baseDataTran.getTaskContext(),resultRecord, batchContext);
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
                    ignoreCount ++;
                    droped ++;
					continue;
				}
				CommonRecord record = importContext.getOutputPlugin().buildRecord(  context );
//				super.metricsMap(record,buildMapDataContext,importContext);
				records.add(record);
				count++;
				totalSize ++;
				if(count >= batchsize || parrelTranCommand.splitCheck(totalSize)){
                    TaskCommandContext taskCommandContext = new TaskCommandContext();
                    taskCommandContext.setTotalCount(totalCount);
                    taskCommandContext.setDataSize(count);
                    taskCommandContext.setTaskNo(taskNo);
                    taskCommandContext.setLastValue(lastValue);
                    taskCommandContext.setCommonRecords(records);
                    taskCommandContext.setService(service);
                    taskCommandContext.setTasks(tasks);
                    taskCommandContext.setTranErrorWrapper(tranErrorWrapper);
                    taskCommandContext.setIgnoreCount(ignoreCount);
                    taskCommandContext.setImportContext(importContext);
					count = 0;
                    droped = 0;
                    ignoreCount = 0;

					taskNo = parrelTranCommand.hanBatchActionTask(taskCommandContext);
					records = new ArrayList<>();

				}


			}
			if (count > 0) {
				if(!tranErrorWrapper.assertCondition()) {
                    Throwable e = tranErrorWrapper.throwError();
					if(e != null){
						throw e;
					}


				}
                TaskCommandContext taskCommandContext = new TaskCommandContext();
                taskCommandContext.setTotalCount(totalCount);
                taskCommandContext.setDataSize(count);
                taskCommandContext.setTaskNo(taskNo);
                taskCommandContext.setLastValue(lastValue);
                taskCommandContext.setCommonRecords(records);
                taskCommandContext.setService(service);
                taskCommandContext.setTasks(tasks);
                taskCommandContext.setTranErrorWrapper(tranErrorWrapper);
                taskCommandContext.setIgnoreCount(ignoreCount);
                taskCommandContext.setImportContext(importContext);
				taskNo = parrelTranCommand.hanBatchActionTask(taskCommandContext);

			}
			if(baseDataTran.isPrintTaskLog()){
                StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                logger.info(builder.append("Pararrel batch submit tasks:").append(taskNo).toString());
            }


		} catch (DataImportException e) {
			exception = e;
			throw e;
		} catch (Exception e) {
			exception = e;
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}catch (Throwable e) {
            exception = e;
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
        }
		finally {
			baseDataTran.waitTasksComplete(tasks, service, exception, lastValue, totalCount, tranErrorWrapper, new WaitTasksCompleteCallBack() {
				@Override
				public void call() {
					parrelTranCommand.parrelCompleteAction();

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

//		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
		long lastSend = 0;
        LastValueWrapper currentLastValueWrapper = currentStatus != null? currentStatus.getCurrentLastValueWrapper():null;
        LastValueWrapper lastValue = null;
		ImportCount importCount = new SerialImportCount( );
		long totalCount = 0;
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
		boolean reachEOFClosed = false;
        int ignoreCount  = 0;
		try {

			Object temp = null;

			//十分钟后打印一次等待日志数据，打印后，就等下次
			long logInterval = 1l * 60l * 1000l;
			boolean printed = false;
			BatchContext batchContext = new BatchContext();
//			BuildMapDataContext buildMapDataContext = buildMapDataContext( importContext);
			while (true) {
                NextAssert hasNext = tranResultSet.next();
				if(hasNext.isNeedFlush()){
					if(baseDataTran.isPrintTaskLog() && !printed) {
						if (lastSend > 0l) {//等待状态下，需一次打印日志
							long end = System.currentTimeMillis();
							long interval = end - lastSend;
							if (interval >= logInterval) {
								logger.info(new StringBuilder().append("Auto Log Send datas Take time:").append((end - start)).append("ms")
										.append(",Send total ").append(totalCount).append(" records,IgnoreTotalCount ")
										.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
										.append(importCount.getFailedCount()).append(" records.").append("Force FlushInterval[").append(importContext.getFlushInterval()).append("ms]").toString());
								lastSend = 0l;
								printed = true;
							}


						}
						else{
							lastSend = System.currentTimeMillis();
						}
					}
                    if(!hasNext.isHasNext())
					    continue;
				}
				if(!hasNext.isHasNext()){
					break;
				}
				lastSend = 0l;
				printed = false;
				try {
                    Record resultRecord = new WrappedRecord(tranResultSet);;
					if (lastValue == null)
						lastValue = importContext.max(currentLastValueWrapper, resultRecord);
					else {
						lastValue = importContext.max(lastValue,resultRecord);
					}
                    if(resultRecord.isRecordDirectIgnore()){
                        continue;
                    }
//					Context context = new ContextImpl(importContext, tranResultSet, null);
					

					if(!reachEOFClosed)
						reachEOFClosed = resultRecord.reachEOFClosed();
					if(resultRecord.removed()){
						if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
                            importCount.increamentIgnoreTotalCount();
                            ignoreCount ++;
                        }
						else
							importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);
						continue;
					}
                    Context context = importContext.buildContext(baseDataTran.getTaskContext(), resultRecord, batchContext);

                    context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
                        ignoreCount ++;
						continue;
					}
					CommonRecord record = importContext.getOutputPlugin().buildRecord(context);
//					super.metricsMap(record,buildMapDataContext,importContext);
                    
					totalCount++;
                    TaskCommandContext taskCommandContext = new TaskCommandContext();
                    taskCommandContext.setTotalCount(importCount);
                    taskCommandContext.setDataSize(1);
                    taskCommandContext.setTaskNo(-1);
                    taskCommandContext.setLastValue(lastValue);
                    taskCommandContext.setCommonRecord(record);
                    taskCommandContext.setIgnoreCount(ignoreCount);
                    taskCommandContext.setImportContext(importContext);
                    ignoreCount = 0;
                    
					serialTranCommand.hanBatchActionTask(taskCommandContext);
                    
					if(totalCount == Long.MAX_VALUE) {
						if(baseDataTran.isPrintTaskLog()) {
							long end = System.currentTimeMillis();
                            StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                            logger.info(builder.append("Send datas  Take time:").append((end - start)).append("ms")
									.append(",Send total").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
									.append(importCount.getFailedCount()).append(" records. totalCount has reach Long.MAX_VALUE and reset").toString());

						}
						totalCount = 0;
					}
					else{
						if(baseDataTran.isPrintTaskLog() && importContext.getLogsendTaskMetric() > 0l && (totalCount % importContext.getLogsendTaskMetric()) == 0l) {//每一万条记录打印一次日志
							long end = System.currentTimeMillis();
                            StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                            logger.info(builder.append("Send datas Take time:").append((end - start)).append("ms")
									.append(",Send total ").append(totalCount).append(" records,IgnoreTotalCount ")
									.append(importCount.getIgnoreTotalCount()).append(" records,FailedTotalCount ")
									.append(importCount.getFailedCount()).append(" records.").toString());

						}
					}
				} catch (Exception e) {
					throw ImportExceptionUtil.buildDataImportException(importContext,e);
				}
			}

            TaskCommandContext taskCommandContext = new TaskCommandContext();
            taskCommandContext.setTotalCount(importCount);
            taskCommandContext.setDataSize(-1);
            taskCommandContext.setTaskNo(-1);
            taskCommandContext.setLastValue(lastValue);
            taskCommandContext.setCommonRecord(null);
            taskCommandContext.setIgnoreCount(ignoreCount);
            taskCommandContext.setImportContext(importContext);
            ignoreCount = 0;

            serialTranCommand.endSerialActionTask(taskCommandContext);
			if(baseDataTran.isPrintTaskLog()) {
				long end = System.currentTimeMillis();
                StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                logger.info(builder.append("Send datas Take time:").append((end - start)).append("ms")
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
			throw ImportExceptionUtil.buildDataImportException(importContext,e);


		} finally {

            baseDataTran.stop2ndClearResultsetQueue(exception != null);// a{2}

			baseDataTran.endJob( reachEOFClosed, importCount, exception);
		}
		return null;

	}

	private String serialExecuteAllRecoreds(SerialTranCommand serialTranCommand,
											Status currentStatus,
											ImportContext importContext,
											TranResultSet tranResultSet, BaseDataTran baseDataTran){

//		logger.info("serial import data Execute started.");
		List<CommonRecord> records = new ArrayList<>();
//		Object lastValue = null;
		Exception exception = null;
		long start = System.currentTimeMillis();
//		Status currentStatus = importContext.getCurrentStatus();
//		Object currentValue = currentStatus != null? currentStatus.getLastValue():null;
        LastValueWrapper currentValue = currentStatus != null? currentStatus.getCurrentLastValueWrapper():null;
        LastValueWrapper lastValue = null;
		ImportCount importCount = new SerialImportCount( );
		BaseCommonRecordDataTran baseCommonRecordDataTran = (BaseCommonRecordDataTran)baseDataTran;
		int taskNo = 0;
		long totalCount = 0;
		int count = 0;
		long ignoreTotalCount = 0;
		boolean reachEOFClosed = false;
        int ignoreCount = 0;

		try {

			//		GetCUDResult CUDResult = null;
			Object temp = null;

//			List<DBRecord> records = new ArrayList<DBRecord>();
			BatchContext batchContext = new BatchContext();
//			BuildMapDataContext buildMapDataContext = buildMapDataContext( importContext);
			while (true) {
                NextAssert hasNext = tranResultSet.next();
				if(hasNext.isNeedFlush()){
					if(records.size() > 0) {
                        TaskCommandContext taskCommandContext = new TaskCommandContext();
                        taskCommandContext.setTotalCount(importCount);
                        taskCommandContext.setDataSize((int)totalCount);
                        taskCommandContext.setTaskNo(taskNo);
                        taskCommandContext.setLastValue(lastValue);
                        taskCommandContext.setCommonRecords(records);
                        taskCommandContext.setIgnoreCount(ignoreCount);
                        taskCommandContext.setImportContext(importContext);
                        ignoreCount = 0;
						taskNo = serialTranCommand.hanBatchActionTask(taskCommandContext);
						records = new ArrayList<>();
					}

					if(baseDataTran.isPrintTaskLog()) {

						long end = System.currentTimeMillis();
                        StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                        logger.info(builder.append("Force flush datas Take time:").append((end - start)).append("ms")
								.append(",Import total ").append(totalCount).append(" records,IgnoreTotalCount ")
								.append(ignoreTotalCount).append(" records.").toString());

					}
                    if(!hasNext.isHasNext())
                        continue;
				}
				if(!hasNext.isHasNext()){
					break;
				}
				try {
                    Record resultRecord = new WrappedRecord(tranResultSet);;
					if (lastValue == null)
						lastValue = importContext.max(currentValue, resultRecord);
					else {
						lastValue = importContext.max(lastValue, resultRecord);
					}
                    if(resultRecord.isRecordDirectIgnore()){
                        continue;
                    }
					if(!reachEOFClosed)
						reachEOFClosed = resultRecord.reachEOFClosed();
					if(resultRecord.removed()){
						if(!reachEOFClosed){//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
							importCount.increamentIgnoreTotalCount();
                            ignoreCount ++;
                        }
						else
							importContext.flushLastValue(lastValue,   currentStatus,reachEOFClosed);

						continue;
					}
                    Context context = importContext.buildContext(baseDataTran.getTaskContext(),resultRecord, batchContext);

                    context.refactorData();
					context.afterRefactor();
					if (context.isDrop()) {
						importCount.increamentIgnoreTotalCount();
                        ignoreCount ++;
						continue;
					}
					CommonRecord record = importContext.getOutputPlugin().buildRecord(  context );
//					super.metricsMap(record,buildMapDataContext,importContext);
					records.add(record);

//					fileUtil.writeData(fileFtpOupputContext.generateReocord(record));
//					//						evalBuilk(this.tranResultSet, batchContext, writer, context, "index", clientInterface.isVersionUpper7());
					totalCount++;
					count ++;
					if(serialTranCommand.splitCheck(totalCount)){//reached max file record size
                        TaskCommandContext taskCommandContext = new TaskCommandContext();
                        taskCommandContext.setTotalCount(importCount);
                        taskCommandContext.setDataSize(count);
                        taskCommandContext.setTaskNo(taskNo);
                        taskCommandContext.setLastValue(lastValue);
                        taskCommandContext.setCommonRecords(records);
                        taskCommandContext.setIgnoreCount(ignoreCount);
                        taskCommandContext.setImportContext(importContext);
                        ignoreCount = 0;
						count = 0;
						taskNo = serialTranCommand.hanBatchActionTask(taskCommandContext);
						records = new ArrayList<>();
					}

				} catch (Exception e) {
					throw ImportExceptionUtil.buildDataImportException(importContext,e);
				}
			}
            TaskCommandContext taskCommandContext = new TaskCommandContext();
            taskCommandContext.setTotalCount(importCount);
            taskCommandContext.setDataSize((int)totalCount);
            taskCommandContext.setTaskNo(taskNo);
            taskCommandContext.setLastValue(lastValue);
            taskCommandContext.setCommonRecords(records);
            taskCommandContext.setIgnoreCount(ignoreCount);
            taskCommandContext.setImportContext(importContext);
			taskNo = serialTranCommand.endSerialActionTask(taskCommandContext);
			if(baseDataTran.isPrintTaskLog()) {
				long end = System.currentTimeMillis();
                StringBuilder builder = builderJobInfo(new StringBuilder(),  importContext);
                logger.info(builder.append("Serial import Take time:").append((end - start)).append("ms")
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
			throw ImportExceptionUtil.buildDataImportException(importContext,e);


		} finally {

            /**
			if(!TranErrorWrapper.assertCondition(exception ,importContext)){

                baseDataTran.stop();
			}
			if(importContext.isCurrentStoped()){

                baseDataTran.stop();

			}*/

            baseDataTran.stop2ndClearResultsetQueue(exception != null);// a{3}
//			Date endTime = new Date();
//			if(baseDataTran.getTaskContext() != null)
//				baseDataTran.getTaskContext().setJobEndTime(endTime);
//			importCount.setJobEndTime(endTime);
			baseDataTran.endJob(  reachEOFClosed, importCount, exception);
		}
		return null;

	}
}
