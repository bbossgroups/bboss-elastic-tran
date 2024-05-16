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

import org.frameworkset.tran.Record;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.ParallImportCount;
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
public class CommonAsynRecordTranJob extends CommonRecordTranJob{
	private static Logger logger = LoggerFactory.getLogger(CommonAsynRecordTranJob.class);
	 

	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
    @Override
	public String parallelBatchExecute(final ParrelTranCommand parrelTranCommand ,
										Status currentStatus,
										ImportContext importContext,
										TranResultSet tranResultSet, BaseDataTran baseDataTran){

		int count = 0;
        int droped = 0;
		long totalSize = 0;
		List<Record> records = new ArrayList<>();
		String ret = null;
		ExecutorService	service = importContext.buildThreadPool();
//        ExecutorService buildRecordHandlerExecutor = importContext.buildRecordHandlerExecutor();
		List<Future> tasks = new ArrayList<Future>();
		int taskNo = 0;
		ImportCount totalCount = new ParallImportCount();
        Throwable exception = null;
        LastValueWrapper currentLastValueWrapper = currentStatus != null? currentStatus.getCurrentLastValueWrapper():null;
        LastValueWrapper lastValue = null;
		TranErrorWrapper tranErrorWrapper = new TranErrorWrapper(importContext);
		int batchsize = importContext.getStoreBatchSize();
		boolean reachEOFClosed = false;
        int ignoreCount = 0;
        long istart = 0;
        long end = 0;
		try {

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
                        taskCommandContext.setRecords(records);
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
                Record resultRecord = new WrappedRecord(tranResultSet);
				if(lastValue == null)
					lastValue = importContext.max(currentLastValueWrapper,resultRecord);
				else{
					lastValue = importContext.max(lastValue,resultRecord);
				}
                if(resultRecord.isRecordDirectIgnore()){
                    droped ++;
                    continue;
                }
//				Context context = importContext.buildContext(baseDataTran.getTaskContext(),resultRecord, batchContext);
				if(!reachEOFClosed)
					reachEOFClosed = resultRecord.reachEOFClosed();
//				Context context = new ContextImpl(importContext, tranResultSet, batchContext);
				if(resultRecord.removed()){
					if(!reachEOFClosed) {//如果是文件末尾，那么是空行记录，不需要记录忽略信息，
                        totalCount.increamentIgnoreTotalCount();
                        ignoreCount ++;
                    }
					continue;
				}
                records.add(resultRecord);
                /**
				context.refactorData();
				context.afterRefactor();
				if (context.isDrop()) {
					totalCount.increamentIgnoreTotalCount();
                    droped ++;
					continue;
				}
				CommonRecord record = baseCommonRecordDataTran.buildRecord(  context );
//				super.metricsMap(record,buildMapDataContext,importContext);
				records.add(record);
                 */
				count++;
				totalSize ++;
				if(count >= batchsize || parrelTranCommand.splitCheck(totalSize)){
                    TaskCommandContext taskCommandContext = new TaskCommandContext();
                    taskCommandContext.setTotalCount(totalCount);
                    taskCommandContext.setDataSize(count);
                    taskCommandContext.setTaskNo(taskNo);
                    taskCommandContext.setLastValue(lastValue);
                    taskCommandContext.setRecords(records);
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
                taskCommandContext.setRecords(records);
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


 
}
