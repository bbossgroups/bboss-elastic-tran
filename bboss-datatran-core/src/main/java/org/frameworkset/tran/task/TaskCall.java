package org.frameworkset.tran.task;
/**
 * Copyright 2008 biaoping.yin
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
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/8/29 21:27
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskCall implements Runnable {
	private static Logger logger = LoggerFactory.getLogger(TaskCall.class);
	private TranErrorWrapper errorWrapper;

	private ImportContext db2ESImportContext;
	private TaskCommand taskCommand;


	public TaskCall(TaskCommand taskCommand,
					TranErrorWrapper errorWrapper){
		this.taskCommand = taskCommand;
		this.errorWrapper = errorWrapper;
		this.db2ESImportContext = taskCommand.getImportContext();
	}


	protected boolean isPrintTaskLog(){
		return db2ESImportContext.isPrintTaskLog() && logger.isInfoEnabled();
	}

    public static void handleException(Throwable e,ImportCount importCount,TaskMetrics taskMetrics,TaskCommand taskCommand,ImportContext importContext){
        long[] metrics = importCount.increamentFailedCount(taskCommand.getDataSize());
        taskMetrics.setFailedRecords(taskCommand.getDataSize());
        taskMetrics.setRecords(taskMetrics.getFailedRecords());
        taskMetrics.setLastValue(taskCommand.getLastValue());
        taskMetrics.setTotalRecords(metrics[1]);
        taskMetrics.setTotalFailedRecords(metrics[0]);
        long ignoreTotalCount = importCount.getIgnoreTotalCount();
        taskMetrics.setIgnoreRecords(ignoreTotalCount - taskMetrics.getTotalIgnoreRecords());
        taskMetrics.setTotalIgnoreRecords(ignoreTotalCount);
        taskMetrics.setTaskEndTime(new Date());
        if (importContext.getExportResultHandler() != null) {
            try {
                importContext.getExportResultHandler().handleException(taskCommand, e);
            }
            catch (Exception ee){
                logger.warn("",e);
            }
        }
    }
    private static BuildMapDataContext buildMapDataContext(ImportContext importContext){
        List<ETLMetrics> etlMetrics = importContext.getMetrics();
        BuildMapDataContext buildMapDataContext = null;
        if(etlMetrics != null) {
            buildMapDataContext = new BuildMapDataContext();
            String dataTimeField = importContext.getDataTimeField();
            buildMapDataContext.setDataTimeField(dataTimeField);
        }
        return buildMapDataContext;
    }

    private static void metricsCompute(ImportContext importContext,List<CommonRecord> records){
        if(importContext.getMetrics() == null || importContext.getMetrics().size() == 0)
            return;
        BuildMapDataContext buildMapDataContext = buildMapDataContext( importContext);
        for(CommonRecord commonRecord:records){
            BaseTranJob. map(  commonRecord,   buildMapDataContext,   importContext.getMetrics(),  importContext.isUseDefaultMapData());

        }
    }

    /**
     * 第二阶段数据加工处理
     */
    private static List<CommonRecord> dataRefactor(TaskCommand taskCommand) throws Exception {
        TaskCommandContext taskCommandContext = taskCommand.getTaskCommandContext();
        if(taskCommandContext.getCommonRecords() != null){
            taskCommand.setRecords(taskCommandContext.getCommonRecords());
        }
        else if(taskCommandContext.getCommonRecord() != null){
            List<CommonRecord> commonRecords = new ArrayList<>(1);
            commonRecords.add(taskCommandContext.getCommonRecord());
            taskCommand.setRecords(commonRecords);            
        }
        else if(taskCommandContext.getRecords() != null){
            List<CommonRecord> commonRecords = new ArrayList<>();

            List<Record> records = taskCommandContext.getRecords();
            BatchContext batchContext = new BatchContext();
            ImportContext importContext = taskCommand.getImportContext();
            ImportCount totalCount = taskCommandContext.getTotalCount();
            int droped = 0;
            for (Record resultRecord : records) {
                Context context = importContext.buildContext(taskCommand.getTaskContext(), resultRecord, batchContext);
                context.refactorData();
                context.afterRefactor();
                if (context.isDrop()) {
                    totalCount.increamentIgnoreTotalCount();
                    taskCommandContext.increamentDataSize(-1);
                    taskCommandContext.increamentIgnoreCount();
                    droped++;
                    continue;
                }
                CommonRecord record = importContext.getOutputPlugin().buildRecord(context);
                commonRecords.add(record);
            }
            taskCommandContext.setCommonRecords(commonRecords);
            taskCommand.setRecords(commonRecords);
            taskCommandContext.setDroped(droped);
        }
        return taskCommand.getRecords();
    }
	public static <RESULT> RESULT call(TaskCommand<RESULT> taskCommand){
		ImportContext importContext = taskCommand.getImportContext();
		ImportCount importCount = taskCommand.getImportCount();
		TaskMetrics taskMetrics = taskCommand.getTaskMetrics();

		try {
            dataRefactor(taskCommand);
            //指标计算
            metricsCompute(  importContext,taskCommand.getRecords());
			taskCommand.init();
			RESULT data = taskCommand.execute();
			Date endTime = new Date();
			long[] metrics = importCount.increamentSuccessCount((long)taskCommand.getDataSize());
			taskMetrics.setTotalSuccessRecords(metrics[0]);
			taskMetrics.setLastValue(taskCommand.getLastValue());
			taskMetrics.setTotalRecords(metrics[1]);
			taskMetrics.setSuccessRecords((long)taskCommand.getDataSize());
			taskMetrics.setRecords(taskMetrics.getSuccessRecords());
			long ignoreTotalCount = importCount.getIgnoreTotalCount();
			taskMetrics.setIgnoreRecords(taskCommand.getTaskCommandContext().getIgnoreCount());
			taskMetrics.setTotalIgnoreRecords(ignoreTotalCount);
			taskMetrics.setTaskEndTime(endTime);
			if (importContext.getExportResultHandler() != null) {//处理返回值
				try {
					importContext.getExportResultHandler().handleResult(taskCommand, data);
				}
				catch (Exception e){
					logger.warn("",e);
				}
			}
			return data;
		}
		catch (DataImportException e){

            handleException(  e,  importCount,  taskMetrics,  taskCommand,  importContext);
            throw e;
		}
		catch (Exception e){

            handleException(  e,  importCount,  taskMetrics,  taskCommand,  importContext);
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}

        catch (Throwable e){

            handleException(  e,  importCount,  taskMetrics,  taskCommand,  importContext);
            throw ImportExceptionUtil.buildDataImportException(importContext,e);
        }
		finally {
			taskCommand.finished();
		}


	}

	@Override
	public void run()   {
		if(!errorWrapper.assertCondition()) {
			if(logger.isWarnEnabled())
				logger.warn(new StringBuilder().append("Task[").append(taskCommand.getTaskNo()).append("] Assert Execute Condition Failed, Ignore").toString());
			return;
		}
		long start = System.currentTimeMillis();
		StringBuilder info = null;
		if(isPrintTaskLog()) {
			info = new StringBuilder();
		}
		try {
			if(isPrintTaskLog()) {
                    BaseTranJob.builderJobInfo(info,db2ESImportContext);
					info.append("Task[").append(taskCommand.getTaskNo()).append("] starting ......");
					logger.info(info.toString());

			}
            
			call(taskCommand);
			if(isPrintTaskLog()) {
				long end = System.currentTimeMillis();
				info.setLength(0);
                BaseTranJob.builderJobInfo(info,db2ESImportContext);
				info.append("Task[").append(taskCommand.getTaskNo()).append("] finish,import ")
						.append(taskCommand.getDataSize())
						.append(" records,Total import ")
						.append(taskCommand.getTaskMetrics().getTotalSuccessRecords()).append(" records,Take time:")
						.append((end - start)).append("ms");
				logger.info(info.toString());
			}
		}
		catch (Exception e){
			errorWrapper.setError(e);
			if(!db2ESImportContext.isContinueOnError()) {
				if (isPrintTaskLog()) {
					long end = System.currentTimeMillis();
					info.setLength(0);
                    BaseTranJob.builderJobInfo(info,db2ESImportContext);
					info.append("Task[").append(taskCommand.getTaskNo()).append("] failed: ")
						.append(taskCommand.getDataSize())
						.append(" records, Take time:").append((end - start)).append("ms");
					logger.info(info.toString());
				}
                StringBuilder stringBuilder = BaseTranJob.builderJobInfo(new StringBuilder(),db2ESImportContext);
				throw new TaskFailedException(stringBuilder.append("Task[").append(taskCommand.getTaskNo()).append("] Execute Failed: ")
						.append(taskCommand.getDataSize())
						.append(" records,").toString(), e);
			}
			else
			{
				long end = System.currentTimeMillis();
				if(info == null){
					info = new StringBuilder();
				}
				else {
					info.setLength(0);
				}
                BaseTranJob.builderJobInfo(info,db2ESImportContext);
				info.append("Task[").append(taskCommand.getTaskNo()).append("] failed: ")
					.append(taskCommand.getDataSize())
					.append(" records,but continue On Error! Take time:").append((end - start)).append("ms");
				logger.warn(info.toString(),e);


			}

		}



	}
}
