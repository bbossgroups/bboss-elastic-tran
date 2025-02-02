package org.frameworkset.tran.plugin.multi.output;

import org.frameworkset.tran.*;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Future;

/**
 * 提供过滤记录集功能的数据转换器
 */
public class FilterMultiOutPutDataTran extends MultiOutPutDataTran {

    protected OutputRecordsFilter outputRecordsFilter;

    public FilterMultiOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
        super(  taskContext,   jdbcResultSet,   importContext,   countDownLatch,    currentStatus);
        outputRecordsFilter = multiOutputConfig.getOutputRecordsFilter();
    }
 
	public FilterMultiOutPutDataTran(BaseDataTran baseDataTran) {
		super(baseDataTran);
	}
 
    @Override
    public  TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        MultiTaskCommandImpl taskCommand = new MultiTaskCommandImpl(taskCommandContext, (records,executorService) -> {
            List<Future> tasks = new ArrayList<>();
            records = Collections.unmodifiableList(records);
            List<CommonRecord> filterRecords = null;
            for(BaseDataTran baseDataTran:baseDataTrans){
                OutputConfig outputConfig = baseDataTran.getOutputConfig();
                filterRecords = outputRecordsFilter.filter(outputConfig,records);
                TaskCommand taskCommand1 = baseDataTran.buildTaskCommand(taskCommandContext);
                taskCommand1.setRecords(filterRecords);
                taskCommandContext.addMultiOutputTask(executorService,tasks,taskCommand1);
            }
            return tasks;
        },outputPlugin.getOutputConfig());
        return taskCommand;
    }

}
