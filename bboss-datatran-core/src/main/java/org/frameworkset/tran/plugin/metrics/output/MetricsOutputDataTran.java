package org.frameworkset.tran.plugin.metrics.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;

public class MetricsOutputDataTran extends AbstraCommonRecordOutPutDataTran {
	private MetricsOutputConfig metricsOutputConfig;
	@Override
	public void init(){
		super.init();
		metricsOutputConfig = (MetricsOutputConfig) importContext.getOutputConfig();
		taskInfo = "Import data to metrics.";
	}
	public MetricsOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(taskContext, jdbcResultSet, importContext, currentStatus, countDownLatch);
	}

	public MetricsOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext, currentStatus);
	}

	@Override
	protected TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext) {
		return new MetricsTaskCommandImpl(   taskCommandContext);



	}
}
