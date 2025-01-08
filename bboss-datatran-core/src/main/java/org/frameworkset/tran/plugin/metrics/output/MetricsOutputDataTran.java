package org.frameworkset.tran.plugin.metrics.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;

public class MetricsOutputDataTran extends AbstraCommonRecordOutPutDataTran {
	private MetricsOutputConfig metricsOutputConfig;
    public MetricsOutputDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	@Override
	public void init(){
		super.init();
		metricsOutputConfig = (MetricsOutputConfig) outputPlugin.getOutputConfig();
		taskInfo = "Import data to metrics.";
	}
	public MetricsOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(taskContext, jdbcResultSet, importContext, currentStatus, countDownLatch);
	}

	public MetricsOutputDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext, currentStatus);
	}

	@Override
	public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext) {
        MetricsOutputConfig metricsOutputConfig = this.metricsOutputConfig;
        if(metricsOutputConfig == null){
            metricsOutputConfig = (MetricsOutputConfig) outputPlugin.getOutputConfig();            
        }
		return new MetricsTaskCommandImpl(   taskCommandContext,metricsOutputConfig);



	}
}
