package org.frameworkset.tran.plugin.custom.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;
import org.slf4j.Logger;

public class CustomOutPutDataTran extends AbstraCommonRecordOutPutDataTran {


	protected String taskInfo ;
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo + " start.");
	}





	public void init(){
		super.init();
		taskInfo = new StringBuilder().append("Import data to custom output.").toString();


	}



	public CustomOutPutDataTran(BaseDataTran baseDataTran) {
		super(baseDataTran);
	}
	public CustomOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,  JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,jdbcResultSet,importContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}

    @Override
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        CustomTaskCommandImpl taskCommand = new CustomTaskCommandImpl(taskCommandContext,this.outputPlugin.getOutputConfig());
        return taskCommand;
    }


}
