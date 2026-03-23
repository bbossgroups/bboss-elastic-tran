package org.frameworkset.tran.plugin.feishu.output;

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

public class FeishuOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
	protected FeishuTableOutputConfig feishuTableOutputConfig;
    public FeishuOutPutDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
 
	@Override
	public void logTaskStart(Logger logger) {
		logger.info(taskInfo + " start.");
	}
	protected String taskInfo ;
  
	public void init(){
		super.init();
 		taskInfo = "Feishu table output datatran job";

	}

	public FeishuOutPutDataTran(TaskContext taskContext, TranResultSet tranResultSet, ImportContext importContext, JobCountDownLatch countDownLatch, Status currentStatus) {
		super(taskContext,tranResultSet,importContext,   currentStatus);
		this.countDownLatch = countDownLatch;
		feishuTableOutputConfig = (FeishuTableOutputConfig) importContext.getOutputConfig();
	}
 

    @Override
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext) {
        return new FeishuTaskCommandImpl(  taskCommandContext,outputPlugin.getOutputConfig());
    }
 







}
