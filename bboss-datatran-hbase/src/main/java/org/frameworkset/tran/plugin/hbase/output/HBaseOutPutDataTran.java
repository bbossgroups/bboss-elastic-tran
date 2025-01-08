package org.frameworkset.tran.plugin.hbase.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;

public class HBaseOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
	protected HBaseOutputConfig hBaseOutputConfig ;
    public HBaseOutPutDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	@Override
	public void init(){
		super.init();
        if(hBaseOutputConfig == null)
		    hBaseOutputConfig = (HBaseOutputConfig) outputPlugin.getOutputConfig();
		StringBuilder builder = new StringBuilder();

		if(hBaseOutputConfig != null){
			builder.append("Import data to hbase[").append(hBaseOutputConfig.getHbaseTable())
					.append("]");
		}

		taskInfo = builder.toString();
	}
	public HBaseOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus,countDownLatch);
	}

	public HBaseOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}
    @Override
	public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        return new HBaseTaskCommandImpl(   taskCommandContext,outputPlugin.getOutputConfig());
	}




}
