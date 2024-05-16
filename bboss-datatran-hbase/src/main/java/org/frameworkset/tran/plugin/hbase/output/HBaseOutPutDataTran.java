package org.frameworkset.tran.plugin.hbase.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;

public class HBaseOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
	protected HBaseOutputConfig hBaseOutputConfig ;

	@Override
	public void init(){
		super.init();
		hBaseOutputConfig = (HBaseOutputConfig) importContext.getOutputConfig();
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
	protected TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
//		return new HBaseTaskCommandImpl( totalCount, importContext, records,
//				taskNo, taskContext.getJobNo(),taskInfo,lastValue,  currentStatus,taskContext);
        return new HBaseTaskCommandImpl(   taskCommandContext);
	}




}
