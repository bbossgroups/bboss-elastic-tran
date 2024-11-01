package org.frameworkset.tran.plugin.milvus.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;


public class MilvusOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
    protected MilvusOutputConfig milvusOutputConfig;

	@Override
	public void init(){
		super.init();
        milvusOutputConfig = (MilvusOutputConfig) importContext.getOutputConfig();
		StringBuilder builder = new StringBuilder();

		if(milvusOutputConfig != null){
			builder.append("Import data to milvus[").append(milvusOutputConfig.getDbName())
					.append("] collection[").append(milvusOutputConfig.getCollectionName())
                    .append("] partitionName[").append(milvusOutputConfig.getPartitionName())
					.append("]");
		}

		taskInfo = builder.toString();
	}
	public MilvusOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus,countDownLatch);
	}

	public MilvusOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}
    @Override
	protected TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
			return new MilvusTaskCommandImpl(taskCommandContext);
	}




}
