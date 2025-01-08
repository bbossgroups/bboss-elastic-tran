package org.frameworkset.tran.plugin.mongodb.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;
import org.frameworkset.tran.task.TaskCommandContext;


public class MongoDBOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
	protected MongoDBOutputConfig mongoDBOutputConfig ;
    public MongoDBOutPutDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }
	@Override
	public void init(){
		super.init();
        if(mongoDBOutputConfig == null)
		    mongoDBOutputConfig = (MongoDBOutputConfig) outputPlugin.getOutputConfig();
		StringBuilder builder = new StringBuilder();

		if(mongoDBOutputConfig != null){
			builder.append("Import data to mongodb[").append(mongoDBOutputConfig.getDB())
					.append("] collection[").append(mongoDBOutputConfig.getDBCollection())
					.append("]");
		}

		taskInfo = builder.toString();
	}
	public MongoDBOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus,JobCountDownLatch countDownLatch) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus,countDownLatch);
	}

	public MongoDBOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}
    @Override
    public TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext){
        MongoDBOutputConfig mongoDBOutputConfig = this.mongoDBOutputConfig;
        if(mongoDBOutputConfig == null){
            mongoDBOutputConfig = (MongoDBOutputConfig) outputConfig;
        }
		if(!mongoDBOutputConfig.isMultiCollections()) {
			return new MongoDBTaskCommandImpl(taskCommandContext,mongoDBOutputConfig);
		}
		else{
			return new MongoDBMultiTargetTaskCommandImpl(  taskCommandContext,mongoDBOutputConfig);
		}
	}




}
