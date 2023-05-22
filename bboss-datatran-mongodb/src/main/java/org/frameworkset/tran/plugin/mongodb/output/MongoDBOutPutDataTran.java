package org.frameworkset.tran.plugin.mongodb.output;

import org.frameworkset.tran.AbstraCommonRecordOutPutDataTran;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.TaskCommand;

import java.util.List;

public class MongoDBOutPutDataTran extends AbstraCommonRecordOutPutDataTran {
	protected MongoDBOutputConfig mongoDBOutputConfig ;

	@Override
	public void init(){
		super.init();
		mongoDBOutputConfig = (MongoDBOutputConfig) importContext.getOutputConfig();
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
	protected TaskCommand buildTaskCommand(ImportCount totalCount,
													List<CommonRecord> records, int taskNo,
													Object lastValue,  boolean reachEOFClosed){
		return new MongoDBTaskCommandImpl( totalCount, importContext, records,
				taskNo, taskContext.getJobNo(),taskInfo,lastValue,  currentStatus,reachEOFClosed,taskContext);
	}




}
