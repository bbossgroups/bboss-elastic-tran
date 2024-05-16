package org.frameworkset.tran;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;


public abstract class AbstraCommonRecordOutPutDataTran extends BaseCommonRecordDataTran {

//	protected JobCountDownLatch countDownLatch;


	public AbstraCommonRecordOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
		this.countDownLatch = countDownLatch;
	}

	public AbstraCommonRecordOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}

 

    protected abstract TaskCommand buildTaskCommand(TaskCommandContext taskCommandContext);
    protected TaskCommand _buildTaskCommand(TaskCommandContext taskCommandContext){
//        taskCommandContext.setTaskContext(taskContext);
//        taskCommandContext.setJobNo(taskContext.getJobNo());
//        taskCommandContext.setCurrentStatus(currentStatus);
//        taskCommandContext.setTaskInfo(taskInfo);
//        taskCommandContext.evalDataSize();
        initTaskCommandContext( taskCommandContext);
        return buildTaskCommand(taskCommandContext);
    }
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
//                List<Record> records = taskCommandContext.getRecords();                
//				List<CommonRecord> commonRecords = convertDatas( taskCommandContext);
                
				if(taskCommandContext.containData())  {
                    taskCommandContext.increamentTaskNo();
                    TaskCommand taskCommand = _buildTaskCommand(taskCommandContext);
//					TaskCommand taskCommand = buildTaskCommand( taskCommandContext);
                    taskCommandContext.addTask( taskCommand);

				}
				return taskCommandContext.getTaskNo();
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(TaskCommandContext taskCommandContext){
//				List<CommonRecord> records = convertDatas( datas);
				if(taskCommandContext.containData())  {
                    taskCommandContext.increamentTaskNo();
//					taskNo++;
					TaskCommand taskCommand = _buildTaskCommand( taskCommandContext);
					TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);

				}
				return taskCommandContext.getTaskNo();
			}
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//					TaskCall.call(taskCommand);
//					taskNo++;
//				}
				return action(taskCommandContext);
			}

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
                return action(taskCommandContext);

			}


		};
	}

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}



}
