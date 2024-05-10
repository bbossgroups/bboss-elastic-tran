package org.frameworkset.tran;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class AbstraCommonRecordOutPutDataTran extends BaseCommonRecordDataTran {

//	protected JobCountDownLatch countDownLatch;


	public AbstraCommonRecordOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus, JobCountDownLatch countDownLatch) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
		this.countDownLatch = countDownLatch;
	}

	public AbstraCommonRecordOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}

	protected abstract TaskCommand buildTaskCommand(ImportCount totalCount,
													List<CommonRecord> records, int taskNo,
                                                    LastValueWrapper lastValue);
	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas,
										  ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					TaskCommand taskCommand = buildTaskCommand(   totalCount,
							  records,   taskNo,lastValue);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas){
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
					TaskCommand taskCommand = buildTaskCommand( totalCount,  records,
							taskNo, lastValue);
					TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//					TaskCall.call(taskCommand);
//					taskNo++;
//				}
				return action(totalCount, dataSize, taskNo, lastValue, datas);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas) {
				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas);
				return taskNo;

			}


		};
	}

	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}



}
