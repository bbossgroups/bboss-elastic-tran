package org.frameworkset.tran.plugin.db.output;

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

public class AsynDBOutPutDataTran extends DBOutPutDataTran {
	private CountDownLatch countDownLatch;


	public AsynDBOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super( taskContext,jdbcResultSet,importContext,    currentStatus);
	}
	public AsynDBOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, CountDownLatch countDownLatch,Status currentStatus) {
		super( taskContext,jdbcResultSet,importContext,     currentStatus);
		this.countDownLatch = countDownLatch;
	}
//	public void appendData(ESDatas datas){
//		esTranResultSet.appendData(new ESDatasWraper(datas));
//	}


	@Override
	public void stop(){
		if(asynTranResultSet != null) {
			asynTranResultSet.stop();
			asynTranResultSet = null;
		}
		super.stop();
	}
	/**
	 * 只停止转换作业，没有处理完成的数据会继续处理完成
	 */
	@Override
	public void stopTranOnly(){
		if(asynTranResultSet != null) {
			asynTranResultSet.stopTranOnly();
			asynTranResultSet = null;
		}
		super.stopTranOnly();
	}

	@Override
	public String tran() throws DataImportException {
		try {
			return super.tran();
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}

	}
}
