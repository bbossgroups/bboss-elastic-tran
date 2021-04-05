package org.frameworkset.tran.db.output;

import org.frameworkset.tran.ESDataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

public class AsynDBOutPutDataTran extends DBOutPutDataTran {
	private CountDownLatch countDownLatch;


	public AsynDBOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, Status currentStatus) {
		super( taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
	}
	public AsynDBOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch,Status currentStatus) {
		super( taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		this.countDownLatch = countDownLatch;
	}
//	public void appendData(ESDatas datas){
//		esTranResultSet.appendData(new ESDatasWraper(datas));
//	}


	@Override
	public void stop(){
		esTranResultSet.stop();
		esTranResultSet = null;
		super.stop();
	}
	/**
	 * 只停止转换作业，没有处理完成的数据会继续处理完成
	 */
	@Override
	public void stopTranOnly(){
		esTranResultSet.stopTranOnly();
		esTranResultSet = null;
		super.stopTranOnly();
	}

	@Override
	public String tran() throws ESDataImportException {
		try {
			return super.tran();
		}
		finally {
			if(this.countDownLatch != null)
				countDownLatch.countDown();
		}

	}
}
