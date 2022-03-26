package org.frameworkset.tran.es.output;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.*;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

public class AsynESOutPutDataTran extends BaseElasticsearchDataTran {

	private CountDownLatch countDownLatch;

	public AsynESOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, String esCluster, CountDownLatch countDownLatch, Status currentStatus) {
		super(  taskContext,jdbcResultSet, importContext,   targetImportContext, esCluster,  currentStatus);
		this.countDownLatch = countDownLatch;
	}





	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,String cluster,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,cluster,  currentStatus);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
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
	 * 只停止转换作业
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
