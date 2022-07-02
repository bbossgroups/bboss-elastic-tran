package org.frameworkset.tran.plugin.es.output;

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

public class AsynESOutPutDataTran extends BaseElasticsearchDataTran {

	private CountDownLatch countDownLatch;

	public AsynESOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext,  String esCluster, CountDownLatch countDownLatch, Status currentStatus) {
		super(  taskContext,jdbcResultSet, importContext, esCluster,  currentStatus);
		this.countDownLatch = countDownLatch;
	}





	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,    currentStatus);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, String cluster,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,   cluster,  currentStatus);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, CountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,     currentStatus);
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
