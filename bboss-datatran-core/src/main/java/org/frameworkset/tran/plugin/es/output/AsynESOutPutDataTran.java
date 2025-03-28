package org.frameworkset.tran.plugin.es.output;

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.JobCountDownLatch;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

public class AsynESOutPutDataTran extends BaseElasticsearchDataTran {

//	private JobCountDownLatch countDownLatch;

 
    public AsynESOutPutDataTran(BaseDataTran baseDataTran) {
        super(baseDataTran);
    }




	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,    currentStatus);
	}
 
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, JobCountDownLatch countDownLatch,Status currentStatus) {
		super(  taskContext,jdbcResultSet,importContext,     currentStatus);
		this.countDownLatch = countDownLatch;
	}
//	public void appendData(ESDatas datas){
//		esTranResultSet.appendData(new ESDatasWraper(datas));
//	}

//	@Override
//	public void stop(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stop();
//			asynTranResultSet = null;
//		}
//		super.stop();
//	}
//	/**
//	 * 只停止转换作业
//	 */
//	@Override
//	public void stopTranOnly(){
//		if(asynTranResultSet != null) {
//			asynTranResultSet.stopTranOnly();
//			asynTranResultSet = null;
//		}
//
//		super.stopTranOnly();
//	}


}
