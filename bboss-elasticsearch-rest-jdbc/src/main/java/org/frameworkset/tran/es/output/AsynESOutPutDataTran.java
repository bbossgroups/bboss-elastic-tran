package org.frameworkset.tran.es.output;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.*;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.concurrent.CountDownLatch;

public class AsynESOutPutDataTran extends BaseElasticsearchDataTran {

	private CountDownLatch countDownLatch;

	public AsynESOutPutDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, String esCluster, CountDownLatch countDownLatch) {
		super(  taskContext,jdbcResultSet, importContext,   targetImportContext, esCluster);
		this.countDownLatch = countDownLatch;
	}





	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,String cluster) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext,cluster);
	}
	public AsynESOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch) {
		super(  taskContext,jdbcResultSet,importContext,   targetImportContext);
		this.countDownLatch = countDownLatch;
	}
//	public void appendData(ESDatas datas){
//		esTranResultSet.appendData(new ESDatasWraper(datas));
//	}


	public void stop(){
		esTranResultSet.stop();
		super.stop();
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
