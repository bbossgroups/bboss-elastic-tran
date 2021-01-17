package org.frameworkset.tran.es.output;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.*;

import java.util.concurrent.CountDownLatch;

public class AsynESOutPutDataTran extends BaseElasticsearchDataTran {

	private CountDownLatch countDownLatch;

	public AsynESOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, String esCluster, CountDownLatch countDownLatch) {
		super(jdbcResultSet, importContext,   targetImportContext, esCluster);
		this.countDownLatch = countDownLatch;
	}

	protected void init(){
		super.init();


	}



	public AsynESOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext) {
		super(jdbcResultSet,importContext,   targetImportContext);
	}
	public AsynESOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext,String cluster) {
		super(jdbcResultSet,importContext,   targetImportContext,cluster);
	}
	public AsynESOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, CountDownLatch countDownLatch) {
		super(jdbcResultSet,importContext,   targetImportContext);
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
