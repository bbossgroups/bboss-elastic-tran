package org.frameworkset.tran.es.input.es;

import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.ESDatasWraper;
import org.frameworkset.tran.es.output.AsynESOutPutDataTran;

import java.util.concurrent.CountDownLatch;

public class ES2ESOutPutDataTran extends AsynESOutPutDataTran<ESDatas> {




	public ES2ESOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext,String esCluster,  CountDownLatch countDownLatch) {
		super(jdbcResultSet, importContext,  esCluster,  countDownLatch);
	}



	public void appendInData(ESDatas datas){
		super.appendData(new ESDatasWraper(datas));
	}
}
