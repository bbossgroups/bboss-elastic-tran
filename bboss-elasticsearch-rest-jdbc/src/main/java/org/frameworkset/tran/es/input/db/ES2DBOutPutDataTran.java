package org.frameworkset.tran.es.input.db;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.elasticsearch.entity.ESDatas;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.es.ESDatasWraper;

import java.util.concurrent.CountDownLatch;

public class ES2DBOutPutDataTran extends AsynDBOutPutDataTran<ESDatas> {


	public ES2DBOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext) {
		super(jdbcResultSet, importContext);
	}

	public ES2DBOutPutDataTran(TranResultSet jdbcResultSet, ImportContext importContext, CountDownLatch countDownLatch) {
		super(jdbcResultSet, importContext, countDownLatch);
	}



	public void appendInData(ESDatas datas){
		super.appendData(new ESDatasWraper(datas));
	}
}
