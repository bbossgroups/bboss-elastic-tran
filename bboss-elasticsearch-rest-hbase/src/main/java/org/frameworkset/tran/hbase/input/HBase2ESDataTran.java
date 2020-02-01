package org.frameworkset.tran.hbase.input;

import org.frameworkset.tran.BaseElasticsearchDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;

public class HBase2ESDataTran extends BaseElasticsearchDataTran {

	public HBase2ESDataTran(TranResultSet jdbcResultSet, ImportContext importContext) {
		super(jdbcResultSet,importContext);
	}
	public HBase2ESDataTran(TranResultSet jdbcResultSet, ImportContext importContext, String cluster) {
		super(jdbcResultSet,importContext, cluster);
	}










}
