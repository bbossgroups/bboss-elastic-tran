package org.frameworkset.tran.db.input.es;

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.BaseElasticsearchDataTran;
import org.frameworkset.tran.db.JDBCResultSet;

public class DB2ESDataTran extends BaseElasticsearchDataTran {

	public DB2ESDataTran(JDBCResultSet jdbcResultSet, ImportContext importContext) {
		super(jdbcResultSet,importContext);
	}
	public DB2ESDataTran(JDBCResultSet jdbcResultSet, ImportContext importContext, String esCluster) {
		super(jdbcResultSet,   importContext,  esCluster);
	}


}
