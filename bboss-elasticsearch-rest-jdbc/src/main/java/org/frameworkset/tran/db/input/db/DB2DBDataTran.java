package org.frameworkset.tran.db.input.db;

import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.DBOutPutDataTran;

import java.util.List;
import java.util.Map;

public class DB2DBDataTran extends DBOutPutDataTran<List<Map<String,Object>>> {

	public DB2DBDataTran(TranResultSet jdbcResultSet, ImportContext importContext) {
		super(jdbcResultSet,importContext);
	}



}
