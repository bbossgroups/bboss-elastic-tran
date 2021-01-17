package org.frameworkset.tran.db.input.db;
/**
 * Copyright 2008 biaoping.yin
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.frameworkset.common.poolman.StatementInfo;
import com.frameworkset.common.poolman.handle.ResultSetHandler;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.JDBCResultSet;
import org.frameworkset.tran.db.output.DBOutPutDataTran;

import java.sql.ResultSet;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/11/8 23:18
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DBResultSetHandler extends ResultSetHandler {
	private ImportContext importContext ;
	private ImportContext targetImportContext;
	public DB2DBResultSetHandler(ImportContext importContext,ImportContext targetImportContext){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;

	}
	@Override
	public void handleResult(ResultSet resultSet, StatementInfo statementInfo) throws Exception {
		JDBCResultSet jdbcResultSet = new JDBCResultSet();
		jdbcResultSet.setResultSet(resultSet);
		jdbcResultSet.setImportContext(importContext);
		jdbcResultSet.setMetaData(statementInfo.getMeta());
		jdbcResultSet.setDbadapter(statementInfo.getDbadapter());
		DBOutPutDataTran db2DBDataTran = new DBOutPutDataTran(jdbcResultSet,importContext,targetImportContext);
		db2DBDataTran.tran(  );
	}
}
