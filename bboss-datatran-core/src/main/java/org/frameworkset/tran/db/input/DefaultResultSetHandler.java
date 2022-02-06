package org.frameworkset.tran.db.input;
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
import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.JDBCResultSet;
import org.frameworkset.tran.db.JDBCTranMetaData;
import org.frameworkset.tran.schedule.TaskContext;

import java.sql.ResultSet;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/11/8 23:18
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultResultSetHandler extends ResultSetHandler {
	private ImportContext importContext ;
	private ImportContext targetImportContext;
	private SQLBaseDataTranPlugin sqlBaseDataTranPlugin ;
	private TaskContext taskContext;
	public DefaultResultSetHandler( TaskContext taskContext,ImportContext importContext,ImportContext targetImportContext,SQLBaseDataTranPlugin sqlBaseDataTranPlugin){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
		this.sqlBaseDataTranPlugin = sqlBaseDataTranPlugin;
		this.taskContext = taskContext;

	}
	@Override
	public void handleResult(ResultSet resultSet, StatementInfo statementInfo) throws Exception {
		JDBCResultSet jdbcResultSet = new JDBCResultSet(taskContext,resultSet,new JDBCTranMetaData(statementInfo.getMeta()),statementInfo.getDbadapter());
		jdbcResultSet.setImportContext(importContext);
//		jdbcResultSet.setResultSet(resultSet);
//		jdbcResultSet.setMetaData(statementInfo.getMeta());
//		jdbcResultSet.setDbadapter(statementInfo.getDbadapter());
		BaseDataTran baseDataTran = sqlBaseDataTranPlugin.createBaseDataTran( taskContext,jdbcResultSet,sqlBaseDataTranPlugin.getCurrentStatus());
		baseDataTran.tran(  );
	}
}
