package org.frameworkset.tran.plugin.db.input;/*
 *  Copyright 2008 biaoping.yin
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import com.frameworkset.common.poolman.StatementInfo;
import com.frameworkset.common.poolman.sql.PoolManResultSetMetaData;
import com.frameworkset.orm.adapter.DB;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.LastValue;
import org.frameworkset.tran.TranMeta;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.schedule.TaskContext;

import java.sql.ResultSet;

public class JDBCResultSet extends LastValue implements TranResultSet {
	protected ResultSet resultSet;
	protected JDBCTranMetaData metaData;
	protected DB dbadapter;
    protected boolean enableLocalDate;
    protected boolean parallelDatarefactor;
    protected TaskContext taskContext;
    protected StatementInfo statementInfo;

//	protected boolean stoped;
	public JDBCResultSet(){

	}
	public JDBCResultSet(TaskContext taskContext, ImportContext importContext, ResultSet resultSet, 
                         JDBCTranMetaData metaData, 
                         DB dbadapter, boolean enableLocalDate,
                         StatementInfo statementInfo){
		this.resultSet = resultSet;
        this.importContext = importContext;
		this.metaData = metaData;
		this.dbadapter = dbadapter;
        this.enableLocalDate = enableLocalDate;
        parallelDatarefactor = importContext.getInputConfig().isParallelDatarefactor();
        this.taskContext = taskContext;
        this.statementInfo = statementInfo;
        if(!parallelDatarefactor) {
            if (!enableLocalDate) {
                record = new JDBCResultRecord(taskContext, importContext, resultSet, metaData, dbadapter, statementInfo);
            } else {
                record = new LocalDateJDBCResultRecord(taskContext, importContext, resultSet, metaData, dbadapter, statementInfo);
            }
            record.setTranMeta(this.getMetaData());
        }

	}



//	@Override
//	public void setBaseDataTran(BaseDataTran baseDataTran) {
//		super.setBaseDataTran(baseDataTran);
//		resultRecord.setTaskContext(baseDataTran.getTaskContext());
//	}

	public ResultSet getResultSet() {
		return resultSet;
	}
	public DB getDbadapter(){
		return dbadapter;
	}


	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}

	@Override
	public TranMeta getMetaData() {
		return metaData;
	}

	@Override
	public Object getRecord() {
		return getResultSet();
	}


//	@Override
//	public void stop(boolean exception) {
//		stoped = true;
//	}
//	@Override
//	public void stopTranOnly(){stoped = true;}

	@Override
	public Object getMetaValue(String fieldName) {
		return getValue(  fieldName);
	}



	public void setMetaData(PoolManResultSetMetaData metaData) {
		this.metaData = new JDBCTranMetaData(metaData);
	}


	public void setDbadapter(DB dbadapter) {
		this.dbadapter = dbadapter;
	}

	@Override
	public Object getValue(  int i, String colName,int sqlType) throws DataImportException
	{
		return record.getValue(i,colName,sqlType);

	}

 

	@Override
	public Object getKeys(){

		return  record.getKeys();
	}

	@Override
	public Object getValue( String colName,int sqlType) throws DataImportException
	{
		return record.getValue(colName,sqlType);

	}



	@Override

	public NextAssert next() throws DataImportException {
        NextAssert nextAssert = new NextAssert();
		try {
			if(isStop() || importContext.getInputPlugin().isStopCollectData())
				return nextAssert;
            nextAssert.setHasNext(resultSet.next());
            if(nextAssert.isHasNext() && parallelDatarefactor){
                if (!enableLocalDate) {
                    record = new JDBCResultRecord(taskContext, importContext, resultSet, metaData, dbadapter, statementInfo);
                } else {
                    record = new LocalDateJDBCResultRecord(taskContext, importContext, resultSet, metaData, dbadapter, statementInfo);
                }
                record.setTranMeta(this.getMetaData());
                
            }
			return nextAssert;
		}
		catch (Exception e){
			throw ImportExceptionUtil.buildDataImportException(importContext,e);
		}
	}
 
}
