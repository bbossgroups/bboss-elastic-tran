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

import com.frameworkset.common.poolman.sql.PoolManResultSetMetaData;
import com.frameworkset.orm.adapter.DB;
import org.frameworkset.tran.*;
import org.frameworkset.tran.schedule.TaskContext;

import java.sql.ResultSet;

public class JDBCResultSet extends LastValue implements TranResultSet {
	protected ResultSet resultSet;
	protected JDBCTranMetaData metaData;
	protected DB dbadapter;

	protected boolean stoped;
	public JDBCResultSet(){

	}
	public JDBCResultSet(TaskContext taskContext,ResultSet resultSet,JDBCTranMetaData metaData,DB dbadapter){
		this.resultSet = resultSet;
		this.metaData = metaData;
		this.dbadapter = dbadapter;
		record = new JDBCResultRecord(taskContext,resultSet,metaData,dbadapter);

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
	public boolean reachEOFClosed(){
		return false ;
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

	@Override
	public Record getCurrentRecord() {
		return record;
	}

	@Override
	public void stop() {
		stoped = true;
	}
//	@Override
//	public void stopTranOnly(){stoped = true;}

	@Override
	public Object getMetaValue(String fieldName) {
		return getValue(  fieldName);
	}

	@Override
	public boolean removed() {
		return false;
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
	public Object getValue( String colName) throws DataImportException
	{
		return record.getValue(colName);

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

	public Boolean next() throws DataImportException {
		try {
			if(stoped || importContext.getInputPlugin().isStopCollectData())
				return false;
			return resultSet.next();
		}
		catch (Exception e){
			throw new DataImportException(e);
		}
	}
	@Override
	public TaskContext getRecordTaskContext() {
		return record.getTaskContext();
	}
}
