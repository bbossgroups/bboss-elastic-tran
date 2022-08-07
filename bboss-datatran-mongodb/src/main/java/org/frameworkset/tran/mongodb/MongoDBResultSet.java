package org.frameworkset.tran.mongodb;
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

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBResultSet extends LastValue implements TranResultSet {
	private DBCursor dbCursor;
	private DBObject dbObject;
	private boolean stoped;
	public MongoDBResultSet(ImportContext importContext, DBCursor dbCursor) {
		this.importContext = importContext;
		this.dbCursor = dbCursor;
	}
	@Override
	public TaskContext getRecordTaskContext() {
		return record.getTaskContext();
	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName) throws DataImportException {
		return record.getValue(colName);

	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		return record.getDateTimeValue(colName);

	}

	@Override
	public Boolean next() throws DataImportException {
		if(stoped || importContext.getInputPlugin().isStopCollectData())
			return false;
		boolean hasNext = dbCursor.hasNext();
		if( hasNext){
			dbObject = dbCursor.next();
			record = new MongoDBRecord(dbObject,getTaskContext());
		}
		return hasNext;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(dbObject.keySet());
	}

	public Object getKeys(){
		return record.getKeys();
	}
	@Override
	public Object getRecord() {
		return dbObject;
	}

	@Override
	public Record getCurrentRecord() {
		return record;
	}

	@Override
	public void stop() {
		stoped = true;
	}
	@Override
	public void stopTranOnly(){
		stoped = true;
	}

	@Override
	public Object getMetaValue(String fieldName) {
		return getValue(fieldName);
	}
	@Override
	public boolean removed() {
		return false;
	}
	public boolean reachEOFClosed(){
		return false ;
	}
}
