package org.frameworkset.tran;/*
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


import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.schedule.TaskContext;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

public interface TranResultSet {
    public Object getValue(  int i, String colName,int sqlType) throws DataImportException;
    public LocalDateTime getLocalDateTimeValue(String colName) throws DataImportException;
    public Object getValue( String colName) throws DataImportException;
	public Object getLastValue(String colName) throws DataImportException;
	public Object getLastOffsetValue() throws DataImportException;

	public Object getValue( String colName,int sqlType) throws DataImportException;
	public BaseDataTran getBaseDataTran();
	public TaskContext getTaskContext();
	public void setBaseDataTran(BaseDataTran baseDataTran);
	public Date getDateTimeValue(String colName) throws DataImportException;
	public Date getDateTimeValue(String colName,String dateFormat) throws DataImportException;
	public TaskContext getRecordTaskContext();
	/**
	 * 如果返回null，说明是强制fush操作，true表示有数据，false表示没有数据
	 * @return
	 * @throws DataImportException
	 */
	public NextAssert next() throws DataImportException;
	public TranMeta getMetaData();

	/**
	 * 获取当前记录对应的原始数据对象，可能是一个map，jdbcresultset，DBObject,hbaseresult
	 * @return
	 */
	Object getRecord();

	/**
	 * 获取当前记录对象
	 * @return
	 */
	Record getCurrentRecord();

	void stop(boolean exception);
//	public void stopTranOnly();
	public Object getKeys();
	Object getMetaValue(String fieldName);


    Map<String, Object> getMetaDatas();



    int getAction();

    public Object getLastValue();

   
    default public void clearQueue(){

    }

    /**
     * 销毁资源
     */
    default public void destroy(){
        
    }
}
