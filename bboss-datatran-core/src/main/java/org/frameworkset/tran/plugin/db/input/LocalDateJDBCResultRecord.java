package org.frameworkset.tran.plugin.db.input;
/**
 * Copyright 2020 bboss
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
import com.frameworkset.orm.adapter.DB;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.record.BaseRecord;
import org.frameworkset.tran.record.RecordBuidler;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;

import java.sql.ResultSet;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/23 11:30
 * @author biaoping.yin
 * @version 1.0
 */
public class LocalDateJDBCResultRecord extends BaseRecord {
	protected ResultSet resultSet;
	protected JDBCTranMetaData metaData;
	protected DB dbadapter;
    protected Map<String,Object> record;
	public LocalDateJDBCResultRecord(TaskContext taskContext, ImportContext importContext, ResultSet resultSet, JDBCTranMetaData metaData, DB dbadapter, StatementInfo statementInfo) {
		super(taskContext,  importContext);
		
		this.metaData = metaData;
		this.dbadapter = dbadapter;
        RecordBuidler recordBuidler = importContext.getInputConfig().getRecordBuidler();
        if(recordBuidler != null){
            DBRecordBuilderContext dbRecordBuilderContext = new DBRecordBuilderContext();
            dbRecordBuilderContext.setStatementInfo(statementInfo);
            dbRecordBuilderContext.setResultSet(resultSet);
            dbRecordBuilderContext.setImportContext(importContext);
            dbRecordBuilderContext.setTaskContext(taskContext);            
            record = recordBuidler.build(dbRecordBuilderContext);
        }
        else{
            this.resultSet = resultSet;
        }
        
	}
    protected boolean isOracleTimestamp(int sqlType){
		return dbadapter.isOracleTimestamp( sqlType);
	}

	@Override
	public Object getValue(  int i, String colName,int sqlType) throws DataImportException {
		try {
			if(record != null){
                Object value = this.record.get(colName);
                value = TimeUtil.convertLocalDate(value);
                return value;
            }
            else if(!this.isOracleTimestamp(sqlType)) {
				Object value = this.resultSet.getObject(i + 1);
				value = TimeUtil.convertLocalDate(value);
				return value;
			}
			else{
				return this.resultSet.getTimestamp(i + 1);
			}
		}
		catch (Exception ex){
			throw ImportExceptionUtil.buildDataImportException(importContext,new StringBuilder().append("getValue(  ")
					.append(i).append(", ").append(colName).append(",").append(sqlType).append(")").toString(),ex);
		}
	}

	@Override
	public Date getDateTimeValue(String colName) throws DataImportException
	{
		if(colName == null)
			return null;
        if(record != null){
            Date value = (Date)this.record.get(colName);            
            return value;
        }
        else {
            try {
                Date value = this.resultSet.getTimestamp(colName);
                return value;
            } catch (Exception e) {
                try {
                    Date value = this.resultSet.getDate(colName);
                    return value;
                } catch (Exception ex) {
                    throw ImportExceptionUtil.buildDataImportException(importContext, new StringBuilder().append("getValue(").append(colName).append(")").toString(), ex);
                }

            }
        }
	}

	@Override
	public Date getDateTimeValue(String colName,String dateformat) throws DataImportException
	{
		if(colName == null)
			return null;
        if(record != null){
            Date value = (Date)this.record.get(colName);
            return value;
        }
        else {
            try {
                Date value = this.resultSet.getTimestamp(colName);
                return value;
            }
            catch (Exception e){
                try {
                    Date value = this.resultSet.getDate(colName);
                    return value;
                }
                catch (Exception ex){
                    throw ImportExceptionUtil.buildDataImportException(importContext,new StringBuilder().append("getValue(").append(colName).append(")").toString(),ex);
                }

            }
        }
		
	}
	@Override
	public Object getValue( String colName,int sqlType) throws DataImportException
	{
		if(colName == null)
			return null;
		try {
            if(record != null){
                Object value = this.record.get(colName);
                value = TimeUtil.convertLocalDate(value);
                return value;
            }
            else if(!this.isOracleTimestamp(sqlType)) {
				Object value = this.resultSet.getObject(colName);
				value = TimeUtil.convertLocalDate(value);
				return value;
			}
			else{
				return this.resultSet.getTimestamp(colName);
			}
		}
		catch (Exception ex){
			throw ImportExceptionUtil.buildDataImportException(importContext,new StringBuilder().append("getValue(  ")
					.append(colName).append(",").append(sqlType).append(")").toString(),ex);
		}


	}
	@Override
	public Object getValue(String colName) {
		if(colName == null)
			return null;
		try {
            if(record != null){
                Object value = this.record.get(colName);
                value = TimeUtil.convertLocalDate(value);
                return value;
            }
            else {
                Object value = this.resultSet.getObject(colName);
                value = TimeUtil.convertLocalDate(value);
                return value;
            }
		}
		catch (Exception ex){
			throw ImportExceptionUtil.buildDataImportException(importContext,new StringBuilder().append("getValue(").append(colName).append(")").toString(),ex);
		}
	}

	@Override
	public Object getKeys() {
        if(metaData == null || metaData.getPoolManResultSetMetaData() == null)
            return null;
		return  metaData.getPoolManResultSetMetaData().get_columnLabel();
	}

	@Override
	public Object getData() {
        if(record != null)
            return record;
		else 
            return resultSet;
	}

	@Override
	public Object getMetaValue(String metaName) {
		return this.getValue(metaName);
	}

	@Override
	public long getOffset() {
		return 0;
	}


}
