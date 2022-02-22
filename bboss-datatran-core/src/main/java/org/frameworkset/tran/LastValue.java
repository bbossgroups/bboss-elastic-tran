package org.frameworkset.tran;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2020/2/1 23:46
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class LastValue implements TranResultSet{
	protected ImportContext importContext;
	protected BaseDataTran baseDataTran;
	protected Record record;
	public BaseDataTran getBaseDataTran(){
		return baseDataTran;
	}

	public TaskContext getTaskContext(){
		return this.baseDataTran.getTaskContext();
	}
	public void setBaseDataTran(BaseDataTran baseDataTran){
		this.baseDataTran = baseDataTran;
	}
	public void setImportContext(ImportContext importContext) {
		this.importContext = importContext;
	}
	public Object getLastValue(String colName) throws ESDataImportException{
		try {
			if (importContext.getLastValueType() == null || importContext.getLastValueType().intValue() == ImportIncreamentConfig.NUMBER_TYPE)
				return getValue(importContext.getLastValueColumnName());
			else if (importContext.getLastValueType().intValue() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
				if(importContext.getLastValueDateformat() == null || importContext.getLastValueDateformat().equals("")) {
					return getDateTimeValue(importContext.getLastValueColumnName());
				}
				else{
					return getDateTimeValue(importContext.getLastValueColumnName(),importContext.getLastValueDateformat());
				}
			}
		}
		catch (ESDataImportException e){
			throw (e);
		}
		catch (Exception e){
			throw new ESDataImportException(e);
		}
		throw new ESDataImportException("Unsupport last value type:"+importContext.getLastValueType().intValue());
	}
	public Object getLastOffsetValue() throws ESDataImportException{
		Record record = this.getCurrentRecord();
		return record.getOffset();
	}

	@Override
	public Date getDateTimeValue(String colName) throws ESDataImportException {
		return record.getDateTimeValue(colName);

	}

	@Override
	public Date getDateTimeValue(String colName,String format) throws ESDataImportException {
		return record.getDateTimeValue(colName,format);

	}

}
