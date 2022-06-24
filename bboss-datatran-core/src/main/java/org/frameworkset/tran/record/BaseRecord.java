package org.frameworkset.tran.record;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/4 16:27
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseRecord implements Record {
	protected TaskContext taskContext;
	public BaseRecord(TaskContext taskContext){
		this.taskContext = taskContext;
	}

	@Override
	public TaskContext getTaskContext() {
		return taskContext;
	}

	public void setTaskContext(TaskContext taskContext) {
		this.taskContext = taskContext;
	}
	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext());

	}

	@Override
	public Date getDateTimeValue(String colName,String dateformat) throws DataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		return TranUtil.getDateTimeValue(colName,value,taskContext.getImportContext(),dateformat);

	}
}
