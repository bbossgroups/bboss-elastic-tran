package org.frameworkset.tran.plugin.custom.input;
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


import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.BaseRecord;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 11:24
 * @author biaoping.yin
 * @version 1.0
 */
public class CustomRecord extends BaseRecord{
	private static Logger logger = LoggerFactory.getLogger(CustomRecord.class);
 	private Map data;
	private CustomDataGenerator customDataGenerator;
	public CustomRecord(TaskContext taskContext, ImportContext importContext,Map data,CustomDataGenerator customDataGenerator){
		super(taskContext,  importContext);
		this.data = data;
		this.customDataGenerator = customDataGenerator;
        initMetaDatas();
    }
    private void initMetaDatas() {
        Map<String, Object> tmp = customDataGenerator.getMetaDatas(  data);
		if(tmp != null && !tmp.isEmpty()){
			this.setMetaDatas(tmp);
		}
       
    }

   
 
	@Override
	public Object getValue(String colName) {        		 
		return data.get(colName);

	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(colName);
	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(colName);
	}
 
 

 

	@Override
	public Object getKeys() {
		return data.keySet();
	}
	public Object getData(){
		return data;
	}
	public static void main(String[] args){
		String c = "c:d";
		String[] cs = c.split(":");
		logger.info(cs[0]+":"+cs[1]);
        c = "c:";
        cs = c.split(":");
        logger.info(cs[0]+":"+cs[1]);
	}

}
