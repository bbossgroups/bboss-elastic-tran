package org.frameworkset.tran.hbase;
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


import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.BaseRecord;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
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
public class HBaseRecord extends BaseRecord{
	private static Logger logger = LoggerFactory.getLogger(HBaseRecord.class);
	private Result data;
	private Map<String,byte[][]> familys;
	public HBaseRecord(TaskContext taskContext, ImportContext importContext, Map<String, byte[][]> familys, Result data){
		super(taskContext,  importContext);
		this.familys = familys;
		this.data = data;
        initMetaDatas();
    }
    private void initMetaDatas() {
        Map<String, Object> tmp = new LinkedHashMap<>();
        tmp.put("rowkey", data.getRow());
        tmp.put("timestamp", new Date(data.rawCells()[0].getTimestamp()));
        this.setMetaDatas(tmp);
    }

 
    private byte[][] parser(String colName){
		byte[][] cs = familys.get(colName);
		if(cs != null){
			return cs;
		}

		cs = parserColumn( colName);
		familys.put(colName,cs);
		return cs;
	}
	public static byte[][] parserColumn(String colName){
		try {
			String[] infos = colName.split(":");
            byte[] family = Bytes.toBytes(infos[0]);

            // 处理没有列限定符的情况（如"info:"）
            byte[] qualifier = infos.length > 1  ?
                    Bytes.toBytes(infos[1]) : Bytes.toBytes("");
			byte[][] cs = new byte[][]{family, qualifier};
			return cs;
		}
		catch (Exception e){
			throw new DataImportException("Parser Column failed: ["+colName+"] is not a hbase colname like c:name",e);
		}
	}
 
	@Override
	public Object getValue(String colName) {        
		byte[][] cs = parser( colName);
		return data.getValue(cs[0],cs[1]);

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
	public Date getDateTimeValue(String colName) throws DataImportException {
        if(colName.equals("_")){
            return (Date)getMetaValue("timestamp");
        }
		Object value = getValue(  colName);
		if(value == null)
			return null;
		Long time = Bytes.toLong((byte[])value);
		return TranUtil.getDateTimeValue(colName,time,taskContext.getImportContext());

	}
	@Override
	public Date getDateTimeValue(String colName,String dateformat) throws DataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		Long time = Bytes.toLong((byte[])value);
		return TranUtil.getDateTimeValue(colName,time,taskContext.getImportContext());

	}

	@Override
	public long getOffset() {
		return 0;
	}

	@Override
	public Object getKeys() {
		return null;
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
