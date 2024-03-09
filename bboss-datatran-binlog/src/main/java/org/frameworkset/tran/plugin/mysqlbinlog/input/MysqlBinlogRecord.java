package org.frameworkset.tran.plugin.mysqlbinlog.input;
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
import org.frameworkset.tran.record.CommonMapRecord;
import org.frameworkset.tran.schedule.TaskContext;

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
public class MysqlBinlogRecord extends CommonMapRecord {
    private MySQLBinlogConfig mySQLBinlogConfig ;
    private MysqlBinLogData mysqlBinLogData;
	public MysqlBinlogRecord(TaskContext taskContext, ImportContext importContext,
                             MySQLBinlogConfig mySQLBinlogConfig,
                             boolean removed, boolean readEOFRecord){
		super(taskContext,  importContext,(Map<String,Object>)null,  removed,   readEOFRecord);
        this.mySQLBinlogConfig = mySQLBinlogConfig;
	}

    public MysqlBinlogRecord(TaskContext taskContext,ImportContext importContext,
                             MysqlBinLogData mysqlBinLogData, MySQLBinlogConfig mySQLBinlogConfig
    ){
        super(taskContext,  importContext,(Map<String,Object>)(mysqlBinLogData.getData()));
        this.mySQLBinlogConfig = mySQLBinlogConfig;
        this.mysqlBinLogData = mysqlBinLogData;
        initMetaDatas();
    }
    public void initMetaDatas(){
        Map<String,Object> tmp = new LinkedHashMap<>();
        tmp.put("position",mysqlBinLogData.getPosition());
        tmp.put("table",mysqlBinLogData.getTable());
        tmp.put("database",mysqlBinLogData.getDatabase());
        tmp.put("host",mySQLBinlogConfig.getHost());
        tmp.put("fileName",mysqlBinLogData.getFileName());
        if(mySQLBinlogConfig.getFileNames() != null)
            tmp.put("fileNames",mySQLBinlogConfig.getFileNames());
        tmp.put("port",mySQLBinlogConfig.getPort());
        tmp.put("action",mysqlBinLogData.getAction());
        this.setMetaDatas(tmp);
    }

//	@Override
//	public Object getMetaValue(String colName) {
//		if(colName.equals("position"))
//			return mysqlBinLogData.getPosition();
//		else if(colName.equals("table"))
//			return mysqlBinLogData.getTable();
//        else if(colName.equals("database"))
//			return mySQLBinlogConfig.getDatabase();
//        else if(colName.equals("host"))
//			return mySQLBinlogConfig.getHost();
//        else if(colName.equals("fileName"))
//			return mysqlBinLogData.getFileName();
//        else if(colName.equals("fileNames"))
//            return mySQLBinlogConfig.getFileNames();
//        else if(colName.equals("port"))
//			return mySQLBinlogConfig.getPort();
//        else if(colName.equals("action"))
//            return mysqlBinLogData.getAction();
//
//		throw new DataImportException("Get Meta Value failed: " + colName + " is not a mysql binlog meta field.mysql binlog meta fields must be {position,table,database,host,fileName,fileNames,port,action}");
//	}

    @Override
    public long getOffset() {
        if(mysqlBinLogData == null){
            return -1;
        }
        Long p = mysqlBinLogData.getPosition();
        return p != null?p:-1;
    }

    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    @Override
    public Map<String, Object> getUpdateFromDatas(){
        return (Map<String, Object>) mysqlBinLogData.getOldValues();
    }

    @Override
    public int getAction(){
        if(mysqlBinLogData != null)
            return mysqlBinLogData.getAction();
        else{
            return RECORD_INSERT;
        }
    }

    public MysqlBinLogData getMysqlBinLogData() {
        return mysqlBinLogData;
    }

    @Override
    public String getStrLastValue() throws DataImportException {
        
        MysqlBinLogData mysqlBinLogData = getMysqlBinLogData();
        if(mysqlBinLogData != null) {
            if(mysqlBinLogData.getGtid() != null){
                return mysqlBinLogData.getGtid() + MySQLBinlogConfig.split + mysqlBinLogData.getFileName();
            }
            else {
                return mysqlBinLogData.getFileName();
            }
        }
        
        return null;
    }
}
