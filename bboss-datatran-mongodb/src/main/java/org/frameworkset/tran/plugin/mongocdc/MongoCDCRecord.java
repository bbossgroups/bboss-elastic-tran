package org.frameworkset.tran.plugin.mongocdc;
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
public class MongoCDCRecord extends CommonMapRecord {
    private MongoCDCInputConfig mongoCDCInputConfig;
    private MongoDBCDCData mongoDBCDCData;
	public MongoCDCRecord(TaskContext taskContext,
                          MongoCDCInputConfig mongoCDCInputConfig,
                          boolean removed, boolean reachEOFClosed, boolean readEOFRecord){
		super(taskContext,(Map<String,Object>)null,  removed,   reachEOFClosed, readEOFRecord);
        this.mongoCDCInputConfig = mongoCDCInputConfig;
	}

    public MongoCDCRecord(TaskContext taskContext,
                          MongoDBCDCData mongoDBCDCData, MongoCDCInputConfig mongoCDCInputConfig
    ){
        super(taskContext,(Map<String,Object>)(mongoDBCDCData.getData()));
        this.mongoCDCInputConfig = mongoCDCInputConfig;
        this.mongoDBCDCData = mongoDBCDCData;
        initMetaDatas();
    }
    public void initMetaDatas(){
//        Map<String,Object> tmp = new LinkedHashMap<>();
//        tmp.put("position", mongoDBCDCData.getPosition());
//        tmp.put("table", mongoDBCDCData.getTable());
//        tmp.put("database", mongoDBCDCData.getDatabase());
//        tmp.put("host", mongoCDCInputConfig.getHost());
//        tmp.put("fileName", mongoDBCDCData.getFileName());
//        if(mongoCDCInputConfig.getFileNames() != null)
//            tmp.put("fileNames", mongoCDCInputConfig.getFileNames());
//        tmp.put("port", mongoCDCInputConfig.getPort());
//        tmp.put("action", mongoDBCDCData.getAction());
//        this.setMetaDatas(tmp);
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
        if(mongoDBCDCData == null){
            return -1;
        }
        /**
        Long p = mongoDBCDCData.getPosition();
        return p != null?p:-1;
         */
        return -1;
    }

    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    @Override
    public Map<String, Object> getUpdateFromDatas(){
        return (Map<String, Object>) mongoDBCDCData.getOldValues();
    }

    @Override
    public int getAction(){
        if(mongoDBCDCData != null)
            return mongoDBCDCData.getAction();
        else{
            return RECORD_INSERT;
        }
    }

    public MongoDBCDCData getMongoDBCDCData() {
        return mongoDBCDCData;
    }
}
