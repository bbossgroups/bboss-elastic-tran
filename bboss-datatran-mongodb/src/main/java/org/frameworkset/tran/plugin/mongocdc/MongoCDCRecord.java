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
public class MongoCDCRecord extends CommonMapRecord {
    private MongoCDCInputConfig mongoCDCInputConfig;
    private MongoDBCDCData mongoDBCDCData;
	public MongoCDCRecord(TaskContext taskContext, ImportContext importContext,
                          MongoCDCInputConfig mongoCDCInputConfig,
                          boolean removed, boolean reachEOFClosed, boolean readEOFRecord){
		super(taskContext,  importContext,(Map<String,Object>)null,  removed,   reachEOFClosed, readEOFRecord);
        this.mongoCDCInputConfig = mongoCDCInputConfig;
	}

    public MongoCDCRecord(TaskContext taskContext,ImportContext importContext,
                          MongoDBCDCData mongoDBCDCData, MongoCDCInputConfig mongoCDCInputConfig
    ){
        super(taskContext,  importContext,(Map<String,Object>)(mongoDBCDCData.getData()));
        this.mongoCDCInputConfig = mongoCDCInputConfig;
        this.mongoDBCDCData = mongoDBCDCData;
        initMetaDatas();
    }
    public void initMetaDatas(){
        Map<String,Object> tmp = new LinkedHashMap<>();
        tmp.put("position", mongoDBCDCData.getPosition());
        tmp.put("table", mongoDBCDCData.getCollection());
        tmp.put("database", mongoDBCDCData.getDatabase());
        tmp.put("action", mongoDBCDCData.getAction());
        tmp.put("clusterTime", mongoDBCDCData.getClusterTime());
        tmp.put("wallTime", mongoDBCDCData.getWallTime());
        if(mongoDBCDCData.getRemovedFields() != null)
            tmp.put("removedFields", mongoDBCDCData.getRemovedFields());
        if(mongoDBCDCData.getUpdateDescription() != null)
            tmp.put("updateDescription", mongoDBCDCData.getUpdateDescription());
        this.setMetaDatas(tmp);
    }

	@Override
	public Object getMetaValue(String colName) {
		if(colName.equals("position"))
			return mongoDBCDCData.getPosition();
		else if(colName.equals("table"))
			return mongoDBCDCData.getCollection();
        else if(colName.equals("database"))
			return mongoDBCDCData.getDatabase();

        else if(colName.equals("action"))
            return mongoDBCDCData.getAction();
        else if(colName.equals("clusterTime"))
            return mongoDBCDCData.getClusterTime();
        else if(colName.equals("wallTime"))
            return mongoDBCDCData.getWallTime();
		throw new DataImportException("Get Meta Value failed: " + colName + " is not a MongoDB CDC meta field.MongoDB CDC meta fields only is one of {position,table,database,action,clusterTime,wallTime}");
	}

    @Override
    public long getOffset() {
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

    @Override
    public Long getLastValueTime(){
        MongoDBCDCData mongoDBCDCData = getMongoDBCDCData();
        if(mongoDBCDCData != null) {
            return mongoDBCDCData.getClusterTime();
        }
        return null;
    }
    @Override
    public String getStrLastValue() throws DataImportException {
        MongoDBCDCData mongoDBCDCData = getMongoDBCDCData();
        if(mongoDBCDCData != null) {
            return mongoDBCDCData.getPosition();
        }
        return null;
    }
}
