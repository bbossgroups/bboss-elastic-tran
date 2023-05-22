package org.frameworkset.tran;
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

import org.frameworkset.tran.record.RecordColumnInfo;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 21:03
 * @author biaoping.yin
 * @version 1.0
 */
public class CommonRecord {

    /**
     * @see Record
     *     public final int RECORD_INSERT = 0;
     *     public final int RECORD_UPDATE = 1;
     *     public final int RECORD_DELETE = 2;
     */
    private int action = Record.RECORD_INSERT;



    private Map<String,Object> metaDatas;

	private Object recordKey;
    /**
     * binlog采集的修改前记录信息
     */
    private Map<String,Object> updateFromDatas;

	/**
	 * 来源数据
	 */
	private Object oringeData;

	public Object getOringeData() {
		return oringeData;
	}

	public void setOringeData(Object oringeData) {
		this.oringeData = oringeData;
	}

	public Map<String, Object> getDatas() {
		return datas;
	}

	public void setDatas(Map<String, Object> datas) {
		this.datas = datas;
	}

	private Map<String,Object> datas;
	private Map<String,RecordColumnInfo> dataInfos;

	public void addData(String name, Object value, RecordColumnInfo recordColumnInfo){
		if(datas == null) {
			datas = new LinkedHashMap<String, Object>();
		}
		if(recordColumnInfo != null){
			if(dataInfos == null ){
				dataInfos = new LinkedHashMap<>();
			}
			dataInfos.put(name,recordColumnInfo);
		}

		datas.put(name,value);
	}
	public RecordColumnInfo getRecordColumnInfo(String name){
		if(dataInfos != null){
			return dataInfos.get(name);
		}
		return null;
	}
	public void addData(String name,Object value){
		if(datas == null) {
			datas = new LinkedHashMap<String, Object>();
		}
		datas.put(name,value);
	}

	public Object getData(String name){
		if(datas != null)
			return datas.get(name);
		return null;
	}
	public boolean containKey(String name){
		if(datas != null)
			return datas.containsKey(name);
		return false;
	}

	public Object getRecordKey() {
		return recordKey;
	}

	public void setRecordKey(Object recordKey) {
		this.recordKey = recordKey;
	}

    public int getAction() {
        return action;
    }

    public void setAction(int action) {
        this.action = action;
    }

    public boolean isInsert(){
        return action ==  Record.RECORD_INSERT;
    }

    public boolean isDelete(){
        return action ==  Record.RECORD_DELETE;
    }

    public boolean isUpdate(){
        return action ==  Record.RECORD_UPDATE;
    }
    public Map<String, Object> getMetaDatas() {
        return metaDatas;
    }

    public void setMetaDatas(Map<String, Object> metaDatas) {
        this.metaDatas = metaDatas;
    }

    public Object getMetaValue(String colName) {
        if(metaDatas != null){
            return metaDatas.get(colName);
        }
        return null;
    }

    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    public Map<String, Object> getUpdateFromDatas() {
        return updateFromDatas;
    }

    public void setUpdateFromDatas(Map<String, Object> updateFromDatas) {
        this.updateFromDatas = updateFromDatas;
    }
}
