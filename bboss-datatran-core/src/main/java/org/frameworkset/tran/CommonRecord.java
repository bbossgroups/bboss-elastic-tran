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

import org.frameworkset.elasticsearch.client.ResultUtil;
import org.frameworkset.tran.cdc.TableMapping;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.ValueConvert;
import org.frameworkset.tran.schedule.timer.TimeUtil;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;
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

    /**
     * 设置kafka的消息key
     */
	private Object recordKey;

    /**
     * 指定包含kafka消息key的字段
     */
	private String recordKeyField;



    /**
     * 记录字段名称集合
     */
    private Object keys;

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

	/**
	 * 在记录处理过程中，使用的临时数据，不会进行持久化处理
	 */
	private Map<String,Object> tempDatas;
	private TableMapping tableMapping;
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

    public boolean isDDL(){
        return action ==  Record.RECORD_DDL;
    }

    public boolean isUpdate(){
        return action ==  Record.RECORD_UPDATE;
    }

	public boolean isReplace(){
		return action ==  Record.RECORD_REPLACE;
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

	/**
	 * 获取用于指标计算处理等的临时数据
	 * @return
	 */
	public Map<String, Object> getTempDatas() {
		return tempDatas;
	}
	/**
	 * 添加用于指标计算处理等的临时数据到记录，不会对临时数据进行持久化处理，
	 * @param tempDatas
	 */
	public void setTempDatas(Map<String, Object> tempDatas) {
		this.tempDatas = tempDatas;
	}
	/**
	 * 获取用于指标计算处理等的临时数据:name
	 * @return
	 */
	public Object getTempData(String name){
		return tempDatas != null? tempDatas.get(name):null;
	}

	public TableMapping getTableMapping() {
		return tableMapping;
	}

	public void setTableMapping(TableMapping tableMapping) {
		this.tableMapping = tableMapping;
	}

	public String getRecordKeyField() {
		return recordKeyField;
	}

	public void setRecordKeyField(String recordKeyField) {
		this.recordKeyField = recordKeyField;
	}

    public long getLongValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.longValue(value,0l);

    }


    public String getStringValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.stringValue(value,null);

    }


    public String getStringValue(String fieldName, ValueConvert valueConvert) throws Exception{
        Object value = this.getData(fieldName);
        return (String)valueConvert.convert(value);
    }
    public String getStringValue(String fieldName,String defaultValue) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.stringValue(value,defaultValue);

    }

    public boolean getBooleanValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.booleanValue(value,false);

    }

    public boolean getBooleanValue(String fieldName,ValueConvert valueConvert) throws Exception {
        Object value = this.getData(fieldName);
        return (boolean)valueConvert.convert(value);

    }
    public boolean getBooleanValue(String fieldName,boolean defaultValue) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.booleanValue(value,defaultValue);

    }
    public double getDoubleValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.doubleValue(value,0d);
    }

    public float getFloatValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.floatValue(value,0f);
    }

    public int getIntegerValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        return ResultUtil.intValue(value,0);
    }



    public Date getDateValue(String fieldName) throws Exception {
        Object value = this.getData(fieldName);
        if(value == null)
            return null;
        
        else if(value instanceof Date){
            return (Date)value;

        }
        else if(value instanceof Long){
            return new Date(((Long)value).longValue());
        }
        else if(value instanceof String){
            LocalDateTime localDateTime = TimeUtil.localDateTime((String)value);
            return TimeUtil.convertLocalDatetime(localDateTime);
        }
        else if(value instanceof LocalDateTime){
            return TimeUtil.convertLocalDatetime((LocalDateTime)value);

        }
        else if(value instanceof LocalDate){
            return TimeUtil.convertLocalDate((LocalDate)value);

        }
        else if(value instanceof BigDecimal){
            return new Date(((BigDecimal)value).longValue());
        }

        throw new IllegalArgumentException("Convert date value failed:"+value );
    }

    public LocalDateTime getLocalDateTime(String fieldName) throws Exception{
        Object value = this.getData(fieldName);
        if(value == null)
            return null;
        else if(value instanceof String){
            return TimeUtil.localDateTime((String)value);

        }
        else if(value instanceof Date){
            return TimeUtil.date2LocalDateTime((Date)value);

        }
        else if(value instanceof LocalDateTime){
            return (LocalDateTime)value;

        }
        else if(value instanceof LocalDate){
            return TimeUtil.date2LocalDateTime(TimeUtil.convertLocalDate((LocalDate)value));

        }
        else if(value instanceof BigDecimal){
            return TimeUtil.date2LocalDateTime(new Date(((BigDecimal)value).longValue()));
        }
        else if(value instanceof Long){
            return TimeUtil.date2LocalDateTime( new Date(((Long)value).longValue()));
        }

        throw new IllegalArgumentException("Convert date value failed:"+value );
    }
    public Date getDateValue(String fieldName, String dateFormat) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        return getDateValue( fieldName, simpleDateFormat);
    }
    public Date getDateValue(String fieldName, DateFormat dateFormat) throws Exception {
        Object value = this.getData(fieldName);
        if(value == null)
            return null;
        else if(value instanceof Date){
            return (Date)value;

        }
        else if(value instanceof LocalDateTime){
            return TimeUtil.convertLocalDatetime((LocalDateTime)value);

        }
        else if(value instanceof LocalDate){
            return TimeUtil.convertLocalDate((LocalDate)value);

        }
        else if(value instanceof BigDecimal){
            return new Date(((BigDecimal)value).longValue());
        }
        else if(value instanceof Long){
            return new Date(((Long)value).longValue());
        }
        else if(value instanceof String){
//			SerialUtil.getDateFormateMeta().toDateFormat();
            return dateFormat.parse((String) value);
        }
        throw new IllegalArgumentException("Convert date value failed:"+value );
    }

    public Object getKeys() {
        return keys;
    }

    public void setKeys(Object keys) {
        this.keys = keys;
    }

}
