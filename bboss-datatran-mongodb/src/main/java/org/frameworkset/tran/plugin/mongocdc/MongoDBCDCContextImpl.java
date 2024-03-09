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

import com.frameworkset.orm.annotation.BatchContext;
import org.bson.BsonValue;
import org.frameworkset.elasticsearch.client.ResultUtil;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/11 17:48
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBCDCContextImpl extends ContextImpl {

	public MongoDBCDCContextImpl(TaskContext taskContext, ImportContext importContext, Record record, BatchContext batchContext){
		super(  taskContext,importContext,   record,batchContext);
	}

	@Override
	public long getLongValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);

		if(value == null)
			return 0l;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isInt64()) {
                return bsonValue.asInt64().longValue();
            }
            else if(bsonValue.isInt32()) {
                return bsonValue.asInt32().longValue();
            }
            else if(bsonValue.isNumber()) {
                return bsonValue.asNumber().longValue();
            }
        }
        return ResultUtil.longValue(value,0l);

	}


	@Override
	public String getStringValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isString()) {
                return bsonValue.asString().getValue();
            }
            
        }
        return ResultUtil.stringValue(value,null);
	}
	@Override
	public String getStringValue(String fieldName,String defaultValue) throws Exception {
        Object value = this.getValue(fieldName);
        if(value == null)
            return defaultValue;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isString()) {
                return bsonValue.asString().getValue();
            }

        }
        return ResultUtil.stringValue(value,null);
	}

	@Override
	public boolean getBooleanValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.booleanValue(value,false);
		if(value == null)
			return false;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isBoolean()) {
                return bsonValue.asBoolean().getValue();
            }
            

        }
        return ResultUtil.booleanValue(value,false);
	}
	@Override
	public boolean getBooleanValue(String fieldName,boolean defaultValue) throws Exception {
        Object value = this.getValue(fieldName);
//		return ResultUtil.booleanValue(value,false);
        if(value == null)
            return defaultValue;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isBoolean()) {
                return bsonValue.asBoolean().getValue();
            }


        }
        return ResultUtil.booleanValue(value,defaultValue);
	}
	@Override
	public double getDoubleValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.doubleValue(value,0d);
		if(value == null)
			return 0d;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isDouble()) {
                return bsonValue.asDouble().getValue();
            }
            else if(bsonValue.isNumber()) {
                return bsonValue.asNumber().longValue();
            }

        }
        return ResultUtil.doubleValue(value,0d);
	}

	@Override
	public float getFloatValue(String fieldName) throws Exception {
		double dd = this.getDoubleValue(fieldName);
        return (float) dd;
	}

	@Override
	public int getIntegerValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return 0;
        if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isInt32()) {
                return bsonValue.asInt32().getValue();
            }
            else if(bsonValue.isNumber()) {
                return bsonValue.asNumber().intValue();
            }

        }
        return ResultUtil.integerValue(value,0);
	}

	@Override
	public Date getDateValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);

		if(value == null)
			return null;
        else if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isDateTime()) {
                return new Date(bsonValue.asDateTime().getValue());
            }
            else if(bsonValue.isNumber()) {
                return new Date(bsonValue.asNumber().longValue());
            }

        }
        else if(value instanceof String){
            LocalDateTime localDateTime = TimeUtil.localDateTime((String)value);
            return TimeUtil.convertLocalDatetime(localDateTime);
        }
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
        throw new IllegalArgumentException("Convert date value failed:"+value );
       
	}
    @Override
    public Date getDateValue(String fieldName, String dateFormat) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        return getDateValue( fieldName, simpleDateFormat);
    }
	@Override
	public Date getDateValue(String fieldName,DateFormat dateFormat) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;
        else if(value instanceof BsonValue){
            BsonValue bsonValue = ((BsonValue)value);
            if(bsonValue.isDateTime()) {
                return new Date(bsonValue.asDateTime().getValue());
            }
            else if(bsonValue.isNumber()) {
                return new Date(bsonValue.asNumber().longValue());
            }
            else if(bsonValue.isString()){
                return dateFormat.parse(bsonValue.asString().getValue());
            }

        }
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




 



}
