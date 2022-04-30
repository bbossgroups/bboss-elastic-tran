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

import com.frameworkset.orm.annotation.BatchContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

import java.text.DateFormat;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/11 17:48
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseRecordContextImpl extends ContextImpl {

	public HBaseRecordContextImpl(TaskContext taskContext, ImportContext importContext, ImportContext targetImportContext, TranResultSet tranResultSet, BatchContext batchContext){
		super(  taskContext,importContext,targetImportContext,tranResultSet,batchContext);
	}

	@Override
	public long getLongValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);

		if(value == null)
			return 0l;
		return Bytes.toLong((byte[])value);
//		return ResultUtil.longValue(value,0l);

	}


	@Override
	public String getStringValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;
//		return ResultUtil.stringValue(value,null);
		return Bytes.toString((byte[])value);
	}
	@Override
	public String getStringValue(String fieldName,String defaultValue) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.stringValue(value,defaultValue);
		if(value == null)
			return defaultValue;
		return Bytes.toString((byte[])value);
	}

	@Override
	public boolean getBooleanValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.booleanValue(value,false);
		if(value == null)
			return false;
		return Bytes.toBoolean((byte[])value);
	}
	@Override
	public boolean getBooleanValue(String fieldName,boolean defaultValue) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.booleanValue(value,defaultValue);
		if(value == null)
			return defaultValue;
		return Bytes.toBoolean((byte[])value);
	}
	@Override
	public double getDoubleValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.doubleValue(value,0d);
		if(value == null)
			return 0d;
		return Bytes.toDouble((byte[])value);
	}

	@Override
	public float getFloatValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.floatValue(value,0f);
		if(value == null)
			return 0f;
		return Bytes.toFloat((byte[])value);
	}

	@Override
	public int getIntegerValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
//		return ResultUtil.intValue(value,0);
		if(value == null)
			return 0;
		return Bytes.toInt((byte[])value);
	}

	@Override
	public Date getDateValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);

		if(value == null)
			return null;
		if(value instanceof Date){
			return (Date)value;

		}
		long time = Bytes.toLong((byte[])value);

		return new Date(time);
	}
	@Override
	public Date getDateValue(String fieldName,DateFormat dateFormat) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;

		if(value instanceof Date){
			return (Date)value;

		}
		long time = Bytes.toLong((byte[])value);

		return new Date(time);
	}





	@Override
	public IpInfo getIpInfo(String fieldName) throws Exception{
		Object _ip = tranResultSet.getValue(fieldName);
		if(_ip == null){
			return null;
		}
		String ip = Bytes.toString((byte[])_ip);
		if(baseImportConfig.getGeoIPUtil(getGeoipConfig()) != null) {
			return baseImportConfig.getGeoIPUtil(getGeoipConfig()).getIpInfo(ip);
		}
		return null;
	}



}
