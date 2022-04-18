package org.frameworkset.tran.status;
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

import org.frameworkset.spi.BaseApplicationContext;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/26 9:16
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseStatusManager implements StatusManager {
	private static Logger logger = LoggerFactory.getLogger(BaseStatusManager.class);
	protected String statusDbname,updateSQL;
	protected int lastValueType;
	private StatusFlushThread flushThread ;
	private DataTranPlugin dataTranPlugin;
	private boolean stoped;
	public BaseStatusManager(String statusDbname,String updateSQL,
							  int lastValueType,
							 DataTranPlugin dataTranPlugin){
		this.statusDbname = statusDbname;
		this.updateSQL = updateSQL;
		this.lastValueType = lastValueType;
		this.dataTranPlugin = dataTranPlugin;
	}

	public DataTranPlugin getDataTranPlugin() {
		return dataTranPlugin;
	}

	public void init(){
		flushThread = new StatusFlushThread(this,
				dataTranPlugin.getImportContext().getAsynFlushStatusInterval());
		flushThread.start();
		BaseApplicationContext.addShutdownHook(new Runnable() {
			@Override
			public void run() {
				if(isStoped())
					return;
				synchronized(BaseStatusManager.this) {
					if(isStoped())
						return;
					flushStatus();
				}
			}
		});
	}

	private ReadWriteLock putStatusLock = new ReentrantReadWriteLock();
	private Lock read = putStatusLock.readLock();
	private Lock write = putStatusLock.writeLock();
	protected abstract void _putStatus(Status currentStatus);

	public void putStatus(Status currentStatus) throws Exception{
		try{
			read.lock();
			_putStatus( currentStatus);
//			if(flushThread.reach())
//				flushThread.notify();
		}
		finally {
			read.unlock();
		}
	}
	protected abstract void _flushStatus() throws Exception;
	public void flushStatus(){
		try {
			write.lock();
			_flushStatus();
		} catch (Exception throwables) {
			logger.error("flushStatus failed:statusDbname["+statusDbname+"],updateSQL["+updateSQL+"]",throwables);
		}
		finally {
			write.unlock();
		}
	}

	@Override
	public synchronized void stop(){
		stoped = true;
		flushThread.interrupt();
	}

	@Override
	public synchronized boolean isStoped() {
		return stoped;
	}

	protected Object convertLastValue(Object lastValue){
		if(lastValue == null){
			return null;
		}
		if(lastValue instanceof Date){
			lastValue = new Long(((Date) lastValue).getTime());
		}
		return lastValue;
	}

	public static boolean needUpdate(Integer lastValueType, Object oldValue,Object newValue){
		if(newValue == null)
			return false;

		if(oldValue == null)
			return true;
//		this.getLastValueType()
		if(lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			Date oldValueDate = (Date)oldValue;
			Date newValueDate = (Date)newValue;
			if(newValueDate.after(oldValueDate))
				return true;
			else
				return false;
		}
		else{
//			Method compareTo = oldValue.getClass().getMethod("compareTo");
			if(oldValue instanceof Integer && newValue instanceof Integer){
				int e = ((Integer)oldValue).compareTo ((Integer)newValue);
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Long || newValue instanceof Long){
				boolean e = ((Number)oldValue).longValue() <= ((Number)newValue).longValue();
				if(e)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof BigDecimal){
				int e = ((BigDecimal)oldValue).compareTo ((BigDecimal)newValue);
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof Integer){
				boolean e = ((BigDecimal)oldValue).longValue() > ((Integer)newValue).intValue();
				if(!e )
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Integer && newValue instanceof BigDecimal){
				boolean e = ((BigDecimal)newValue).longValue() > ((Integer)oldValue).intValue();
				if(!e )
					return false;
				else
					return true;
			}
			else if(oldValue instanceof Double || newValue instanceof Double){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return true;
				else
					return false;
			}
			else if(oldValue instanceof Float || newValue instanceof Float){
				int e = Float.compare(((Number)oldValue).floatValue(), ((Number)newValue).floatValue());
				if(e < 0)
					return true;
				else
					return false;
			}

			else if(oldValue instanceof BigDecimal || newValue instanceof BigDecimal){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return true;
				else
					return false;
			}
			else {
				boolean e = ((Number)oldValue).intValue() <= ((Number)newValue).intValue();
				if(e)
					return true;
				else
					return false;
			}

		}
	}
	public static Object max(Integer lastValueType, Object oldValue,Object newValue){
		if(newValue == null)
			return oldValue;

		if(oldValue == null)
			return newValue;
//		this.getLastValueType()
		if(lastValueType == ImportIncreamentConfig.TIMESTAMP_TYPE) {
			Date oldValueDate = (Date)oldValue;
			Date newValueDate = (Date)newValue;
			if(newValueDate.after(oldValueDate))
				return newValue;
			else
				return oldValue;
		}
		else{
//			Method compareTo = oldValue.getClass().getMethod("compareTo");
			if(oldValue instanceof Integer && newValue instanceof Integer){
				int e = ((Integer)oldValue).compareTo ((Integer)newValue);
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Long || newValue instanceof Long){
				boolean e = ((Number)oldValue).longValue() <= ((Number)newValue).longValue();
				if(e)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof BigDecimal){
				int e = ((BigDecimal)oldValue).compareTo ((BigDecimal)newValue);
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof BigDecimal && newValue instanceof Integer){
				boolean e = ((BigDecimal)oldValue).longValue() > ((Integer)newValue).intValue();
				if(!e )
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Integer && newValue instanceof BigDecimal){
				boolean e = ((BigDecimal)newValue).longValue() > ((Integer)oldValue).intValue();
				if(!e )
					return oldValue;
				else
					return newValue;
			}
			else if(oldValue instanceof Double || newValue instanceof Double){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else if(oldValue instanceof Float || newValue instanceof Float){
				int e = Float.compare(((Number)oldValue).floatValue(), ((Number)newValue).floatValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}

			else if(oldValue instanceof BigDecimal || newValue instanceof BigDecimal){
				int e = Double.compare(((Number)oldValue).doubleValue(), ((Number)newValue).doubleValue());
				if(e < 0)
					return newValue;
				else
					return oldValue;
			}
			else {
				boolean e = ((Number)oldValue).intValue() <= ((Number)newValue).intValue();
				if(e)
					return newValue;
				else
					return oldValue;
			}

		}
	}
}
