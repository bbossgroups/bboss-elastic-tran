package org.frameworkset.tran;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.util.TranUtil;

import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.frameworkset.tran.util.TranConstant.STATUS_STOP;
import static org.frameworkset.tran.util.TranConstant.STATUS_STOPTRANONLY;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/28 22:37
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class AsynBaseTranResultSet extends  LastValue implements AsynTranResultSet {
	private Record record;
	private List records;
	private int pos = 0;
	private int size;

	private volatile int status;
	private BlockingQueue<Data> queue ;

	public AsynBaseTranResultSet(ImportContext importContext) {
		queue = new ArrayBlockingQueue(importContext.getTranDataBufferQueue());
		this.importContext = importContext;
	}
	protected abstract Record buildRecord(Object data);
	public Object getKeys(){
		return record.getKeys();
	}
	public void appendData(Data datas){

		try {
			queue.put(datas);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
//		this.esDatas = datas;
//		this.records = esDatas.getDatas();
//		size = records !=null ?records.size():0;
//		pos = 0;
//		totalSize = esDatas.getTotalSize();
//		this.handlerInfo = handlerInfo;


	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws ESDataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName) throws ESDataImportException {
		return record.getValue(colName);
	}


	@Override
	public Object getValue(String colName, int sqlType) throws ESDataImportException {
		return getValue(  colName);
	}
	@Override
	public Object getMetaValue(String fieldName) {
		return record.getMetaValue(fieldName);
	}


	@Override
	public Date getDateTimeValue(String colName) throws ESDataImportException {
		Object value = getValue(  colName);
		if(value == null)
			return null;
		return TranUtil.getDateTimeValue(colName,value,importContext);

	}
	@Override
	public void stop(){
		status = STATUS_STOP;
	}
	@Override
	public void stopTranOnly(){
		status = STATUS_STOPTRANONLY;
	}
	private boolean reachEnd;
	public void reachEend(){
		this.reachEnd = true;
	}

	@Override
	public Boolean next() throws ESDataImportException {
		if(status == STATUS_STOP){
			return false;
		}
		if( pos < size){

			record = buildRecord(records.get(pos));
			pos ++;

			return true;
		}
		else{

			try {

				Data datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
				if(status == STATUS_STOP){
					return false;
				}


				if(datas != null){
					this.records = datas.getDatas();
					size = records != null ? records.size():0;
				}

				if(datas == null || size == 0)
				{
					if(status == STATUS_STOPTRANONLY || importContext.getDataTranPlugin().isPluginStopAppending()){
						return false;
					}
					long pollStartTime = System.currentTimeMillis();
					do{
						datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
						if(status == STATUS_STOP ){
							return false;
						}
						if(datas == null){
							if(reachEnd)
								break;
							if(status == STATUS_STOPTRANONLY || importContext.getDataTranPlugin().isPluginStopAppending())
								return false;
							if(importContext.getFlushInterval() > 0) {
								long interval = System.currentTimeMillis() - pollStartTime;
								if (interval > importContext.getFlushInterval()){
									return null;
								}
							}
							continue;
						}
						this.records = datas.getDatas();
						size = records != null ? records.size():0;
						if(size > 0)
							break;
						else{
							if(status == STATUS_STOPTRANONLY || importContext.getDataTranPlugin().isPluginStopAppending())
								return false;
						}
					}while (true);
					if(datas == null && reachEnd){
						return false;
					}
				}

				pos = 0;
				record = buildRecord(records.get(pos));
				pos ++;
				if(status == STATUS_STOP){
					return false;
				}
				return true;
			} catch (InterruptedException e) {
				return false;
			}
		}
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(record.getKeys());
	}
	public Object getRecord(){
		return record.getData();
	}

	public boolean removed(){
		return record.removed();
	}
	public boolean reachEOFClosed(){
		return this.record.reachEOFClosed() ;
	}
}
