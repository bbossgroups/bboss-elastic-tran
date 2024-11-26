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
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.record.TranMetaDataLazeLoad;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/28 22:37
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class AsynBaseTranResultSet extends  LastValue implements AsynTranResultSet {
	private List records;
	private int pos = 0;
	private int size;


	private BlockingQueue<Data> queue ;
    private boolean preReachEOFRecord;

	public AsynBaseTranResultSet(ImportContext importContext) {
		queue = new ArrayBlockingQueue(importContext.getTranDataBufferQueue());
		this.importContext = importContext;
	}
	protected abstract Record buildRecord(Object data);
	public Object getKeys(){
		return record.getKeys();
	}

    /**
     * 销毁资源
     */
    public void destroy(){
        this.clearQueue();
    }



	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return record.getValue(  i,colName,sqlType);
	}

 


	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return record.getValue(  colName,sqlType);
	}
	@Override
	public Object getMetaValue(String fieldName) {
		return record.getMetaValue(fieldName);
	}




//	@Override
//	public void stopTranOnly(){
//		status = STATUS_STOPTRANONLY;
//	}
	private boolean reachEnd;
	public void reachEend(){
		this.reachEnd = true;
	}

	private boolean stopIterator(){
		return  isStop() || importContext.getDataTranPlugin().checkTranToStop();
//				|| importContext.getDataTranPlugin().isPluginStopREADY();
//				|| importContext.getDataTranPlugin().isStopCollectData();
	}
    private boolean reachEOFRecord(){
        return record.reachEOFRecord();
    }
    private Long pollStartTime ;

    private boolean recordsCheckForceFlush(){
        boolean needCheckForceFlush = false;
        if(this.records == null || this.records.size() == 0)
            needCheckForceFlush = true;
        else{
            if(records.size() == 1 ){
                Object record = records.get(0);
                if(record instanceof Record){
                    needCheckForceFlush = ((Record)record).getAction() == Record.RECORD_DIRECT_IGNORE;
                }
            }
        }
        return needCheckForceFlush;
    }

    private boolean checkForceFlush(NextAssert nextAssert){
        if(importContext.getFlushInterval() > 0) {
            long interval = System.currentTimeMillis() - pollStartTime;
            if (interval > importContext.getFlushInterval()){
                nextAssert.setNeedFlush(true);
                pollStartTime = null;
                return true;
            }
        }
        return false;
    }

    public void appendData(Data datas) throws InterruptedException{

        try {
            if(isStop() || queue == null){
                if(!isStopFromException()) {
                    throw new InterruptedException("AsynBaseTranResultSet already stopped.");
                }
                else {
                    throw new InterruptedException("AsynBaseTranResultSet already stopped by exception.");
                }
            }
            queue.put(datas);
        } catch (InterruptedException e) {
            throw e;
        }
        catch (Throwable e) {
//            throw e;
        }

//		this.esDatas = datas;
//		this.records = esDatas.getDatas();
//		size = records !=null ?records.size():0;
//		pos = 0;
//		totalSize = esDatas.getTotalSize();
//		this.handlerInfo = handlerInfo;


    }
    @Override
    public void clearQueue(){
        if(queue == null){
            return;
        }
        try {

           queue.clear();
           do{
               Data datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
               if(datas == null) {
                   queue = null;
                   break;
               }
               else {
                   queue.clear();
               }
           }while (true);


        } catch (Exception e) {
        }
    }
    @Override
	public NextAssert next() throws DataImportException {
		/**
		 * 要把数据处理完毕，才停迭代器
		 */
//		if(status == STATUS_STOP){
//			return false;
//		}
        NextAssert nextAssert = new NextAssert();
        if(preReachEOFRecord){
            stop(false);
            clearQueue();
            return nextAssert;
        }
		if(baseDataTran.getDataTranPlugin().checkTranToStop()) {//作业已停止或者准备停止，修改迭代器状态，同时清空队列数据
            stop(false);
            clearQueue();
            return nextAssert;
        }
		if( pos < size){

			record = buildRecord(records.get(pos));
            record.setTranMeta(this.getMetaData());
            preReachEOFRecord = reachEOFRecord();
			pos ++;
            nextAssert.setHasNext(true);
			return nextAssert;
		}
		else{

			try {
				Data datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
				/**
				 * 要把数据处理完毕，才停迭代器
				 */
//				if(status == STATUS_STOP){
//					return false;
//				}

                boolean needCheckForceFlush = false;
                if(datas != null){
					this.records = datas.getDatas();
					size = records != null ? records.size():0;
                    needCheckForceFlush = recordsCheckForceFlush();
				}
                else{
                    this.records = null;
                    this.size = 0;
                    needCheckForceFlush = true;
                }

                if(needCheckForceFlush){
                    if(pollStartTime == null)
                        pollStartTime = System.currentTimeMillis();
                }
                else{
                    pollStartTime = null;
                }

				if(datas == null || size == 0)
				{
					if(stopIterator()){
                        stop(false);
                        clearQueue();
                        return nextAssert;
					}

					do{
                        if(queue == null){
                            return nextAssert;
                        }
						datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
                        if(isStopFromException()){//因异常终止则停止后续数据处理，不管有没有数据，如果是正常停止，则需要处理后续数据
                            clearQueue();
                            return nextAssert;
                        }
//						if(isStop() ){
//							return false;
//						}
						if(datas == null){
							if(reachEnd)
								break;
							if(stopIterator()) {
                                stop(false);
                                clearQueue();
                                return nextAssert;
                            }
//							if(importContext.getFlushInterval() > 0) {
//								long interval = System.currentTimeMillis() - pollStartTime;
//								if (interval > importContext.getFlushInterval()){
//                                    nextAssert.setNeedFlush(true);
//                                    pollStartTime = null;
//									return nextAssert;
//								}
//							}
                            if(checkForceFlush( nextAssert))
                                return nextAssert;
							continue;
						}

						this.records = datas.getDatas();
						size = records != null ? records.size():0;
                        needCheckForceFlush = recordsCheckForceFlush();
                        if(needCheckForceFlush){
                            if(pollStartTime == null)
                                pollStartTime = System.currentTimeMillis();
                        }
                        else{
                            pollStartTime = null;
                        }
						if(size > 0) {
                            if(needCheckForceFlush )
                                checkForceFlush(nextAssert);
                            break;
                        }
						else{
							if(stopIterator()) {
                                stop(false);
                                clearQueue();
                                return nextAssert;
                            }
						}
					}while (true);
					if((datas == null )&& reachEnd){
						return nextAssert;
					}
				}
                else{
                    if(needCheckForceFlush){
                        checkForceFlush( nextAssert);
                    }
                }

				pos = 0;
				record = buildRecord(records.get(pos));
                record.setTranMeta(this.getMetaData());
                preReachEOFRecord = reachEOFRecord();
				pos ++;
				/**
				 * 要把数据处理完毕，才停迭代器
				 */
//				if(status == STATUS_STOP){
//					return false;
//				}
                nextAssert.setHasNext(true);
				return nextAssert;
			} catch (InterruptedException e) {
                stop(false);
                clearQueue();
				return nextAssert;
			}
		}
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(new TranMetaDataLazeLoad() {
            @Override
            public String[] lazeLoad() {
                return DefaultTranMetaData.convert(record.getKeys());
            }
        });
	}
	public Object getRecord(){
		return record.getData();
	}

 
 
}
