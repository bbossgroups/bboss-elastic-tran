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
import org.frameworkset.tran.record.TranMetaDataLazeLoadImpl;

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
	/**
	 * 终止标记（Poison Pill）：采集端数据采集完毕时写入队列，用于立即唤醒消费线程
	 */
	private static final Data<?> END_MARKER = new Data<Object>() {
		@Override
		public List<Object> getDatas() {
			return null;
		}
	};
	/**
	 * 当前正在处理的数据批次
	 */
	private List records;
	/**
	 * 当前批次中下一条待处理数据的位置
	 */
	private int pos = 0;
	private int size;

	/**
	 * 数据缓冲队列：采集线程写入，转换线程读取；
	 * 停止或者销毁时置为null，用于拒绝采集线程后续的数据写入，由volatile保证多线程可见性
	 */
	private volatile BlockingQueue<Data> queue ;
	/**
	 * 构造当前批次时的最后一条记录是否已经是EOF记录，是则处理完当前批次后不再迭代
	 */
    private boolean preReachEOFRecord;
	/**
	 * 队列中没有有效数据时开始计时的时间点，0表示尚未开始计时，用于计算强制flush的时间间隔
	 *  仅在空数据周期开始计时，获取有效数据后清零
	 */
	private long pollStartTime ;
	
	/**
	 * 采集端数据是否已经采集完毕
	 */
	private volatile boolean endReached;
	private long flushInterval  ;
	private long asynResultPollTimeOut;
	public AsynBaseTranResultSet(ImportContext importContext) {
		queue = new ArrayBlockingQueue(importContext.getTranDataBufferQueue());
		this.importContext = importContext;
		flushInterval = importContext.getFlushInterval();
		asynResultPollTimeOut = importContext.getAsynResultPollTimeOut();
	}
	protected abstract Record buildRecord(Object data);
	public Object getKeys(){
		return record.getKeys();
	}
	private volatile boolean destroyed;
	/**
	 * 销毁资源,调用方法是数据迭代处理已经结束，不会再调用next方法
	 */
    public void destroy(){
		destroyed = true;
        this.clearQueue(true);
 
		
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
	
	
	
	
	/**
	 * 采集端数据采集完毕，通知转换线程处理完队列中的剩余数据后结束迭代。
	 * 向队列写入终止标记（Poison Pill），立即唤醒可能在 poll 阻塞的消费线程。
	 */
	public void reachEnd(){
		this.endReached = true;
		// 写入终止标记，唤醒消费线程
		BlockingQueue<Data> q = this.queue;
		if(q != null){
			try {
				q.offer((Data) END_MARKER, 1000L, TimeUnit.MILLISECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private boolean stopIterator(){
		return  isStop() || dataTranPlugin.checkTranToStop() ;
//				|| importContext.getDataTranPlugin().isPluginStopREADY();
//				|| importContext.getDataTranPlugin().isStopCollectData();
	}
    private boolean reachEOFRecord(){
        return record.reachEOFRecord();
    }
  

    /**
     * 判断当前批次是否需要检查强制刷数据的时间间隔：
     * 只有空批次，或者批次中只有一条被标记为直接忽略的记录（仅用于传递数据位置状态）时才需要检查，
     * 避免每一批数据都去计算flush时间间隔
     * @return true 需要检查强制刷数据的时间间隔
     */
    private boolean recordsCheckForceFlush(){
        boolean needCheckForceFlush = false;
		int size = this.records != null ?this.records.size():0;
        if(this.records == null || size == 0)
            needCheckForceFlush = true;
        else{
            if(size == 1 ){
                Object record = records.get(0);
                if(record instanceof Record){
                    needCheckForceFlush = ((Record)record).getAction() == Record.RECORD_DIRECT_IGNORE;
                }
            }
        }
        return needCheckForceFlush;
    }
	/**
	 * 检查开始等待数据的时间是否已经超过flushInterval，超过则要求上层强制刷数据
	 * @return true 需要强制刷数据
	 */
    private boolean checkForceFlush(NextAssert nextAssert){
		if(flushInterval > 0 && pollStartTime > 0) {
            long interval = System.currentTimeMillis() - pollStartTime;
            if (interval > flushInterval){
                nextAssert.setNeedFlush(true);
                pollStartTime = 0;
                return true;
            }
        }
        return false;
    }

    public void appendData(Data datas) throws DataAppendException{
		if (datas == null) {
			return;
		}
		BlockingQueue<Data> writeQueue = this.queue;
		if(isStop() || writeQueue == null || destroyed){
			if(!isStopFromException()) {
				throw new DataAppendException("AsynBaseTranResultSet already stopped.");
			}
			else {
				throw new DataAppendException("AsynBaseTranResultSet already stopped by exception.");
			}
		}
		/**
		 * 队列可能在判断之后才被清空，此时数据写入的是一个已经废弃的队列，等同于丢弃，
		 * 这是结果集停止后的正常行为，不需要再做额外处理
		 */
		
//		writeQueue.put(datas);

//		boolean success = writeQueue.offer(datas, 1000L, TimeUnit.MILLISECONDS);
//		if(!success ){
//			do {
//				if(isStop()){
//					break;
//				}
//				writeQueue = this.queue;
//				if (writeQueue != null) {
//					success = writeQueue.offer(datas, 1000L, TimeUnit.MILLISECONDS);
//					if(success){
//						break;
//					}
//				}
//				else{
//					break;
//				}
//			}while (true);
//		}
		BlockingQueue<Data> q = null;
		while (true) {
			q = this.queue;
			if (destroyed || q == null || isStop()) {
				throw new DataAppendException("AsynBaseTranResultSet already stopped.");  // 或 return 丢弃
			}
			
			
			try {
				if (q.offer(datas, 2000L, TimeUnit.MILLISECONDS)) {
					break;
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new DataAppendException("AsynBaseTranResultSet already stopped by exception.",e);
//				throw new RuntimeException(e);
			}
//			Thread.sleep(500l);
		}
		 

    }
    @Override
    public void clearQueue(boolean fromDestory){
         
		BlockingQueue<Data> clearQueue = this.queue;
		this.queue = null;
		if(clearQueue != null){
			
			clearQueue.clear();
			if(fromDestory){
				// 如果是外部终止作业，则写入终止标记，唤醒消费线程
				try {
					clearQueue.offer((Data) END_MARKER, 100L, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
		
//        try {
//
//           queue.clear();
//           do{
//               Data datas = queue.poll(importContext.getAsynResultPollTimeOut(), TimeUnit.MILLISECONDS);
//               if(datas == null) {
//                   queue = null;
//                   break;
//               }
//               else {
//                   queue.clear();
//               }
//           }while (true);
//
//
//        } catch (Exception e) {
//        }
    }
	
	/**
	 * 由 ResultSet 复用，调用方不得跨 next() 保存或跨线程共享
	 */
	private  final NextAssert nextAssert = new NextAssert();
    @Override
	public NextAssert next() throws DataImportException {
		/**
		 * 要把数据处理完毕，才停迭代器
		 */
//		if(status == STATUS_STOP){
//			return false;
//		}
		nextAssert.reset();
	
        if(preReachEOFRecord){
            stop(false);
            clearQueue(false);
            return nextAssert;
        }
		if(dataTranPlugin.checkTranToStop()) {//作业已停止或者准备停止，修改迭代器状态，同时清空队列数据
            stop(false);
            clearQueue(false);
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
				BlockingQueue<Data> pollQueue = this.queue;
				if(pollQueue == null){//结果集已经被销毁
					return nextAssert;
				}
				Data datas = pollQueue.poll(asynResultPollTimeOut, TimeUnit.MILLISECONDS);
				/**
				 * 要把数据处理完毕，才停迭代器
				 */
//				if(status == STATUS_STOP){
//					return false;
//				}

                boolean needCheckForceFlush = false;
                if(datas != null){
					// 检查是否为终止标记
					if(datas == END_MARKER){
						return nextAssert;
					}
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
                    if(pollStartTime == 0)
                        pollStartTime = System.currentTimeMillis();
                }
                else{
                    pollStartTime = 0;
                }

				if(datas == null || size == 0)
				{
					if(stopIterator()){
                        stop(false);
                        clearQueue(false);
                        return nextAssert;
					}

					do{
						pollQueue = this.queue;
						 
                        if(pollQueue == null){
                            return nextAssert;
                        }
						datas = pollQueue.poll(asynResultPollTimeOut, TimeUnit.MILLISECONDS);
                        if(isStopFromException()){//因异常终止则停止后续数据处理，不管有没有数据，如果是正常停止，则需要处理后续数据
                            clearQueue(false);
                            return nextAssert;
                        }
//						if(isStop() ){
//							return false;
//						}
						if(datas == null){
							if(endReached)
								break;
							if(stopIterator()) {
                                stop(false);
                                clearQueue(false);
                                return nextAssert;
                            }
                            if(checkForceFlush( nextAssert))
                                return nextAssert;
							continue;
						}
						// 检查是否为终止标记
						if(datas == END_MARKER){
							return nextAssert;
						}

						this.records = datas.getDatas();
						size = records != null ? records.size():0;
                        needCheckForceFlush = recordsCheckForceFlush();
                        if(needCheckForceFlush){
                            if(pollStartTime == 0)
                                pollStartTime = System.currentTimeMillis();
                        }
                        else{
                            pollStartTime = 0;
                        }
						if(size > 0) {
                            if(needCheckForceFlush )
                                checkForceFlush(nextAssert);
                            break;
                        }
						else{
							if(stopIterator()) {
                                stop(false);
                                clearQueue(false);
                                return nextAssert;
                            }
						}
					}while (true);
					if((datas == null )&& endReached){
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
				Thread.currentThread().interrupt();
                stop(false);
                clearQueue(false);
				return nextAssert;
			}
		}
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(new TranMetaDataLazeLoadImpl(record));
	}
	public Object getRecord(){
		return record.getData();
	}

 
 
}
