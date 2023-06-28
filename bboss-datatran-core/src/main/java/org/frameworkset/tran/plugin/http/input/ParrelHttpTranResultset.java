package org.frameworkset.tran.plugin.http.input;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.http.HttpResult;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/30
 * @author biaoping.yin
 * @version 1.0
 */
public class ParrelHttpTranResultset extends LastValue implements TranResultSet {
    private static Logger logger = LoggerFactory.getLogger(ParrelHttpTranResultset.class);
	private BlockingQueue<HttpResult<Map>> httpResults;
    private HttpResult<Map> currentResult;
	private Map current;
	private Iterator<Map> iterator;
//	private boolean stoped;
	private HttpInputConfig httpInputConfig;
    private ExecutorService blockedExecutor;
	private List<QueryAction> queryActions;
    private int tasks;
    private int completedQueryActions;
    private Object queryLock = new Object();
    private DataTranPlugin dataTranPlugin;
	public ParrelHttpTranResultset(List<QueryAction> queryActions, ImportContext importContext, ExecutorService blockedExecutor) {
		this.importContext = importContext;
		httpInputConfig = (HttpInputConfig) importContext.getInputConfig();
		this.queryActions = queryActions;
        tasks = queryActions.size();
        this.blockedExecutor = blockedExecutor;
        this.dataTranPlugin = importContext.getDataTranPlugin();

	}
    private void completeQueryAction(){
        synchronized (queryLock) {
            completedQueryActions++;
        }
    }
    private void addResult(HttpResult<Map> httpResult){
        httpResults.add(httpResult);
    }

    private void doQueryAction(QueryAction queryAction){
        blockedExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    HttpResult<Map> httpResult = queryAction.execute();
                    List<Map> datas = httpResult.getDatas();
                    if(datas != null && datas.size() > 0) {
                        try {
                            httpResults.put(httpResult);

                        } catch (InterruptedException e) {
                            completeQueryAction();
                        }
                    }
                    else{
                        completeQueryAction();
                    }
                }
                catch (Throwable e){
                    completeQueryAction();
                    dataTranPlugin.throwException(getTaskContext(),e);

                }

            }
        });
    }
    private void parrelExecute(){
        for(QueryAction queryAction: queryActions){
            doQueryAction( queryAction);
        }
    }
	public  void init(){
        httpResults = new ArrayBlockingQueue<>(httpInputConfig.getQueryResultQueue());
        parrelExecute();

	}
	@Override
	public TaskContext getRecordTaskContext() {
		return record.getTaskContext();
	}

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Object getValue(String colName) throws DataImportException {
		return record.getValue(colName);

	}

	@Override
	public Object getValue(String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
	}

	@Override
	public Date getDateTimeValue(String colName) throws DataImportException {
		return record.getDateTimeValue(colName);

	}

	@Override
	public NextAssert next() throws DataImportException {
        NextAssert nextAssert = new NextAssert();
        boolean hasNext = false;
        do {
            if(isStop() || importContext.getInputPlugin().isStopCollectData()) {
                hasNext = false;
                break;
            }
            if (currentResult == null) {
                if (completedQueryActions < tasks) {
                    try {
                        currentResult = httpResults.poll(5000L, TimeUnit.MILLISECONDS);
                        if(currentResult != null){
                            List<Map> datas = currentResult.getDatas();//放入队列的结果都是经过非空判断，所以此处无需进行非空判断
                            iterator = datas.iterator();
                            current = iterator.next();
                            record = new HttpRecord(currentResult,current,getTaskContext());
                            hasNext = true;
                            break;
                        }
                        else{
                            continue;
                        }

                    } catch (InterruptedException e) {
                        hasNext = false;
                        break;
                    }
                } else {
                    hasNext = false;
                    break;
                }
            }
            else{
                hasNext = iterator.hasNext();
                if( hasNext){
                    current = iterator.next();
                    record = new HttpRecord(currentResult,current,getTaskContext());
                    break;
                }
                else{
                    if(currentResult.hasMore()){
                        if(isStop() )
                            return nextAssert;

                        doQueryAction( currentResult.getQueryAction());
                        currentResult = null;
                        iterator = null;

                    }
                    else {
                        currentResult = null;
                        iterator = null;
                        completeQueryAction();
                    }

                }
            }
        }while (true);
        nextAssert.setHasNext(hasNext);
		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(current.keySet());
	}

	public Object getKeys(){
		return record.getKeys();
	}
	@Override
	public Object getRecord() {
		return current;
	}

	@Override
	public Record getCurrentRecord() {
		return record;
	}

//	@Override
//	public void stop() {
//		stoped = true;
//	}


	@Override
	public Object getMetaValue(String fieldName) {
		return getValue(fieldName);
	}

}
