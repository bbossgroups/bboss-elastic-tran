package org.frameworkset.tran.plugin.milvus.input;
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

import io.milvus.orm.iterator.QueryIterator;
import io.milvus.response.QueryResultsWrapper;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.NextAssert;
import org.frameworkset.tran.record.TranMetaDataLazeLoad;

import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusResultSet extends LastValue implements TranResultSet {
	private QueryIterator queryIterator;
    private List<QueryResultsWrapper.RowRecord> res;
    private MilvusRecord milvusRecord;
    private int pos;
	public MilvusResultSet(ImportContext importContext, QueryIterator queryIterator) {
		this.importContext = importContext;
		this.queryIterator = queryIterator;
	}
 

	@Override
	public Object getValue(int i, String colName, int sqlType) throws DataImportException {
		return getValue(  colName);
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
		if(isStop() || importContext.getInputPlugin().isStopCollectData()) {
            queryIterator.close();
            return nextAssert;
        }
        boolean hasNext = false;
        pos ++;
        if(res != null && pos < res.size()){            
            hasNext = true;
        }
        else{
            pos = 0;
            res = queryIterator.next();
            hasNext = !res.isEmpty();
            
        }
        if( hasNext){

            milvusRecord = new MilvusRecord(res.get(pos),getTaskContext(),importContext);
            milvusRecord.setTranMeta(this.getMetaData());
            record = milvusRecord;
           
        }
        else{
            queryIterator.close();
        }
        
        nextAssert.setHasNext(hasNext);
		return nextAssert;
	}

	@Override
	public TranMeta getMetaData() {
		return new DefaultTranMetaData(new TranMetaDataLazeLoad() {
            @Override
            public String[] lazeLoad() {
                return DefaultTranMetaData.convert(milvusRecord.getKeys());
            }
        });
	}

	public Object getKeys(){
		return record.getKeys();
	}
	@Override
	public Object getRecord() {
		return record.getData();
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
