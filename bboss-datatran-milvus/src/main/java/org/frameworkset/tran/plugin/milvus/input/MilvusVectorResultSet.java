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

import io.milvus.orm.iterator.SearchIterator;
import io.milvus.response.QueryResultsWrapper;
import org.frameworkset.tran.context.ImportContext;

import java.util.List;

/**
 * <p>Description: Milvus向量search结果集</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/8/3 12:27
 * @author biaoping.yin
 * @version 1.0
 */
public class MilvusVectorResultSet extends MilvusResultSet {
	private SearchIterator searchIterator;
	public MilvusVectorResultSet(ImportContext importContext, SearchIterator searchIterator) {
        super(importContext,null);
		this.searchIterator = searchIterator;
	}
    protected void _close(){
        if(searchIterator != null) {
            searchIterator.close();
        }
    }
    protected List<QueryResultsWrapper.RowRecord> _next(){
        return searchIterator.next();
    }
 
}
