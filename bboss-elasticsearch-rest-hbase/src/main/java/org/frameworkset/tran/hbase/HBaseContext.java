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

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/28 14:11
 * @author biaoping.yin
 * @version 1.0
 */
public interface HBaseContext {

//	int threadCount,int threadQueue,long keepAliveTime,
//	long blockedWaitTimeout,int warnMultsRejects,boolean preStartAllCoreThreads,final Boolean daemon
	Map<String,String> getHbaseClientProperties();

	public int getHbaseClientThreadCount();
	public String getHbaseTable();
	public String getStartRow();

	public String getEndRow();
	public boolean isIncrementByTimeRange();
	//.setCaching => .setNumberOfRowsFetchSize (客户端每次 rpc fetch 的行数)
	// see fetchSize;
	/**
	 * 客户端缓存的最大字节数
	 * @return
	 */
	public Long getMaxResultSize();
	/**
	 * 客户端每次获取的列数
	 * @return
	 */
	public Integer getHbaseBatch();

	public Filter getScanFilter();
	/**
	 *
	 FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE); //数据只要满足一组过滤器中的一个就可以

	 SingleColumnValueFilter filter1 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,Bytes.toBytes("my value"));

	 list.add(filter1);

	 SingleColumnValueFilter filter2 = new SingleColumnValueFilter(cf,column,CompareOp.EQUAL,Bytes.toBytes("my other value"));



	 * @return
	 */
	public FilterList getScanFilters();

	/**
	 * 如果setFilterIfMissing(true), 有匹配只会返回当前列所在的行数据，基于行的数据 country 也返回了，因为他么你的rowkey是相同的
	 * 如果setFilterIfMissing（false），有匹配的列的值相同会返回，没有此列的 name的也会返回，， 不匹配的name则不会返回。
	 * https://blog.csdn.net/kangkangwanwan/article/details/89332536
	 * @return
	 */
	public Boolean getFilterIfMissing();

	public int getHbaseClientThreadQueue() ;



	public long getHbaseClientKeepAliveTime() ;



	public long getHbaseClientBlockedWaitTimeout() ;



	public int getHbaseClientWarnMultsRejects() ;



	public boolean isHbaseClientPreStartAllCoreThreads() ;


	public Boolean getHbaseClientThreadDaemon();

	/**
	 *
	 * @return
	 */
	public String getIncrementFamilyName();
	public Long getEndTimestamp() ;
	public Long getStartTimestamp();

}
