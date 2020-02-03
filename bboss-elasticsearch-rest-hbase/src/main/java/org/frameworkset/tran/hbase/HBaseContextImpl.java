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
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.BaseImportContext;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.hbase.input.HBase2ESInputPlugin;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/28 14:11
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseContextImpl extends BaseImportContext implements HBaseContext{
	private HBaseImportConfig hBaseImportConfig;
	public HBaseContextImpl(HBaseImportConfig importConfig) {
		super(importConfig);

	}
	public Context buildContext(TranResultSet jdbcResultSet, BatchContext batchContext){
		return new HBaseRecordContextImpl(this, jdbcResultSet, batchContext);
	}
	protected void init(BaseImportConfig baseImportConfig){
		this.hBaseImportConfig = (HBaseImportConfig)baseImportConfig;
	}
	public boolean isIncrementByTimeRange() {
		return hBaseImportConfig.isIncrementByTimeRange();
	}
	@Override
	protected DataTranPlugin buildDataTranPlugin() {
		return new HBase2ESInputPlugin(this);
	}

	public Map<String, String> getHbaseClientProperties() {
		return hBaseImportConfig.getHbaseClientProperties();
	}



	public int getHbaseClientThreadCount() {
		return hBaseImportConfig.getHbaseClientThreadCount();
	}

	@Override
	public String getHbaseTable() {
		return hBaseImportConfig.getHbaseTable();
	}

	@Override
	public String getStartRow() {
		return hBaseImportConfig.getStartRow();
	}

	@Override
	public String getEndRow() {
		return hBaseImportConfig.getEndRow();
	}

	@Override
	public Long getMaxResultSize() {
		return hBaseImportConfig.getMaxResultSize();
	}

	@Override
	public Integer getHbaseBatch() {
		return hBaseImportConfig.getHbaseBatch();
	}

	@Override
	public FilterList getScanFilters() {
		return hBaseImportConfig.getFilterList();
	}

	public Filter getScanFilter() {
		return hBaseImportConfig.getFilter();
	}

	@Override
	public Boolean getFilterIfMissing() {
		return hBaseImportConfig.getFilterIfMissing();
	}


	public int getHbaseClientThreadQueue() {
		return hBaseImportConfig.getHbaseClientThreadQueue();
	}



	public long getHbaseClientKeepAliveTime() {
		return hBaseImportConfig.getHbaseClientKeepAliveTime();
	}



	public long getHbaseClientBlockedWaitTimeout() {
		return hBaseImportConfig.getHbaseClientBlockedWaitTimeout();
	}



	public int getHbaseClientWarnMultsRejects() {
		return hBaseImportConfig.getHbaseClientWarnMultsRejects();
	}



	public boolean isHbaseClientPreStartAllCoreThreads() {
		return hBaseImportConfig.isHbaseClientPreStartAllCoreThreads();
	}



	public Boolean getHbaseClientThreadDaemon() {
		return hBaseImportConfig.getHbaseClientThreadDaemon();
	}

	@Override
	public String getIncrementFamilyName() {
		return hBaseImportConfig.getIncrementFamilyName();
	}
	public Long getEndTimestamp() {
		return hBaseImportConfig.getEndTimestamp();
	}
	public Long getStartTimestamp() {
		return hBaseImportConfig.getStartTimestamp();
	}

}
