package org.frameworkset.tran.plugin.hbase.input;
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
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseInputConfig extends BaseConfig implements InputConfig {
	private Map<String,String> hbaseClientProperties;
	private int hbaseClientThreadCount;
	private int hbaseClientThreadQueue;
	private long hbaseClientKeepAliveTime;
	private long hbaseClientBlockedWaitTimeout;
	private int hbaseClientWarnMultsRejects;
	private boolean hbaseClientPreStartAllCoreThreads;
	private  Boolean hbaseClientThreadDaemon;

	private String hbaseTable;
	private String startRow;
	private String endRow;
	private Long maxResultSize;
	private Integer hbaseBatch;
	/**
	 * filter和filterList只能指定一个
	 */
	private FilterList filterList;
	/**
	 * filter和filterList只能指定一个
	 */
	private Filter filter;
	private Boolean filterIfMissing;
	private Long startTimestamp;
	private Long endTimestamp;
	private String incrementFamilyName;
	private boolean incrementByTimeRange;
	private Boolean hbaseAsynMetricsEnable;
	public Map<String, String> getHbaseClientProperties() {
		return hbaseClientProperties;
	}

	public void setHbaseClientProperties(Map<String, String> properties) {
		this.hbaseClientProperties = properties;
	}

	public int getHbaseClientThreadCount() {
		return hbaseClientThreadCount;
	}

	public void setHbaseClientThreadCount(int hbaseClientThreadCount) {
		this.hbaseClientThreadCount = hbaseClientThreadCount;
	}

	public int getHbaseClientThreadQueue() {
		return hbaseClientThreadQueue;
	}

	public void setHbaseClientThreadQueue(int hbaseClientThreadQueue) {
		this.hbaseClientThreadQueue = hbaseClientThreadQueue;
	}

	public long getHbaseClientKeepAliveTime() {
		return hbaseClientKeepAliveTime;
	}

	public void setHbaseClientKeepAliveTime(long hbaseClientKeepAliveTime) {
		this.hbaseClientKeepAliveTime = hbaseClientKeepAliveTime;
	}

	public long getHbaseClientBlockedWaitTimeout() {
		return hbaseClientBlockedWaitTimeout;
	}

	public void setHbaseClientBlockedWaitTimeout(long hbaseClientBlockedWaitTimeout) {
		this.hbaseClientBlockedWaitTimeout = hbaseClientBlockedWaitTimeout;
	}

	public int getHbaseClientWarnMultsRejects() {
		return hbaseClientWarnMultsRejects;
	}

	public void setHbaseClientWarnMultsRejects(int hbaseClientWarnMultsRejects) {
		this.hbaseClientWarnMultsRejects = hbaseClientWarnMultsRejects;
	}

	public boolean isHbaseClientPreStartAllCoreThreads() {
		return hbaseClientPreStartAllCoreThreads;
	}

	public void setHbaseClientPreStartAllCoreThreads(boolean hbaseClientPreStartAllCoreThreads) {
		this.hbaseClientPreStartAllCoreThreads = hbaseClientPreStartAllCoreThreads;
	}

	public Boolean getHbaseClientThreadDaemon() {
		return hbaseClientThreadDaemon;
	}

	public void setHbaseClientThreadDaemon(Boolean hbaseClientThreadDaemon) {
		this.hbaseClientThreadDaemon = hbaseClientThreadDaemon;
	}

	public String getHbaseTable() {
		return hbaseTable;
	}

	public void setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
	}

	public String getStartRow() {
		return startRow;
	}

	public void setStartRow(String startRow) {
		this.startRow = startRow;
	}

	public String getEndRow() {
		return endRow;
	}

	public void setEndRow(String endRow) {
		this.endRow = endRow;
	}

	public Long getMaxResultSize() {
		return maxResultSize;
	}

	public void setMaxResultSize(Long maxResultSize) {
		this.maxResultSize = maxResultSize;
	}

	public Integer getHbaseBatch() {
		return hbaseBatch;
	}

	public void setHbaseBatch(Integer hbaseBatch) {
		this.hbaseBatch = hbaseBatch;
	}

	public FilterList getFilterList() {
		return filterList;
	}

	public void setFilterList(FilterList filterList) {
		this.filterList = filterList;
	}

	public Boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	public void setFilterIfMissing(Boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
	}

	public String getIncrementFamilyName() {
		return incrementFamilyName;
	}

	public void setIncrementFamilyName(String incrementFamilyName) {
		this.incrementFamilyName = incrementFamilyName;
	}

	public Long getStartTimestamp() {
		return startTimestamp;
	}

	public void setStartTimestamp(Long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}

	public Long getEndTimestamp() {
		return endTimestamp;
	}

	public void setEndTimestamp(Long endTimestamp) {
		this.endTimestamp = endTimestamp;
	}

	public boolean isIncrementByTimeRange() {
		return incrementByTimeRange;
	}

	public void setIncrementByTimeRange(boolean incrementByTimeRange) {
		this.incrementByTimeRange = incrementByTimeRange;
	}

	public Filter getFilter() {
		return filter;
	}

	public void setFilter(Filter filter) {
		this.filter = filter;
	}
	public Boolean getHbaseAsynMetricsEnable(){
		return hbaseAsynMetricsEnable;

	}

	public void setHbaseAsynMetricsEnable(Boolean hbaseAsynMetricsEnable) {
		this.hbaseAsynMetricsEnable = hbaseAsynMetricsEnable;
	}

	@Override
	public void build(ImportBuilder importBuilder) {
//		if(getEsIdGenerator() == null || hBaseImportConfig.getEsIdGenerator() == hBaseImportConfig.DEFAULT_EsIdGenerator) {
//			hBaseImportConfig.setEsIdGenerator(new HBaseEsIdGenerator());
//		}
		ImportIncreamentConfig importIncreamentConfig = importBuilder.getImportIncreamentConfig();
		if(importIncreamentConfig != null){
			if(importIncreamentConfig.isLastValueDateType()
					&& importIncreamentConfig.getLastValueColumn() == null ){
				setIncrementByTimeRange(true);
				importIncreamentConfig.setLastValueColumn("_");
			}
		}
	}
	@Override
	public void afterBuild(ImportBuilder importBuilder,ImportContext importContext){
		if(importContext.getOutputConfig() instanceof ElasticsearchOutputConfig) {
			ElasticsearchOutputConfig elasticsearchInputInputConfig = (ElasticsearchOutputConfig) importContext.getOutputConfig();

			if (elasticsearchInputInputConfig.getEsIdGenerator() == null || elasticsearchInputInputConfig.getEsIdGenerator() == ElasticsearchOutputConfig.DEFAULT_EsIdGenerator) {
				elasticsearchInputInputConfig.setEsIdGenerator(new HBaseEsIdGenerator());
			}
		}
	}

	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new HBaseInputDatatranPlugin(importContext);
	}
}
