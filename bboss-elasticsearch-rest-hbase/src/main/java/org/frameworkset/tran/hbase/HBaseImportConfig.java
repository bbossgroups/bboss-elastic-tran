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

import org.apache.hadoop.hbase.filter.FilterList;
import org.frameworkset.tran.config.BaseImportConfig;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseImportConfig extends BaseImportConfig {
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
	private Long hbaseBatch;
	private FilterList filterList;
	private Boolean filterIfMissing;
	private String incrementFamilyName;

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

	public Long getHbaseBatch() {
		return hbaseBatch;
	}

	public void setHbaseBatch(Long hbaseBatch) {
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
}
