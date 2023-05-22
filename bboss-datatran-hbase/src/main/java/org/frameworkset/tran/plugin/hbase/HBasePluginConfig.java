package org.frameworkset.tran.plugin.hbase;
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

import org.frameworkset.tran.plugin.BaseConfig;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class HBasePluginConfig extends BaseConfig  {
	private String name = "default";
	private Map<String,String> hbaseClientProperties;
	private int hbaseClientThreadCount;
	private int hbaseClientThreadQueue;
	private long hbaseClientKeepAliveTime;
	private long hbaseClientBlockedWaitTimeout;
	private int hbaseClientWarnMultsRejects;
	private boolean hbaseClientPreStartAllCoreThreads;
	private  Boolean hbaseClientThreadDaemon;

	private String hbaseTable;

	private Integer hbaseBatch;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Map<String, String> getHbaseClientProperties() {
		return hbaseClientProperties;
	}

	public HBasePluginConfig addHbaseClientProperty(String name, String value){
		if(hbaseClientProperties == null){
			hbaseClientProperties = new LinkedHashMap<>();
		}
		hbaseClientProperties.put(name,value);
		return this;
	}
	public HBasePluginConfig setHbaseClientProperties(Map<String, String> properties) {
		this.hbaseClientProperties = properties;
		return this;
	}

	public int getHbaseClientThreadCount() {
		return hbaseClientThreadCount;
	}

	public HBasePluginConfig setHbaseClientThreadCount(int hbaseClientThreadCount) {
		this.hbaseClientThreadCount = hbaseClientThreadCount;
		return this;
	}

	public int getHbaseClientThreadQueue() {
		return hbaseClientThreadQueue;
	}

	public HBasePluginConfig setHbaseClientThreadQueue(int hbaseClientThreadQueue) {
		this.hbaseClientThreadQueue = hbaseClientThreadQueue;
		return this;
	}

	public long getHbaseClientKeepAliveTime() {
		return hbaseClientKeepAliveTime;
	}

	public HBasePluginConfig setHbaseClientKeepAliveTime(long hbaseClientKeepAliveTime) {
		this.hbaseClientKeepAliveTime = hbaseClientKeepAliveTime;
		return this;
	}

	public long getHbaseClientBlockedWaitTimeout() {
		return hbaseClientBlockedWaitTimeout;
	}

	public HBasePluginConfig setHbaseClientBlockedWaitTimeout(long hbaseClientBlockedWaitTimeout) {
		this.hbaseClientBlockedWaitTimeout = hbaseClientBlockedWaitTimeout;
		return this;
	}

	public int getHbaseClientWarnMultsRejects() {
		return hbaseClientWarnMultsRejects;
	}

	public HBasePluginConfig setHbaseClientWarnMultsRejects(int hbaseClientWarnMultsRejects) {
		this.hbaseClientWarnMultsRejects = hbaseClientWarnMultsRejects;
		return this;
	}

	public boolean isHbaseClientPreStartAllCoreThreads() {
		return hbaseClientPreStartAllCoreThreads;
	}

	public HBasePluginConfig setHbaseClientPreStartAllCoreThreads(boolean hbaseClientPreStartAllCoreThreads) {
		this.hbaseClientPreStartAllCoreThreads = hbaseClientPreStartAllCoreThreads;
		return this;
	}

	public Boolean getHbaseClientThreadDaemon() {
		return hbaseClientThreadDaemon;
	}

	public HBasePluginConfig setHbaseClientThreadDaemon(Boolean hbaseClientThreadDaemon) {
		this.hbaseClientThreadDaemon = hbaseClientThreadDaemon;
		return this;
	}

	public String getHbaseTable() {
		return hbaseTable;
	}

	/**
	 * 设置hbase表，表必须自行创建，建表参考示例：
	 * https://gitee.com/bboss/hbase-elasticsearch/blob/master/src/test/java/org/frameworkset/elasticsearch/imp/HBaseHelperTest.java
	 * @param hbaseTable
	 * @return
	 */
	public HBasePluginConfig setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
		return this;
	}


	public Integer getHbaseBatch() {
		return hbaseBatch;
	}

	public HBasePluginConfig setHbaseBatch(Integer hbaseBatch) {
		this.hbaseBatch = hbaseBatch;
		return this;
	}


}
