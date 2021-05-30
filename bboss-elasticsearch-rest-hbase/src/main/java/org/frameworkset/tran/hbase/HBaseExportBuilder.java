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
import org.frameworkset.tran.DataStream;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.ExportResultHandler;
import org.frameworkset.tran.WrapedExportResultHandler;
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.es.ESExportResultHandler;
import org.frameworkset.tran.hbase.input.es.HBase2ESInputPlugin;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 21:29
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseExportBuilder extends BaseImportBuilder {
	private Map<String,String> hbaseClientProperties;

	public HBaseExportBuilder setHbaseClientProperties(Map<String, String> hbaseClientProperties) {
		this.hbaseClientProperties = hbaseClientProperties;
		return this;
	}

	public Boolean getHbaseAsynMetricsEnable() {
		return hbaseAsynMetricsEnable;
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext,ImportContext targetImportContext){
		return new HBase2ESInputPlugin(  importContext,  targetImportContext);
	}

	public HBaseExportBuilder setHbaseAsynMetricsEnable(Boolean hbaseAsynMetricsEnable) {
		this.hbaseAsynMetricsEnable = hbaseAsynMetricsEnable;
		return this;
	}

	private Boolean hbaseAsynMetricsEnable;
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
	private String incrementFamilyName;
	private Long startTimestamp;
	private Long endTimestamp;
	public Map<String, String> getHbaseClientProperties() {
		return hbaseClientProperties;
	}

	public static HBaseExportBuilder newInstance(){
		return new HBaseExportBuilder();
	}
	 public HBaseExportBuilder addHbaseClientProperty(String name,String value){
		if(hbaseClientProperties == null){
			hbaseClientProperties = new HashMap<String, String>();
		}
		 hbaseClientProperties.put(name,value);
		return this;
	 }
	protected ImportContext buildImportContext(BaseImportConfig importConfig) {
		HBaseContextImpl hBaseContext = new HBaseContextImpl((HBaseImportConfig)importConfig);
		hBaseContext.init();
		return hBaseContext;
	}
	protected void setTargetImportContext(DataStream dataStream){

		dataStream.setTargetImportContext(dataStream.getImportContext());
	}
	public int getHbaseClientThreadCount() {
		return hbaseClientThreadCount;
	}

	public HBaseExportBuilder setHbaseClientThreadCount(int hbaseClientThreadCount) {
		this.hbaseClientThreadCount = hbaseClientThreadCount;
		return this;
	}

	public int getHbaseClientThreadQueue() {
		return hbaseClientThreadQueue;
	}

	public HBaseExportBuilder setHbaseClientThreadQueue(int hbaseClientThreadQueue) {
		this.hbaseClientThreadQueue = hbaseClientThreadQueue;
		return this;
	}

	public long getHbaseClientKeepAliveTime() {
		return hbaseClientKeepAliveTime;
	}

	public HBaseExportBuilder setHbaseClientKeepAliveTime(long hbaseClientKeepAliveTime) {
		this.hbaseClientKeepAliveTime = hbaseClientKeepAliveTime;
		return this;
	}

	public long getHbaseClientBlockedWaitTimeout() {
		return hbaseClientBlockedWaitTimeout;
	}

	public HBaseExportBuilder setHbaseClientBlockedWaitTimeout(long hbaseClientBlockedWaitTimeout) {
		this.hbaseClientBlockedWaitTimeout = hbaseClientBlockedWaitTimeout;
		return this;
	}

	public int getHbaseClientWarnMultsRejects() {
		return hbaseClientWarnMultsRejects;
	}

	public HBaseExportBuilder setHbaseClientWarnMultsRejects(int hbaseClientWarnMultsRejects) {
		this.hbaseClientWarnMultsRejects = hbaseClientWarnMultsRejects;
		return this;
	}

	public boolean isHbaseClientPreStartAllCoreThreads() {
		return hbaseClientPreStartAllCoreThreads;
	}

	public HBaseExportBuilder setHbaseClientPreStartAllCoreThreads(boolean hbaseClientPreStartAllCoreThreads) {
		this.hbaseClientPreStartAllCoreThreads = hbaseClientPreStartAllCoreThreads;
		return this;
	}

	public Boolean getHbaseClientThreadDaemon() {
		return hbaseClientThreadDaemon;
	}

	public HBaseExportBuilder setHbaseClientThreadDaemon(Boolean hbaseClientThreadDaemon) {
		this.hbaseClientThreadDaemon = hbaseClientThreadDaemon;
		return this;
	}

	@Override
	protected WrapedExportResultHandler buildExportResultHandler(ExportResultHandler exportResultHandler) {
		return new ESExportResultHandler(exportResultHandler);
	}

	public DataStream builder(){
		super.builderConfig();
//		this.buildDBConfig();
//		this.buildStatusDBConfig();
		try {
			if(logger.isInfoEnabled()) {
				logger.info("HBase Import Configs:");
				logger.info(this.toString());
			}
		}
		catch (Exception e){

		}
		HBaseImportConfig hBaseImportConfig = new HBaseImportConfig();
		super.buildImportConfig(hBaseImportConfig);
		if(hBaseImportConfig.getEsIdGenerator() == null || hBaseImportConfig.getEsIdGenerator() == hBaseImportConfig.DEFAULT_EsIdGenerator) {
			hBaseImportConfig.setEsIdGenerator(new HBaseEsIdGenerator());
		}
		if(this.importIncreamentConfig != null){
			if(importIncreamentConfig.isLastValueDateType()
					&& importIncreamentConfig.getLastValueColumn() == null ){
				hBaseImportConfig.setIncrementByTimeRange(true);
				importIncreamentConfig.setLastValueColumn("_");
			}
		}
		hBaseImportConfig.setHbaseClientProperties(hbaseClientProperties);
		hBaseImportConfig.setHbaseAsynMetricsEnable(hbaseAsynMetricsEnable);
		hBaseImportConfig.setHbaseClientThreadCount(hbaseClientThreadCount);
		hBaseImportConfig.setHbaseClientThreadQueue(hbaseClientThreadQueue);
		hBaseImportConfig.setHbaseClientKeepAliveTime(hbaseClientKeepAliveTime);
		hBaseImportConfig.setHbaseClientBlockedWaitTimeout(hbaseClientBlockedWaitTimeout);
		hBaseImportConfig.setHbaseClientWarnMultsRejects(hbaseClientWarnMultsRejects);
		hBaseImportConfig.setHbaseClientPreStartAllCoreThreads(hbaseClientPreStartAllCoreThreads);
		hBaseImportConfig.setHbaseClientThreadDaemon(hbaseClientThreadDaemon);

		hBaseImportConfig.setHbaseTable(hbaseTable);
		hBaseImportConfig.setStartRow(startRow);
		hBaseImportConfig.setEndRow(endRow);
		hBaseImportConfig.setMaxResultSize(maxResultSize);
		hBaseImportConfig.setHbaseBatch(hbaseBatch);
		hBaseImportConfig.setFilterList(filterList);
		hBaseImportConfig.setFilter(filter);
		hBaseImportConfig.setFilterIfMissing(filterIfMissing);
		hBaseImportConfig.setIncrementFamilyName(incrementFamilyName);
		hBaseImportConfig.setStartTimestamp(this.startTimestamp);
		hBaseImportConfig.setEndTimestamp(this.endTimestamp);
		/**
		MongoDBImportConfig es2DBImportConfig = new MongoDBImportConfig();
		super.buildImportConfig(es2DBImportConfig);
		es2DBImportConfig.setName(this.name);
		es2DBImportConfig.setServerAddresses(serverAddresses);
		es2DBImportConfig.setOption(option);//private String option;
		es2DBImportConfig.setWriteConcern(writeConcern);//private String writeConcern;
		es2DBImportConfig.setReadPreference(readPreference);//private String readPreference;
		es2DBImportConfig.setAutoConnectRetry(autoConnectRetry);//private Boolean autoConnectRetry = true;

		es2DBImportConfig.setConnectionsPerHost(connectionsPerHost);//private int connectionsPerHost = 50;

		es2DBImportConfig.setMaxWaitTime(maxWaitTime);//private int maxWaitTime = 120000;
		es2DBImportConfig.setSocketTimeout(socketTimeout);//private int socketTimeout = 0;
		es2DBImportConfig.setConnectTimeout(connectTimeout);//private int connectTimeout = 15000;


		//是否启用sql日志，true启用，false 不启用，
		es2DBImportConfig.setThreadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier);//private int threadsAllowedToBlockForConnectionMultiplier;
		es2DBImportConfig.setSocketKeepAlive(socketKeepAlive);//private Boolean socketKeepAlive = false;

		es2DBImportConfig.setMode( mode);

		es2DBImportConfig.setDbCollectionFindOptions( this.dbCollectionFindOptions);
		es2DBImportConfig.setQuery( this.query);
		es2DBImportConfig.setFetchFields(this.fetchFields);
		es2DBImportConfig.setDbCollection( this.dbCollection);
		es2DBImportConfig.setDb( this.db);
		es2DBImportConfig.setCredentials(this.credentials);
//		MongoDB2ESDataStreamImpl dataStream = new MongoDB2ESDataStreamImpl();
//		dataStream.setMongoDB2ESImportConfig(es2DBImportConfig);
		super.buildDBImportConfig(es2DBImportConfig);
		*/
		DataStream dataStream = this.createDataStream();
		dataStream.setImportConfig(hBaseImportConfig);
		dataStream.setImportContext(this.buildImportContext(hBaseImportConfig));
		setTargetImportContext(  dataStream);
		dataStream.setDataTranPlugin(this.buildDataTranPlugin(dataStream.getImportContext(),dataStream.getTargetImportContext()));

		return dataStream;
	}


	public String getHbaseTable() {
		return hbaseTable;
	}

	public HBaseExportBuilder setHbaseTable(String hbaseTable) {
		this.hbaseTable = hbaseTable;
		return this;
	}

	public String getStartRow() {
		return startRow;
	}

	public HBaseExportBuilder setStartRow(String startRow) {
		this.startRow = startRow;
		return this;
	}

	public String getEndRow() {
		return endRow;
	}

	public HBaseExportBuilder setEndRow(String endRow) {
		this.endRow = endRow;
		return this;
	}

	public Long getMaxResultSize() {
		return maxResultSize;
	}

	public HBaseExportBuilder setMaxResultSize(Long maxResultSize) {
		this.maxResultSize = maxResultSize;
		return this;
	}

	public Integer getHbaseBatch() {
		return hbaseBatch;
	}

	public HBaseExportBuilder setHbaseBatch(Integer hbaseBatch) {
		this.hbaseBatch = hbaseBatch;
		return this;
	}

	public FilterList getFilterList() {
		return filterList;
	}

	public HBaseExportBuilder setFilterList(FilterList filterList) {
		this.filterList = filterList;
		return this;
	}

	public Boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	public HBaseExportBuilder setFilterIfMissing(Boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
		return this;
	}

	public String getIncrementFamilyName() {
		return incrementFamilyName;
	}

	public HBaseExportBuilder setIncrementFamilyName(String incrementFamilyName) {
		this.incrementFamilyName = incrementFamilyName;
		return this;
	}

	public Long getStartTimestamp() {
		return startTimestamp;
	}

	public HBaseExportBuilder setStartTimestamp(Long startTimestamp) {
		this.startTimestamp = startTimestamp;
		return this;
	}

	public Long getEndTimestamp() {
		return endTimestamp;
	}

	public HBaseExportBuilder setEndTimestamp(Long endTimestamp) {
		this.endTimestamp = endTimestamp;
		return this;
	}

	public Filter getFilter() {
		return filter;
	}

	public HBaseExportBuilder setFilter(Filter filter) {
		this.filter = filter;
		return this;
	}
}
