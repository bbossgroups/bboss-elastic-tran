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
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.plugin.hbase.HBasePluginConfig;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class HBaseInputConfig extends HBasePluginConfig implements InputConfig {

	private String startRow;
	private String endRow;
	private Long maxResultSize;
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

	public String getStartRow() {
		return startRow;
	}

	public HBaseInputConfig setStartRow(String startRow) {
		this.startRow = startRow;
		return this;
	}

	public String getEndRow() {
		return endRow;
	}

	public HBaseInputConfig setEndRow(String endRow) {
		this.endRow = endRow;
		return this;
	}

	public Long getMaxResultSize() {
		return maxResultSize;
	}

	public HBaseInputConfig setMaxResultSize(Long maxResultSize) {
		this.maxResultSize = maxResultSize;
		return this;
	}



	public FilterList getFilterList() {
		return filterList;
	}

	public HBaseInputConfig setFilterList(FilterList filterList) {
		this.filterList = filterList;
		return this;
	}

	public Boolean getFilterIfMissing() {
		return filterIfMissing;
	}

	public HBaseInputConfig setFilterIfMissing(Boolean filterIfMissing) {
		this.filterIfMissing = filterIfMissing;
		return this;
	}

	public String getIncrementFamilyName() {
		return incrementFamilyName;
	}

	public HBaseInputConfig setIncrementFamilyName(String incrementFamilyName) {
		this.incrementFamilyName = incrementFamilyName;
		return this;
	}

	public Long getStartTimestamp() {
		return startTimestamp;
	}

	public HBaseInputConfig setStartTimestamp(Long startTimestamp) {
		this.startTimestamp = startTimestamp;
		return this;
	}

	public Long getEndTimestamp() {
		return endTimestamp;
	}

	public HBaseInputConfig setEndTimestamp(Long endTimestamp) {
		this.endTimestamp = endTimestamp;
		return this;
	}

	public boolean isIncrementByTimeRange() {
		return incrementByTimeRange;
	}

	public HBaseInputConfig setIncrementByTimeRange(boolean incrementByTimeRange) {
		this.incrementByTimeRange = incrementByTimeRange;
		return this;
	}

	public Filter getFilter() {
		return filter;
	}

	public HBaseInputConfig setFilter(Filter filter) {
		this.filter = filter;
		return this;
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
