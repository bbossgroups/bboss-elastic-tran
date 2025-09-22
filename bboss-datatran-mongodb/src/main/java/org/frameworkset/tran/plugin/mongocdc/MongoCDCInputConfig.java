package org.frameworkset.tran.plugin.mongocdc;
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

import org.frameworkset.tran.BaseMongoDBConfig;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.cdc.ChangeStreamPipeline;
import org.frameworkset.tran.plugin.InputPlugin;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoCDCInputConfig extends BaseMongoDBConfig<MongoCDCInputConfig> implements InputConfig<MongoCDCInputConfig> {

	private String replicaSet;

	private boolean updateLookup = true;
	private boolean includePreImage;
	private long cursorMaxAwaitTime;

	private String dbIncludeList;
	private String dbExcludeList;
	private String collectionIncludeList;
	private String collectionExcludeList;
	private String signalDataCollection;



	private Long lastTimeStamp;
	private String position;


	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval ;

	private boolean enableIncrement;

	private List<Integer> includedOperations;

	public String getUserPipeline() {
		return userPipeline;
	}

	public MongoCDCInputConfig setUserPipeline(String userPipeline) {
		this.userPipeline = userPipeline;
		return this;
	}

	private String userPipeline;
	private ChangeStreamPipeline changeStreamPipeline;
	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new MongoCDCInputDatatranPlugin(importContext);
	}


	public String getReplicaSet() {
		return replicaSet;
	}

	@Override
	public void build(ImportContext importContext,ImportBuilder importBuilder) {
		super.build(  importContext,importBuilder);
		if(userPipeline != null && userPipeline.equals("")) {
			ChangeStreamPipeline changeStreamPipeline = new ChangeStreamPipeline(userPipeline);
		}
	}

	public MongoCDCInputConfig setReplicaSet(String replicaSet) {
		this.replicaSet = replicaSet;
		return this;
	}
	@Override
	public DataTranPlugin buildDataTranPlugin(ImportContext importContext){
		DataTranPlugin dataTranPlugin = new MongoCDCDataTranPluginImpl(importContext);
		return dataTranPlugin;
	}
	public boolean isUpdateLookup() {
		return updateLookup;
	}

	public MongoCDCInputConfig setUpdateLookup(boolean updateLookup) {
		this.updateLookup = updateLookup;
		return this;
	}

	public boolean isIncludePreImage() {
		return includePreImage;
	}

	public MongoCDCInputConfig setIncludePreImage(boolean includePreImage) {
		this.includePreImage = includePreImage;
		return this;
	}

	public long getCursorMaxAwaitTime() {
		return cursorMaxAwaitTime;
	}

	public MongoCDCInputConfig setCursorMaxAwaitTime(long cursorMaxAwaitTime) {
		this.cursorMaxAwaitTime = cursorMaxAwaitTime;
		return this;
	}

	public String getDbIncludeList() {
		return dbIncludeList;
	}

	public MongoCDCInputConfig setDbIncludeList(String dbIncludeList) {
		this.dbIncludeList = dbIncludeList;
		return this;
	}

	public String getDbExcludeList() {
		return dbExcludeList;
	}

	public MongoCDCInputConfig setDbExcludeList(String dbExcludeList) {
		this.dbExcludeList = dbExcludeList;
		return this;
	}

	public String getCollectionIncludeList() {
		return collectionIncludeList;
	}

	public MongoCDCInputConfig setCollectionIncludeList(String collectionIncludeList) {
		this.collectionIncludeList = collectionIncludeList;
		return this;
	}

	public String getCollectionExcludeList() {
		return collectionExcludeList;
	}

	public MongoCDCInputConfig setCollectionExcludeList(String collectionExcludeList) {
		this.collectionExcludeList = collectionExcludeList;
		return this;
	}

	public String getSignalDataCollection() {
		return signalDataCollection;
	}

	public MongoCDCInputConfig setSignalDataCollection(String signalDataCollection) {
		this.signalDataCollection = signalDataCollection;
		return this;
	}

	/**
	 * Record.    public final int RECORD_INSERT = 0;
	 *     public final int RECORD_UPDATE = 1;
	 *     public final int RECORD_DELETE = 2;
	 *     public final int RECORD_DDL = 5;
	 * @param operation
	 * @return
	 */
	public MongoCDCInputConfig addIncludeOperation(int operation){
		if(includedOperations == null){
			includedOperations = new ArrayList<>();
		}
		includedOperations.add(operation);
		return this;
	}

	public List<Integer> getIncludedOperations() {
		return includedOperations;
	}

	public ChangeStreamPipeline getChangeStreamPipeline() {
		return changeStreamPipeline;
	}

	public boolean isEnableIncrement() {
		return enableIncrement;
	}

	public MongoCDCInputConfig setEnableIncrement(boolean enableIncrement) {
		this.enableIncrement = enableIncrement;
		return this;
	}

	public long getMetricsInterval() {
		return metricsInterval;
	}

	public MongoCDCInputConfig setMetricsInterval(long metricsInterval) {
		this.metricsInterval = metricsInterval;
		return this;
	}
	public Long getLastTimeStamp() {
		return lastTimeStamp;
	}

	public MongoCDCInputConfig setLastTimeStamp(Long lastTimeStamp) {
		this.lastTimeStamp = lastTimeStamp;
		return this;
	}

	public String getPosition() {
		return position;
	}

	public MongoCDCInputConfig setPosition(String position) {
		this.position = position;
		return this;
	}

	@Override
	public String getDBCollection() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getDB() {
		throw new UnsupportedOperationException();
	}

	@Override
	public MongoCDCInputConfig setDbCollection(String dbCollection) {
		throw new UnsupportedOperationException();
	}

	@Override
	public MongoCDCInputConfig setDb(String db) {
		throw new UnsupportedOperationException();
	}
}
