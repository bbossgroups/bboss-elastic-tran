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
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.mongodb.cdc.ChangeStreamPipeline;
import org.frameworkset.tran.mongodb.cdc.Operation;
import org.frameworkset.tran.plugin.InputPlugin;
import org.frameworkset.tran.plugin.mongodb.input.MongoDBInputDatatranPlugin;

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
public class MongoCDCInputConfig extends BaseMongoDBConfig implements InputConfig {

	private String replicaSet;

	private boolean updateLookup = true;
	private boolean includePreImage;
	private Long cursorMaxAwaitTime;

	private String dbIncludeList;
	private String dbExcludeList;
	private String collectionIncludeList;
	private String collectionExcludeList;
	private String signalDataCollection;



	private Long lastTimeStamp;


	/**
	 * 默认十分钟执行一次拦截器监控数据afterCall方法
	 */
	private long metricsInterval ;

	private boolean enableIncrement;

	private List<Operation> skippedOperations;

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
		return new MongoDBInputDatatranPlugin(importContext);
	}


	public String getReplicaSet() {
		return replicaSet;
	}

	@Override
	public void build(ImportBuilder importBuilder) {
		super.build(importBuilder);
		if(userPipeline != null && userPipeline.equals("")) {
			ChangeStreamPipeline changeStreamPipeline = new ChangeStreamPipeline(userPipeline);
		}
	}

	public MongoCDCInputConfig setReplicaSet(String replicaSet) {
		this.replicaSet = replicaSet;
		return this;
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

	public Long getCursorMaxAwaitTime() {
		return cursorMaxAwaitTime;
	}

	public MongoCDCInputConfig setCursorMaxAwaitTime(Long cursorMaxAwaitTime) {
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

	public MongoCDCInputConfig addSkipedOperation(Operation operation){
		if(skippedOperations == null){
			skippedOperations = new ArrayList<>();
		}
		skippedOperations.add(operation);
		return this;
	}

	public List<Operation> getSkippedOperations() {
		return skippedOperations;
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
}
