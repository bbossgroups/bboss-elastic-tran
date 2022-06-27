package org.frameworkset.tran.plugin.mongodb.input;
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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.frameworkset.util.SimpleStringUtil;
import com.mongodb.DBObject;
import com.mongodb.client.model.DBCollectionFindOptions;
import org.frameworkset.nosql.mongodb.ClientMongoCredential;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BaseConfig;
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
public class MongoDBInputConfig extends BaseConfig implements InputConfig {
	private String name;
	private String serverAddresses;
	private String option;
	private String writeConcern;
	private String readPreference;
	private Boolean autoConnectRetry = true;
	private DBObject fetchFields;


	private List<ClientMongoCredential> credentials;
	private int connectionsPerHost = 50;

	private int maxWaitTime = 120000;
	private int socketTimeout = 0;
	private int connectTimeout = 15000;


	/**是否启用sql日志，true启用，false 不启用，*/
	private int threadsAllowedToBlockForConnectionMultiplier = 5;
	private Boolean socketKeepAlive = false;

	private String mode;
	private DBCollectionFindOptions dbCollectionFindOptions;
	private DBObject query;
	private String dbCollection;
	private String db;

	public String getName() {
		return name;
	}

	public MongoDBInputConfig setName(String name) {
		this.name = name;
		return this;
	}

	public String getServerAddresses() {
		return serverAddresses;
	}

	public MongoDBInputConfig setServerAddresses(String serverAddresses) {
		this.serverAddresses = serverAddresses;
		return this;
	}

	public String getOption() {
		return option;
	}

	public MongoDBInputConfig setOption(String option) {
		this.option = option;
		return this;
	}

	public String getWriteConcern() {
		return writeConcern;
	}

	public MongoDBInputConfig setWriteConcern(String writeConcern) {
		this.writeConcern = writeConcern;
		return this;
	}

	public String getReadPreference() {
		return readPreference;
	}

	public MongoDBInputConfig setReadPreference(String readPreference) {
		this.readPreference = readPreference;
		return this;
	}

	public Boolean getAutoConnectRetry() {
		return autoConnectRetry;
	}

	public MongoDBInputConfig setAutoConnectRetry(Boolean autoConnectRetry) {
		this.autoConnectRetry = autoConnectRetry;
		return this;
	}

	public int getConnectionsPerHost() {
		return connectionsPerHost;
	}

	public MongoDBInputConfig setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
		return this;
	}

	public int getMaxWaitTime() {
		return maxWaitTime;
	}

	public MongoDBInputConfig setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
		return this;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public MongoDBInputConfig setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public MongoDBInputConfig setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
		return this;
	}

	public int getThreadsAllowedToBlockForConnectionMultiplier() {
		return threadsAllowedToBlockForConnectionMultiplier;
	}

	public MongoDBInputConfig setThreadsAllowedToBlockForConnectionMultiplier(int threadsAllowedToBlockForConnectionMultiplier) {
		this.threadsAllowedToBlockForConnectionMultiplier = threadsAllowedToBlockForConnectionMultiplier;
		return this;
	}

	public Boolean getSocketKeepAlive() {
		return socketKeepAlive;
	}

	public MongoDBInputConfig setSocketKeepAlive(Boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
		return this;
	}

	public String getMode() {
		return mode;
	}

	public MongoDBInputConfig setMode(String mode) {
		this.mode = mode;
		return this;
	}

	public DBCollectionFindOptions getDBCollectionFindOptions() {
		return this.dbCollectionFindOptions;
	}

	public DBObject getQuery() {
		return query;
	}

	public String getDBCollection() {
		return dbCollection;
	}

	public String getDB() {
		return db;
	}

	public MongoDBInputConfig setDbCollectionFindOptions(DBCollectionFindOptions dbCollectionFindOptions) {
		this.dbCollectionFindOptions = dbCollectionFindOptions;
		return this;
	}

	public MongoDBInputConfig setQuery(DBObject query) {
		this.query = query;
		return this;
	}

	public MongoDBInputConfig setDbCollection(String dbCollection) {
		this.dbCollection = dbCollection;
		return this;
	}

	public MongoDBInputConfig setDb(String db) {
		this.db = db;
		return this;
	}
	public List<ClientMongoCredential> getCredentials() {
		return credentials;
	}

	public MongoDBInputConfig setCredentials(List<ClientMongoCredential> credentials) {
		this.credentials = credentials;
		return this;
	}

	public DBObject getFetchFields() {
		return fetchFields;
	}

	public MongoDBInputConfig setFetchFields(DBObject fetchFields) {
		this.fetchFields = fetchFields;
		return this;
	}

	private String mechanism;
	private String userName;
	@JsonIgnore
	private String password;
	@Override
	public void build(ImportBuilder importBuilder) {

		if(SimpleStringUtil.isNotEmpty(userName) && SimpleStringUtil.isNotEmpty(password)) {
			if (credentials == null) {
				credentials = new ArrayList<ClientMongoCredential>();

				ClientMongoCredential clientMongoCredential = new ClientMongoCredential();
				clientMongoCredential.setDatabase(db);
				clientMongoCredential.setMechanism(mechanism);
				clientMongoCredential.setUserName(userName);
				clientMongoCredential.setPassword(password);
				credentials.add(clientMongoCredential);
			}
		}
	}

	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new MongoDBInputDatatranPlugin(importContext);
	}

	public MongoDBInputConfig setPassword(String password) {
		this.password = password;
		return this;
	}

	public MongoDBInputConfig setUserName(String userName) {
		this.userName = userName;
		return this;
	}

	public MongoDBInputConfig setMechanism(String mechanism) {
		this.mechanism = mechanism;
		return this;
	}
}
