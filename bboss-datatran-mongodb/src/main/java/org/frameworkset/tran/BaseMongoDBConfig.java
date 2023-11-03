package org.frameworkset.tran;
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
import org.frameworkset.nosql.mongodb.ClientMongoCredential;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.plugin.BaseConfig;

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
public class BaseMongoDBConfig extends BaseConfig  {
	private String name;
	private String serverAddresses;
	private String option;
	private String writeConcern;
	private String readPreference;


	private List<ClientMongoCredential> credentials;
	private int connectionsPerHost = 50;

	private int maxWaitTime = 120000;
	private int socketTimeout = 0;
	private int connectTimeout = 15000;
	private String connectString;



	private Boolean socketKeepAlive = false;

	private String mode;
	private String dbCollection;
	private String db;




	public String getName() {
		return name;
	}

	public BaseMongoDBConfig setName(String name) {
		this.name = name;
		return this;
	}

	public String getServerAddresses() {
		return serverAddresses;
	}

	public BaseMongoDBConfig setServerAddresses(String serverAddresses) {
		this.serverAddresses = serverAddresses;
		return this;
	}

	public String getOption() {
		return option;
	}

	public BaseMongoDBConfig setOption(String option) {
		this.option = option;
		return this;
	}

	public String getWriteConcern() {
		return writeConcern;
	}

	public BaseMongoDBConfig setWriteConcern(String writeConcern) {
		this.writeConcern = writeConcern;
		return this;
	}

	public String getReadPreference() {
		return readPreference;
	}

	public BaseMongoDBConfig setReadPreference(String readPreference) {
		this.readPreference = readPreference;
		return this;
	}



	public int getConnectionsPerHost() {
		return connectionsPerHost;
	}

	public BaseMongoDBConfig setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
		return this;
	}

	public int getMaxWaitTime() {
		return maxWaitTime;
	}

	public BaseMongoDBConfig setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
		return this;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public BaseMongoDBConfig setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
		return this;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public BaseMongoDBConfig setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
		return this;
	}


	public Boolean getSocketKeepAlive() {
		return socketKeepAlive;
	}

	public BaseMongoDBConfig setSocketKeepAlive(Boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
		return this;
	}

	public String getMode() {
		return mode;
	}

	public BaseMongoDBConfig setMode(String mode) {
		this.mode = mode;
		return this;
	}



	public String getDBCollection() {
		return dbCollection;
	}

	public String getDB() {
		return db;
	}

	public BaseMongoDBConfig setDbCollection(String dbCollection) {
		this.dbCollection = dbCollection;
		return this;
	}

	public BaseMongoDBConfig setDb(String db) {
		this.db = db;
		return this;
	}
	public List<ClientMongoCredential> getCredentials() {
		return credentials;
	}

	public BaseMongoDBConfig setCredentials(List<ClientMongoCredential> credentials) {
		this.credentials = credentials;
		return this;
	}

	private String mechanism;
	private String userName;
	@JsonIgnore
	private String password;
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


	public BaseMongoDBConfig setPassword(String password) {
		this.password = password;
		return this;
	}

	public BaseMongoDBConfig setUserName(String userName) {
		this.userName = userName;
		return this;
	}

	public BaseMongoDBConfig setMechanism(String mechanism) {
		this.mechanism = mechanism;
		return this;
	}

	/**
	 * see https://www.mongodb.com/docs/manual/reference/connection-string/#std-label-mongodb-uri
	 * @return
	 */
	public String getConnectString() {
		return connectString;
	}

	public BaseMongoDBConfig setConnectString(String connectString) {
		this.connectString = connectString;
		return this;
	}
}
