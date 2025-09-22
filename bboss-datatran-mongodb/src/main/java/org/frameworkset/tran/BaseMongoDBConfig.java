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
import org.frameworkset.nosql.mongodb.CustomSettingBuilder;
import org.frameworkset.tran.config.ImportBuilder;
import org.frameworkset.tran.context.ImportContext;
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
public class BaseMongoDBConfig<T extends BaseMongoDBConfig> extends BaseConfig<T>  {
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

    @JsonIgnore
    private CustomSettingBuilder customSettingBuilder;


	public String getName() {
		return name;
	}

	public T setName(String name) {
		this.name = name;
		return (T)this;
	}

	public String getServerAddresses() {
		return serverAddresses;
	}

	public T setServerAddresses(String serverAddresses) {
		this.serverAddresses = serverAddresses;
		return (T)this;
	}

	public String getOption() {
		return option;
	}

	public T setOption(String option) {
		this.option = option;
		return (T)this;
	}

	public String getWriteConcern() {
		return writeConcern;
	}

	public T setWriteConcern(String writeConcern) {
		this.writeConcern = writeConcern;
		return (T)this;
	}

	public String getReadPreference() {
		return readPreference;
	}

	public T setReadPreference(String readPreference) {
		this.readPreference = readPreference;
		return (T)this;
	}



	public int getConnectionsPerHost() {
		return connectionsPerHost;
	}

	public T setConnectionsPerHost(int connectionsPerHost) {
		this.connectionsPerHost = connectionsPerHost;
		return (T)this;
	}

	public int getMaxWaitTime() {
		return maxWaitTime;
	}

	public T setMaxWaitTime(int maxWaitTime) {
		this.maxWaitTime = maxWaitTime;
		return (T)this;
	}

	public int getSocketTimeout() {
		return socketTimeout;
	}

	public T setSocketTimeout(int socketTimeout) {
		this.socketTimeout = socketTimeout;
		return (T)this;
	}

	public int getConnectTimeout() {
		return connectTimeout;
	}

	public T  setConnectTimeout(int connectTimeout) {
		this.connectTimeout = connectTimeout;
		return (T)this;
	}


	public Boolean getSocketKeepAlive() {
		return socketKeepAlive;
	}

	public T  setSocketKeepAlive(Boolean socketKeepAlive) {
		this.socketKeepAlive = socketKeepAlive;
		return (T)this;
	}

	public String getMode() {
		return mode;
	}

	public T  setMode(String mode) {
		this.mode = mode;
		return (T)this;
	}



	public String getDBCollection() {
		return dbCollection;
	}

	public String getDB() {
		return db;
	}

	public T  setDbCollection(String dbCollection) {
		this.dbCollection = dbCollection;
		return (T)this;
	}

	public T  setDb(String db) {
		this.db = db;
		return (T)this;
	}
	public List<ClientMongoCredential> getCredentials() {
		return credentials;
	}

	public T  setCredentials(List<ClientMongoCredential> credentials) {
		this.credentials = credentials;
		return (T)this;
	}

	private String mechanism;
	private String userName;
	@JsonIgnore
	private String password;


	private String authDb;

	public String getAuthDb() {
		return authDb;
	}

	public T  setAuthDb(String authDb) {
		this.authDb = authDb;
		return (T)this;
	}
	public void build(ImportContext importContext, ImportBuilder importBuilder) {

		if(SimpleStringUtil.isNotEmpty(userName) && SimpleStringUtil.isNotEmpty(password)) {
			if (credentials == null) {
				credentials = new ArrayList<ClientMongoCredential>();
				ClientMongoCredential clientMongoCredential = new ClientMongoCredential();
				clientMongoCredential.setDatabase(authDb == null?db:authDb);
				clientMongoCredential.setMechanism(mechanism);
				clientMongoCredential.setUserName(userName);
				clientMongoCredential.setPassword(password);
				credentials.add(clientMongoCredential);
			}
		}
	}


	public T  setPassword(String password) {
		this.password = password;
		return (T)this;
	}

	public T  setUserName(String userName) {
		this.userName = userName;
		return (T)this;
	}

	/**
	 * MONGODB-AWS
	 * GSSAPI
	 * PLAIN
	 * MONGODB_X509
	 * SCRAM-SHA-1
	 * SCRAM-SHA-256
	 */
	public T  setMechanism(String mechanism) {
		this.mechanism = mechanism;
		return (T)this;
	}

	/**
	 * see https://www.mongodb.com/docs/manual/reference/connection-string/#std-label-mongodb-uri
	 * @return
	 */
	public String getConnectString() {
		return connectString;
	}

	public T  setConnectString(String connectString) {
		this.connectString = connectString;
		return (T)this;
	}

    public CustomSettingBuilder getCustomSettingBuilder() {
        return customSettingBuilder;
    }

    public T  setCustomSettingBuilder(CustomSettingBuilder customSettingBuilder) {
        this.customSettingBuilder = customSettingBuilder;
        return (T)this;
    }
}
