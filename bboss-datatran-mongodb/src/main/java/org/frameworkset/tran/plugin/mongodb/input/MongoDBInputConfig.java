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

import com.mongodb.BasicDBObject;
import org.frameworkset.tran.BaseMongoDBConfig;
import org.frameworkset.tran.config.InputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.InputPlugin;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBInputConfig extends BaseMongoDBConfig implements InputConfig {

	private List<String> fetchFields;


//	private DBCollectionFindOptions dbCollectionFindOptions;
	private BasicDBObject query;

//	public DBCollectionFindOptions getDbCollectionFindOptions() {
//		return dbCollectionFindOptions;
//	}
//
//	public MongoDBInputConfig setDbCollectionFindOptions(DBCollectionFindOptions dbCollectionFindOptions) {
//		this.dbCollectionFindOptions = dbCollectionFindOptions;
//		return this;
//	}

	public MongoDBInputConfig setQuery(BasicDBObject query) {
		this.query = query;
		return this;
	}

	public BasicDBObject getQuery() {
		return query;
	}

	public List<String> getFetchFields() {
		return fetchFields;
	}

	public MongoDBInputConfig setFetchFields(List<String> fetchFields) {
		this.fetchFields = fetchFields;
		return this;
	}



	@Override
	public InputPlugin getInputPlugin(ImportContext importContext) {
		return new MongoDBInputDatatranPlugin(importContext);
	}


}
