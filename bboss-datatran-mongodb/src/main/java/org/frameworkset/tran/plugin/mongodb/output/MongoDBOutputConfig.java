package org.frameworkset.tran.plugin.mongodb.output;
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
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;


/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/9/20 11:46
 * @author biaoping.yin
 * @version 1.0
 */
public class MongoDBOutputConfig extends BaseMongoDBConfig implements OutputConfig {

	private String objectIdField;
	private boolean multiCollections = true;
	public boolean isMultiCollections() {
		return multiCollections;
	}

	/**
	 * 将数据输出到多库多表中
	 * @param multiCollections
	 * @return
	 */
	public MongoDBOutputConfig setMultiCollections(boolean multiCollections) {
		this.multiCollections = multiCollections;
		return this;
	}

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new MongoDBOutputDataTranPlugin(importContext,this);
	}



	public String getObjectIdField() {
		return objectIdField;
	}

	public MongoDBOutputConfig setObjectIdField(String objectIdField) {
		this.objectIdField = objectIdField;
		return this;
	}


}