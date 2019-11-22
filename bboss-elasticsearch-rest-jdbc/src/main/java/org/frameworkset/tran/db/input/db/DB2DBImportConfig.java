package org.frameworkset.tran.db.input.db;
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

import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.db.DBImportConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/1/11 15:10
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DBImportConfig extends DBImportConfig {


	private DBConfig targetDBConfig;
	private String insertSqlName;

	public DBConfig getTargetDBConfig() {
		return targetDBConfig;
	}

	public void setTargetDBConfig(DBConfig targetDBConfig) {
		this.targetDBConfig = targetDBConfig;
	}

	public String getInsertSqlName() {
		return insertSqlName;
	}

	public void setInsertSqlName(String insertSqlName) {
		this.insertSqlName = insertSqlName;
	}

}
