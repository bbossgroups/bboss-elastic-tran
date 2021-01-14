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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.db.DBImportContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/21 16:13
 * @author biaoping.yin
 * @version 1.0
 */
public class DB2DBImportContext extends DBImportContext implements DB2DBContext{
	private DB2DBImportConfig db2DBImportConfig;


	protected  DataTranPlugin buildDataTranPlugin(){
		return new DB2DBDataTranPlugin(this);
	}
	protected void init(BaseImportConfig baseImportConfig){
		super.init(baseImportConfig);
		db2DBImportConfig = (DB2DBImportConfig)baseImportConfig;
	}
	public DB2DBImportContext(){
		this(new DB2DBImportConfig());

	}
	public DB2DBImportContext(BaseImportConfig baseImportConfig){
		super(baseImportConfig);

	}



}
