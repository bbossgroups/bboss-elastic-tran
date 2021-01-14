package org.frameworkset.tran.db.output;
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
import org.frameworkset.tran.db.DBContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/15 21:27
 * @author biaoping.yin
 * @version 1.0
 */
public interface DBOutPutContext extends DBContext {
	public DBConfig getTargetDBConfig() ;
	/**
	 * 数据库相关配置
	 * @return
	 */
	public TranSQLInfo getTargetSqlInfo() ;

	public void setTargetSqlInfo(TranSQLInfo sqlInfo) ;

	public String getInsertSqlName() ;
	public String getInsertSql();



}
