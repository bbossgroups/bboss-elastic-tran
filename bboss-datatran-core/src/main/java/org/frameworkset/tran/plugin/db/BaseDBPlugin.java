package org.frameworkset.tran.plugin.db;
/**
 * Copyright 2022 bboss
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

import com.frameworkset.common.poolman.util.DBStartResult;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.BasePlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/20
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseDBPlugin extends BasePlugin {
	protected Logger logger = LoggerFactory.getLogger(this.getClass());
	/**
	 * 包含所有启动成功的db数据源
	 */
	protected DBStartResult dbStartResult = new DBStartResult();

	public BaseDBPlugin(ImportContext importContext) {
		super(importContext);
	}

	public void destroy(boolean waitTranStop) {
		DataTranPluginImpl.stopDatasources(dbStartResult);
	}



}
