package org.frameworkset.tran.output.db;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.output.AsynDBOutPutDataTran;
import org.frameworkset.tran.db.output.DBOutPutContext;
import org.frameworkset.tran.input.file.FileBaseDataTranPlugin;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;

/**
 * <p>Description: 采集日志数据并导入Database插件，支持增量和全量导入</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/3/29 11:25
 * @author yin-bp@163.com
 * @version 1.0
 */
public class FileLog2DBDataTranPlugin extends FileBaseDataTranPlugin {
	protected DBOutPutContext dbOutPutContext;
	public FileLog2DBDataTranPlugin(ImportContext importContext, ImportContext targetImportContext) {
		super(importContext, targetImportContext);
		dbOutPutContext = (DBOutPutContext) targetImportContext;
	}
	@Override
	public void beforeInit() {
		if(importContext.getDbConfig() != null)
			this.initDS(importContext.getDbConfig());
		if(dbOutPutContext.getTargetDBConfig() != null)
			this.initDS(dbOutPutContext.getTargetDBConfig());
		TranUtil.initTargetSQLInfo(dbOutPutContext,dbOutPutContext.getTargetDBConfig());
		super.beforeInit();
	}
	@Override
	protected BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, Status currentStatus) {

		AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,jdbcResultSet,importContext,   targetImportContext,  currentStatus);
		asynDBOutPutDataTran.init();
		return asynDBOutPutDataTran;

	}
}
