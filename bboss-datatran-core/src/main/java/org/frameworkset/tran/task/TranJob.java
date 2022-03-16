package org.frameworkset.tran.task;
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

import org.frameworkset.tran.BaseDataTran;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;

/**
 * <p>Description:
 * 处理作业逻辑接口：String文本类型输出、Record对象类型输出</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/15
 * @author biaoping.yin
 * @version 1.0
 */
public interface TranJob {
	/**
	 * 串行批处理导入
	 * @return
	 */
	public String batchExecute(SerialTranCommand serialTranCommand ,
							   Status currentStatus,
							   ImportContext importContext,
							   ImportContext targetImportContext,
							   TranResultSet jdbcResultSet, BaseDataTran baseDataTran);
	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	public String parallelBatchExecute(final ParrelTranCommand serialTranCommand ,
									   Status currentStatus,
									   ImportContext importContext,
									   ImportContext targetImportContext,
									   TranResultSet jdbcResultSet, BaseDataTran baseDataTran);

	/**
	 * 串行处理数据，importContext.serialAllData为true，一次性将数据加载处理到内存，一次性写入 false 一次处理一条写入一条
	 * @param serialTranCommand
	 * @param currentStatus
	 * @param importContext
	 * @param targetImportContext
	 * @param jdbcResultSet
	 * @param baseDataTran
	 * @return
	 */
	public String serialExecute(SerialTranCommand serialTranCommand,
								Status currentStatus,
								ImportContext importContext,
								ImportContext targetImportContext,
								TranResultSet jdbcResultSet, BaseDataTran baseDataTran);
}
