package org.frameworkset.tran.plugin;
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

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/18
 * @author biaoping.yin
 * @version 1.0
 */
public interface InputPlugin {
	public ImportContext getImportContext();
	public Context buildContext(TaskContext taskContext, TranResultSet jdbcResultSet, BatchContext batchContext);
	public void initStatusTableId();
	public String getLastValueVarName();
	public Long getTimeRangeLastValue();
	public void doImportData( TaskContext taskContext)  throws DataImportException;
	public void afterInit();
	public void beforeInit();
	public void init();
	public boolean isMultiTran();
	public void destroy(boolean waitTranStop);

	public boolean isEnablePluginTaskIntercept() ;
	public boolean isEnableAutoPauseScheduled();
	public void setDataTranPlugin(DataTranPlugin dataTranPlugin);
}
