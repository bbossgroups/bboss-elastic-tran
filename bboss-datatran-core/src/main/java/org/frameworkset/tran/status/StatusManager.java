package org.frameworkset.tran.status;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.schedule.Status;

import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/25 10:33
 * @author biaoping.yin
 * @version 1.0
 */
public interface StatusManager {
	public LoadCurrentStatus getLoadCurrentStatus();
	public void putStatus(Status currentStatus) throws Exception;
	public void flushStatus();
	public void stop();
	public boolean isStoped();
	public void initLastValueClumnName();
	public void setIncreamentImport(boolean increamentImport);
	public void initLastValueType();
	public void initTableAndStatus(InitLastValueClumnName initLastValueClumnName);
	public String getLastValueClumnName();
	public void stopStatusDatasource();

	public  void handleLostedTasks(List<Status> losteds , boolean needSyn);
	public  void handleCompletedTasks(List<Status> completed , boolean needSyn, long registLiveTime);
	public Map getParamValue(Map params);
//    public Object getLastValue();
	public Object[] putLastParamValue(Map params);
	public void updateStatus(Status currentStatus) throws Exception;
	public boolean isIncreamentImport();
	public void handleOldedTasks(List<Status> olded);
	public void handleOldedTask(Status olded);
	public Status getCurrentStatus();
	public void addStatus(Status currentStatus) throws DataImportException;
//	public void forceflushLastValue(Status currentStatus);

    public void flushLastValue(LastValueWrapper lastValue, Status currentStatus, boolean reachEOFClosed);
//	public void flushLastValue(LastValueWrapper lastValue,Status currentStatus);

	public int getLastValueType();

	List<Status> getPluginStatuses();
	public Status getStatus(String jobId, String jobType, String statusId);
}
