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
     boolean isOldRegistRecord(Status completed ,long registLiveTime);

    boolean isOldRegistRecordWithDeleteTime(Status completed ,long deleteTime);
	LoadCurrentStatus getLoadCurrentStatus();
	void putStatus(Status currentStatus) throws Exception;
	void flushStatus();
	void stop();
	boolean isStoped();
	void initLastValueClumnName();
	void setIncreamentImport(boolean increamentImport);
	void initLastValueType();
	void initTableAndStatus(InitLastValueClumnName initLastValueClumnName);
	String getLastValueClumnName();
	void stopStatusDatasource();

	 void handleLostedTasks(List<Status> losteds , boolean needSyn);
	 void handleOldedRegistedRecordTasks(List<Status> completed);


     void handleOldedRegistedRecordTasks(long deleteTime);

	Map getParamValue(Map params);
//    Object getLastValue();
	Object[] putLastParamValue(Map params);
	void updateStatus(Status currentStatus) throws Exception;
	boolean isIncreamentImport();
	void handleOldedTasks(List<Status> olded);
	void handleOldedTask(Status olded);
	Status getCurrentStatus();
	void addStatus(Status currentStatus) throws DataImportException;
//	void forceflushLastValue(Status currentStatus);

    void flushLastValue(LastValueWrapper lastValue, Status currentStatus, boolean reachEOFClosed);
//	void flushLastValue(LastValueWrapper lastValue,Status currentStatus);

	int getLastValueType();

	List<Status> getPluginStatuses();
    /**
	 * 根据任务id，任务类型，任务状态id获取任务状态
	 * @param jobId
	 * @param jobType
	 * @param statusId
	 * @return
	 */
	Status getStatus(String jobId, String jobType, String statusId);

    /**
	 * 根据fileId获取已完成状态Status
	 * @param fileId
	 * @return
	 */
    Status getComplateStatusByFileId(String fileId);
}
