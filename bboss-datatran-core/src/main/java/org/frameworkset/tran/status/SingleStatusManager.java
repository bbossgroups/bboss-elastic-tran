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

import com.frameworkset.common.poolman.SQLExecutor;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/25 10:26
 * @author biaoping.yinq
 * @version 1.0
 */
public class SingleStatusManager  extends BaseStatusManager{
	private static Logger logger = LoggerFactory.getLogger(SingleStatusManager.class);
	private Status currentStatus ;

	private long lastUpdateTime;
	private long lastPutTime;
	public SingleStatusManager(DataTranPlugin dataTranPlugin) {
		super( dataTranPlugin);
	}

	protected void _putStatus(Status currentStatus){

		this.currentStatus = currentStatus;
		lastPutTime = System.currentTimeMillis();
	}
	protected void _flushStatus() throws Exception {
		if(lastUpdateTime < lastPutTime) {
			if(currentStatus.getJobId() == null) {
				SQLExecutor.updateWithDBName(statusDbname, updateSQL, currentStatus.getTime(), convertLastValue(currentStatus.getLastValue()),convertStrLastValue(currentStatus.getStrLastValue()),
						lastValueType, currentStatus.getFilePath(), currentStatus.getRelativeParentDir(), currentStatus.getFileId(),
						currentStatus.getStatus(), currentStatus.getId(),currentStatus.getJobType());
			}
			else{
				SQLExecutor.updateWithDBName(statusDbname, updateByJobIdSQL, currentStatus.getTime(), convertLastValue(currentStatus.getLastValue()),convertStrLastValue(currentStatus.getStrLastValue()),
						lastValueType, currentStatus.getFilePath(), currentStatus.getRelativeParentDir(), currentStatus.getFileId(),
						currentStatus.getStatus(), currentStatus.getId(),currentStatus.getJobId(),currentStatus.getJobType());
			}
			lastUpdateTime = System.currentTimeMillis();
		}
	}
}
