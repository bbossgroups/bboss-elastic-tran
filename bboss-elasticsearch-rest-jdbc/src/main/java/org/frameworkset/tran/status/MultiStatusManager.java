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

import com.frameworkset.common.poolman.BatchHandler;
import com.frameworkset.common.poolman.SQLExecutor;
import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.schedule.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/25 10:26
 * @author biaoping.yinq
 * @version 1.0
 */
public class MultiStatusManager extends BaseStatusManager{
	private static Logger logger = LoggerFactory.getLogger(MultiStatusManager.class);
	private Map<String,WrapStatus> statuses = new LinkedHashMap<>();
	private long lastUpdateTime;
	public MultiStatusManager(String statusDbname, String updateSQL,
							  int lastValueType, DataTranPlugin dataTranPlugin){
		super( statusDbname, updateSQL,lastValueType,dataTranPlugin);
	}
	static class WrapStatus{
		Status currentStatus;
		long lastPutTime;

	}
	protected void _putStatus(Status currentStatus){
		WrapStatus wrapStatus = new WrapStatus();
		wrapStatus.currentStatus = currentStatus ;
		wrapStatus.lastPutTime = System.currentTimeMillis();
		statuses.put(currentStatus.getFileId(),wrapStatus);

	}
	protected void _flushStatus()  throws Exception{
		if(logger.isDebugEnabled()){
			logger.debug("flushStatus start.");
		}
		List<Status> datas = new ArrayList<>();
		Set<Map.Entry<String,WrapStatus>> statusSet = statuses.entrySet();
		WrapStatus wrapStatus = null;
		for(Map.Entry<String,WrapStatus> entry : statusSet){
			wrapStatus = entry.getValue();
			if(lastUpdateTime < wrapStatus.lastPutTime )
				datas.add(wrapStatus.currentStatus);
		}
		if(datas.size() > 0) {
			SQLExecutor.executeBatch(statusDbname, updateSQL, datas, 100, new BatchHandler<Status>() {
				@Override
				public void handler(PreparedStatement stmt, Status record, int i) throws SQLException {
					stmt.setLong(1, record.getTime());
					stmt.setObject(2, record.getLastValue());
					stmt.setInt(3, lastValueType);
					stmt.setString(4, record.getFilePath());
					stmt.setString(5, record.getFileId());
					stmt.setInt(6, record.getStatus());
					stmt.setInt(7, record.getId());
				}
			});
			lastUpdateTime = System.currentTimeMillis();
			for(Status status: datas){
				statuses.remove(status.getFileId());
			}

		}
		if(logger.isDebugEnabled()){
			logger.debug("flush {} Statuses end .",datas.size());
		}


	}
}
