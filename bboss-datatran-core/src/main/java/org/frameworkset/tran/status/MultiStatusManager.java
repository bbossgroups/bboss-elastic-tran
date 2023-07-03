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
 * @author biaoping.yin
 * @version 1.0
 */
public class MultiStatusManager extends BaseStatusManager{
	private static Logger logger = LoggerFactory.getLogger(MultiStatusManager.class);
	private Map<String,WrapStatus> statuses = new LinkedHashMap<>();
	private long liveTime = 2 * 24 * 60 * 60 * 1000;

	public MultiStatusManager(DataTranPlugin dataTranPlugin){
		super(dataTranPlugin);
	}
	static class WrapStatus{
		Status currentStatus;
		long lastPutTime;
        boolean needUpdate;

	}
	protected void _putStatus(Status currentStatus){
		WrapStatus wrapStatus = statuses.get(currentStatus.getFileId());
		if(wrapStatus == null) {
			wrapStatus = new WrapStatus();
			wrapStatus.currentStatus = currentStatus;
			wrapStatus.lastPutTime = System.currentTimeMillis();
            wrapStatus.needUpdate = true;
			statuses.put(currentStatus.getFileId(), wrapStatus);
		}
        else {
            boolean needUpdate = importContext.needUpdateLastValueWrapper(wrapStatus.currentStatus.getOriginCurrentLastValueWrapper(), currentStatus.getOriginCurrentLastValueWrapper());
            if (needUpdate) {

                wrapStatus.currentStatus = currentStatus;
                wrapStatus.lastPutTime = System.currentTimeMillis();
                wrapStatus.needUpdate = true;
            }
            else if(importContext.getDataTranPlugin().isComplete(currentStatus) ){
                wrapStatus.currentStatus = currentStatus;
                wrapStatus.lastPutTime = System.currentTimeMillis();
                wrapStatus.needUpdate = true;
            }
        }
	}
	protected void _flushStatus()  throws Exception{
		if(logger.isDebugEnabled()){
			logger.debug("flushStatus start.");
		}
		List<WrapStatus> datas = new ArrayList<>();
		List<Status> removeDatas = new ArrayList<>();
		Set<Map.Entry<String,WrapStatus>> statusSet = statuses.entrySet();
		WrapStatus wrapStatus = null;
        long interval = 0l;
        long ntime  = System.currentTimeMillis();
		for(Map.Entry<String,WrapStatus> entry : statusSet){
			wrapStatus = entry.getValue();
			if(wrapStatus.needUpdate ) {
                datas.add(wrapStatus);
            }
            else{
                interval = ntime - wrapStatus.lastPutTime;
                if(interval >= liveTime){ //静默超过2天的记录将被从statuses中清除
                    removeDatas.add(wrapStatus.currentStatus);
                }
            }

		}
		if(datas.size() > 0) {
			if(importContext.getJobId() == null) {
				SQLExecutor.executeBatch(statusDbname, updateSQL, datas, 100, new BatchHandler<WrapStatus>() {
					@Override
					public void handler(PreparedStatement stmt, WrapStatus wrapStatus1, int i) throws SQLException {
                        Status record = wrapStatus1.currentStatus;
						stmt.setLong(1, record.getTime());
						stmt.setObject(2, convertLastValue(record.getCurrentLastValueWrapper().getLastValue()));
                        stmt.setObject(3, convertStrLastValue(record.getStrLastValue()));
						stmt.setInt(4, lastValueType);
						stmt.setString(5, record.getFilePath());
						stmt.setString(6, record.getRelativeParentDir());
						stmt.setString(7, record.getFileId());
						stmt.setInt(8, record.getStatus());
						stmt.setString(9, record.getId());
						stmt.setString(10, record.getJobType());
                        wrapStatus1.needUpdate = false;

					}
				});
			}
			else{
				SQLExecutor.executeBatch(statusDbname, updateByJobIdSQL, datas, 100, new BatchHandler<WrapStatus>() {
					@Override
					public void handler(PreparedStatement stmt, WrapStatus wrapStatus1, int i) throws SQLException {
                        Status record = wrapStatus1.currentStatus;
						stmt.setLong(1, record.getTime());
						stmt.setObject(2, convertLastValue(record.getCurrentLastValueWrapper().getLastValue()));
                        stmt.setObject(3, convertStrLastValue(record.getStrLastValue()));
                        stmt.setInt(4, lastValueType);
						stmt.setString(5, record.getFilePath());
						stmt.setString(6, record.getRelativeParentDir());
						stmt.setString(7, record.getFileId());
						stmt.setInt(8, record.getStatus());
						stmt.setString(9, record.getId());
						stmt.setString(10,importContext.getJobId());
						stmt.setString(11,importContext.getJobType());
                        wrapStatus1.needUpdate = false;
					}
				});
			}
			for(Status status: removeDatas){
				statuses.remove(status.getFileId());
			}

		}
		if(logger.isDebugEnabled()){
			logger.debug("flush {} Statuses end .",datas.size());
		}


	}
}
