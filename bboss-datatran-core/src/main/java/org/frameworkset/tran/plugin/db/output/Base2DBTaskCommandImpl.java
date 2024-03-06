package org.frameworkset.tran.plugin.db.output;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.db.input.DBRecord;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class Base2DBTaskCommandImpl extends MultiSQLConf2DBTaskCommandImpl {

	private static final Logger blogger = LoggerFactory.getLogger(Base2DBTaskCommandImpl.class);
	public Base2DBTaskCommandImpl(ImportCount importCount, ImportContext importContext,
                                  List<CommonRecord> datas, int taskNo, String jobNo, String taskInfo,
                                  boolean needBatch, LastValueWrapper lastValue, Status currentStatus,  TaskContext taskContext) {
        super( importCount,  importContext,
                 datas,  taskNo,  jobNo,  taskInfo,
         needBatch,  lastValue,  currentStatus,   taskContext);
		if(dbOutputConfig.optimize()){
			sortData();
		}
	}

	private void sortData(){
		List<DBRecord> _idatas = new ArrayList<DBRecord>();
		List<DBRecord> _udatas = new ArrayList<DBRecord>();
		List<DBRecord> _ddatas = new ArrayList<DBRecord>();
		for(int i = 0; datas != null && i < datas.size(); i ++){
			DBRecord dbRecord = (DBRecord)datas.get(i);
			if(dbRecord.isInsert())
				_idatas.add(dbRecord);
			else if(dbRecord.isUpdate()){
				_udatas.add(dbRecord);
			}
			else {
				_ddatas.add(dbRecord);
			}
		}
		if((_udatas.size() == 0 && _ddatas.size() == 0)
				|| (_idatas.size() == 0 && _ddatas.size() == 0)
				|| (_idatas.size() == 0 && _udatas.size() == 0)){
			return;
		}
		else {
			datas.clear();
			if(_idatas.size() > 0) {
				datas.addAll(_idatas);
			}

			if(_udatas.size() > 0) {
				datas.addAll(_udatas);
			}

			if(_ddatas.size() > 0) {
				datas.addAll(_ddatas);
			}
		}
	}

    protected String getSQL(CommonRecord record){
        if(record.isInsert()) {
            TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(taskContext);
            return insertSqlinfo.getSql();
        }
        else if(record.isUpdate()){
            TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(taskContext);
            return updateSqlinfo.getSql();
        }
        else if(record.isDelete()){
            TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(taskContext);
            return deleteSqlinfo.getSql();
        }
        else{
            throw new DataImportException("record action type must be insert or update or delete record.");
        }
    }

}
