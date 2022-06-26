package org.frameworkset.tran.db.output;
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

import com.frameworkset.common.poolman.*;
import org.frameworkset.persitent.type.BaseTypeMethod;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.db.DBRecord;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
public class Base2DBTaskCommandImpl extends BaseTaskCommand<List<CommonRecord>, String> {
	private DBOutputConfig dbOutputConfig;
	private String taskInfo;
	private boolean needBatch;
	private static final Logger logger = LoggerFactory.getLogger(Base2DBTaskCommandImpl.class);
	public Base2DBTaskCommandImpl(ImportCount importCount, ImportContext importContext,
								  List<CommonRecord> datas, int taskNo, String jobNo, String taskInfo,
								  boolean needBatch, Object lastValue, Status currentStatus, boolean reachEOFClosed, TaskContext taskContext) {
		super(importCount,importContext, datas.size(),  taskNo,  jobNo,lastValue,  currentStatus,reachEOFClosed,   taskContext);
		this.needBatch = needBatch;
		this.importContext = importContext;
		this.datas = datas;
		dbOutputConfig = (DBOutputConfig) importContext.getOutputConfig();
		this.taskInfo = taskInfo;
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
			else if(dbRecord.isUpate()){
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

	public List<CommonRecord> getDatas() {
		return datas;
	}


	private List<CommonRecord> datas;
	private int tryCount;



	public void setDatas(List<CommonRecord> datas) {
		this.datas = datas;
	}

	private void debugDB(String name){
		DBUtil.debugStatus(name);

//		java.util.List<AbandonedTraceExt> traceobjects = DBUtil.getGoodTraceObjects(name);
//		for(int i = 0; traceobjects != null && i < traceobjects.size() ; i ++){
//			AbandonedTraceExt abandonedTraceExt = traceobjects.get(i);
//			if(abandonedTraceExt.getStackInfo() != null)
//				logger.info(abandonedTraceExt.getStackInfo());
//		}
//		logger.info("{}",traceobjects);
	}
	public String execute(){
		String data = null;
		if(this.importContext.getMaxRetry() > 0){
			if(this.tryCount >= this.importContext.getMaxRetry())
				throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
		}
		this.tryCount ++;

		StatementInfo stmtInfo = null;
		PreparedStatement statement = null;
		TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(taskContext);
		TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(taskContext);
		TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(taskContext);
		Connection con_ = null;
		int batchsize = importContext.getStoreBatchSize();
		try {
//			DBConfig targetDB = es2DBContext.getTargetDBConfig(taskContext);
//			if(targetDB == null)
//				targetDB = importContext.getDbConfig();
//		GetCUDResult CUDResult = null;
//			String dbname = targetDB.getDbName();
//			logger.info("DBUtil.getConection(dbname)");
//			debugDB(dbname);
			String dbname = dbOutputConfig.getTargetDBName(taskContext);
			if(dbname == null){
				dbname = dbOutputConfig.getTargetDbname();
			}
			con_ = DBUtil.getConection(dbname);
			stmtInfo = new StatementInfo(dbname,
					null,
					false,
					con_,
					false);
			stmtInfo.init();

			String oldSql = null;

			String sql = null;
			if(batchsize <= 1 || !needBatch) {//如果batchsize被设置为0或者1直接一次性批处理所有记录
				List resources = null;
				for(CommonRecord dbRecord:datas){
					DBRecord record = (DBRecord)dbRecord;
					if(record.isInsert()) {
						sql = insertSqlinfo.getSql();
					}
					else if(record.isUpate()){
						sql = updateSqlinfo.getSql();
					}
					else{
						sql = deleteSqlinfo.getSql();
					}

					if(oldSql == null){

						oldSql = sql;
						statement = stmtInfo
								.prepareStatement(sql);
					}
					else if(!oldSql.equals(sql)){
						try {
							statement.executeBatch();
						}

						finally {
							DBOptionsPreparedDBUtil.releaseResources(resources);
							try {
								statement.close();
							}

							catch (Exception e){

							}
						}
						finishTask();

						oldSql = sql;
						statement = stmtInfo
								.prepareStatement(sql);
					}
					if(dbOutputConfig.getStatementHandler() == null) {
						BaseTypeMethod baseTypeMethod = null;

						for(int i = 0;i < record.size(); i ++)
						{
							Param param = record.get(i);
							baseTypeMethod = param.getMethod();
							if(baseTypeMethod == null){
								stmtInfo.getDbadapter().setObject(statement,null,param.getIndex(), param.getData());
							}
							else{
								if(resources == null){
									resources = new ArrayList();
								}
								baseTypeMethod.action(stmtInfo,param,statement,null,resources);
							}

//							statement.setObject(param.getIndex(),param.getValue());
						}
					}
					else{
						dbOutputConfig.getStatementHandler().handler(statement,record);
					}


					try {
						statement.addBatch();
					}
					catch (SQLException e){
						throw new NestedSQLException(record.toString(),e);
					}
				}
				if(statement != null) {
					try {
						statement.executeBatch();
					}
					finally {
						DBOptionsPreparedDBUtil.releaseResources(resources);
					}
					finishTask();
				}
			}
			else
			{
				List resources = null;
				int point = batchsize - 1;
				int count = 0;
				for(CommonRecord dbRecord:datas) {
					DBRecord record = (DBRecord)dbRecord;
					if(record.isInsert()) {
						sql = insertSqlinfo.getSql();
					}
					else if(record.isUpate()){
						sql = updateSqlinfo.getSql();
					}
					else{
						sql = deleteSqlinfo.getSql();
					}
					if(oldSql == null){

						oldSql = sql;
						statement = stmtInfo
								.prepareStatement(sql);
					}
					else if(!oldSql.equals(sql)){
						if(count > 0) {
							try {
								statement.executeBatch();
							}

							finally {
								DBOptionsPreparedDBUtil.releaseResources(resources);
								try {
									statement.close();
								}

								catch (Exception e){

								}
							}
							finishTask();

						}
						count = 0;
						oldSql = sql;
						statement = stmtInfo
								.prepareStatement(sql);
					}
					if(dbOutputConfig.getStatementHandler() == null) {
						BaseTypeMethod baseTypeMethod = null;

						for (int i = 0; i < record.size(); i++) {
							Param param = record.get(i);
							baseTypeMethod = param.getMethod();
							if(baseTypeMethod == null) {
								stmtInfo.getDbadapter().setObject(statement, null, param.getIndex(), param.getData());
							}
							else{
								if(resources == null){
									resources = new ArrayList();
								}
								baseTypeMethod.action(stmtInfo,param,statement,null,resources);
							}
//							statement.setObject(param.getIndex(), param.getValue());
						}
					}
					else{
						dbOutputConfig.getStatementHandler().handler(statement,record);
					}
					statement.addBatch();
					if ((count > 0 && count % point == 0)) {
						try {
							statement.executeBatch();
						}

						finally {
							DBOptionsPreparedDBUtil.releaseResources(resources);

						}
						finishTask();
						statement.clearBatch();
						count = 0;
						continue;
					}
					count++;
				}
				if(count > 0) {
					try {
						statement.executeBatch();
					}

					finally {
						DBOptionsPreparedDBUtil.releaseResources(resources);

					}
					finishTask();
				}
			}

		}
		catch(BatchUpdateException error)
		{
			if(stmtInfo != null) {
				try {
					stmtInfo.errorHandle(error);
				} catch (SQLException ex) {
					throw new DataImportException(taskInfo,error);
				}
			}
			throw new DataImportException(taskInfo,error);
		}
		catch (Exception e) {
			if(stmtInfo != null) {

				try {
					stmtInfo.errorHandle(e);
				} catch (SQLException ex) {
					throw new DataImportException(taskInfo,e);
				}
			}
			throw new DataImportException(taskInfo,e);

		} finally {
			if(stmtInfo != null)
				stmtInfo.dofinally();
			if(con_ != null){
				try {
					con_.close();
				}
				catch (Exception e){

				}
			}
//			logger.info("stmtInfo.dofinally()");
//			debugDB(importContext.getDbConfig().getDbName());
			stmtInfo = null;


		}
		return data;
	}

	public int getTryCount() {
		return tryCount;
	}


}
