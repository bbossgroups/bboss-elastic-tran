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

import com.frameworkset.common.poolman.*;
import org.frameworkset.persitent.type.BaseTypeMethod;
import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.db.input.DBRecord;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.BaseTaskCommand;
import org.frameworkset.tran.task.TaskFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * <p>Description: import datas to database task command</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/3/1 11:32
 * @author biaoping.yin
 * @version 1.0
 */
public class MultiSQLConf2DBTaskCommandImpl extends BaseTaskCommand<List<CommonRecord>, String> {
    protected DBOutputConfig dbOutputConfig;
    protected String taskInfo;
    protected boolean needBatch;
	private static final Logger logger = LoggerFactory.getLogger(MultiSQLConf2DBTaskCommandImpl.class);
	public MultiSQLConf2DBTaskCommandImpl(ImportCount importCount, ImportContext importContext,
                                          List<CommonRecord> datas, int taskNo, String jobNo, String taskInfo,
                                          boolean needBatch, LastValueWrapper lastValue, Status currentStatus, boolean reachEOFClosed, TaskContext taskContext) {
		super(importCount,importContext, datas.size(),  taskNo,  jobNo,lastValue,  currentStatus,reachEOFClosed,   taskContext);
		this.needBatch = needBatch;
		this.importContext = importContext;
		this.datas = datas;
		dbOutputConfig = (DBOutputConfig) importContext.getOutputConfig();
		this.taskInfo = taskInfo;

	}


	public List<CommonRecord> getDatas() {
		return datas;
	}


	protected List<CommonRecord> datas;
    protected int tryCount;



	public void setDatas(List<CommonRecord> datas) {
		this.datas = datas;
	}

	private void debugDB(String name){
		DBUtil.debugStatus(name);


	}

    protected String getSQL(CommonRecord record){
        if(record.isInsert()) {
            TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(taskContext,record);
            return insertSqlinfo.getSql();
        }
        else if(record.isUpdate()){
            TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(taskContext,record);
            return updateSqlinfo.getSql();
        }
        else if(record.isDelete()){
            TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(taskContext,record);
            return deleteSqlinfo.getSql();
        }
        else{
            throw new DataImportException("record action type must be insert or update or delete record.");
        }
    }


    protected TranSQLInfo getTranSQLInfo(CommonRecord record){
        if(record.isInsert()) {
            TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(taskContext,record);
            return insertSqlinfo;
        }
        else if(record.isUpdate()){
            TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(taskContext,record);
            return updateSqlinfo;
        }
        else if(record.isDelete()){
            TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(taskContext,record);
            return deleteSqlinfo;
        }
        else{
            throw new DataImportException("record action type must be insert or update or delete record.");
        }
    }
	public String execute(){
		if(!dbOutputConfig.isMultiSQLConfTargetDBName()){
            return singleTargetDBExecute();
        }
        else{
            return multiTargetDBExecute();
        }
	}

    private String singleTargetDBExecute() {
        if(this.importContext.getMaxRetry() > 0){
            if(this.tryCount >= this.importContext.getMaxRetry())
                throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
        }
        this.tryCount ++;
        String dbname = dbOutputConfig.getTargetDBName(taskContext);
        if(dbname == null){
            dbname = dbOutputConfig.getTargetDbname();
        }
        return _execute( datas, dbname);
    }
    private String _execute(List<CommonRecord> datas,String dbname){
        String data = null;


        StatementInfo stmtInfo = null;
        PreparedStatement statement = null;

        Connection con_ = null;
        int batchsize = importContext.getStoreBatchSize();
        try {

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
                    sql = getSQL( record);

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
                    sql = getSQL( record);

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
            stmtInfo = null;


        }
        return data;
    }
    private Map<String,List<CommonRecord>> dbRecords;
    private void splitRecords(){
        if(dbRecords != null)
            return ;
        String dbname = dbOutputConfig.getTargetDBName(taskContext);
        if(dbname == null){
            dbname = dbOutputConfig.getTargetDbname();
        }
        Map<String,List<CommonRecord>> dbRecords = new LinkedHashMap<>();
        for(CommonRecord dbRecord:datas){
            TranSQLInfo tranSQLInfo =getTranSQLInfo(dbRecord);
            String tmpDBName = null;
            if(tranSQLInfo.getTargetDBName() != null){
                tmpDBName = tranSQLInfo.getTargetDBName();

            }
            else{
                tmpDBName = dbname;

            }
            List<CommonRecord> temp = dbRecords.get(tmpDBName);
            if(temp == null){
                temp = new ArrayList<>();
                dbRecords.put(tmpDBName,temp);
            }
            temp.add(dbRecord);

        }
        this.dbRecords = dbRecords;

    }
    private String multiTargetDBExecute(){
        splitRecords();
        String data = null;
        if(this.importContext.getMaxRetry() > 0){
            if(this.tryCount >= this.importContext.getMaxRetry())
                throw new TaskFailedException("task execute failed:reached max retry times "+this.importContext.getMaxRetry());
        }
        this.tryCount ++;
        Iterator<Map.Entry<String, List<CommonRecord>>> iterator = dbRecords.entrySet().iterator();
        while (iterator.hasNext()){
            Map.Entry<String, List<CommonRecord>> entry = iterator.next();
            this._execute(entry.getValue(),entry.getKey());
        }
        return data;
    }

	public int getTryCount() {
		return tryCount;
	}


}
