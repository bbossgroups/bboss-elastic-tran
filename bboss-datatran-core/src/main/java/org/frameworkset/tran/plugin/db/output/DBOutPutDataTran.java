package org.frameworkset.tran.plugin.db.output;

import com.frameworkset.common.poolman.Param;
import com.frameworkset.util.VariableHandler;
import org.frameworkset.persitent.util.PersistentSQLVariable;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.plugin.db.input.DBRecord;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;
import org.frameworkset.tran.task.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class DBOutPutDataTran extends BaseCommonRecordDataTran {
	protected DBOutputConfig dbOutputConfig ;


	public void init(){
		super.init();
		dbOutputConfig = (DBOutputConfig) importContext.getOutputConfig();
		StringBuilder builder = new StringBuilder();
		DBConfig dbConfig = dbOutputConfig.getTargetDBConfig(taskContext) ;
		if(dbConfig == null)
			dbConfig = dbOutputConfig.getTargetDBConfig();
		if(dbConfig != null){
			builder.append("Import data to db[").append(dbConfig.getDbName())
					.append("]");
		}
		else{
			String targetDBName = dbOutputConfig.getTargetDBName(taskContext);
			if(targetDBName == null){
				targetDBName = dbOutputConfig.getTargetDbname();
			}
			builder.append("Import data to db[").append(targetDBName)
					.append("]");
		}


/**
		if(dbOutputConfig.getTargetSqlInfo(taskContext) != null ) {
			builder.append(" insert sql[").append( dbOutputConfig.getTargetSqlInfo(taskContext).getOriginSQL()).append("]");
		}
		if(dbOutputConfig.getTargetUpdateSqlInfo(taskContext) != null ) {
			builder.append("\r\nupdate sql[")
					.append(dbOutputConfig.getTargetUpdateSqlInfo(taskContext).getOriginSQL()).append("]");
		}
		if(dbOutputConfig.getTargetDeleteSqlInfo(taskContext) != null ) {
			builder.append("\r\ndelete sql[")
					.append(dbOutputConfig.getTargetDeleteSqlInfo(taskContext).getOriginSQL()).append("]");
		}*/
		taskInfo = builder.toString();
	}


	public DBOutPutDataTran(TaskContext taskContext,TranResultSet jdbcResultSet, ImportContext importContext,Status currentStatus) {
		super(   taskContext,jdbcResultSet,importContext,   currentStatus);
	}


	@Override
	public CommonRecord buildRecord(Context context){
		DBRecord dbRecord = new DBRecord();


		super.buildRecord(dbRecord,context);
        if(!context.isDDL()) {
            List<VariableHandler.Variable> vars = null;
            Object temp = null;
            Param param = null;


            TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(context.getTaskContext(), dbRecord);
            TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(context.getTaskContext(), dbRecord);
            TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(context.getTaskContext(), dbRecord);

            if (context.isInsert()) {
				if(insertSqlinfo != null) {
					vars = insertSqlinfo.getVars();
				}
				else{
					throw new DataImportException("Record is marked insert,but insert sql not setted. See document to set insert sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
				}
            } else if (context.isUpdate()) {
				if(updateSqlinfo != null) {
					vars = updateSqlinfo.getVars();
				}
				else{
					throw new DataImportException("Record is marked update,but update sql not setted. See document to set update sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
				}

            } else {
				if(deleteSqlinfo != null) {
					vars = deleteSqlinfo.getVars();
				}
				else{
					throw new DataImportException("Record is marked delete,but delete sql not setted. See document to set delete sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
				}
            }
            String varName = null;
            List<Param> record = new ArrayList<>();
            for (int i = 0; i < vars.size(); i++) {
                PersistentSQLVariable var = (PersistentSQLVariable) vars.get(i);
                varName = var.getVariableName();
                temp = dbRecord.getData(varName);
                if (temp == null) {
                    if (logger.isDebugEnabled())
                        logger.debug("未指定绑定变量的值：{}", varName);
                }
                param = new Param();
                param.setVariable(var);
                param.setIndex(var.getPosition() + 1);
                param.setData(temp);
                param.setName(varName);
                param.setMethod(var.getMethod());

                record.add(param);

            }
            dbRecord.setParams(record);
        }
		return dbRecord;

	}

	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper,boolean forceFlush) {
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
                    TaskCommand taskCommand = null;
                    if(!dbOutputConfig.isMultiSQLConf()) {
                        taskCommand = new Base2DBTaskCommandImpl(totalCount, importContext, records,
                                taskNo, taskContext.getJobNo(), taskInfo, false, lastValue, currentStatus, reachEOFClosed, taskContext);
                    }
                    else{
                        taskCommand = new MultiSQLConf2DBTaskCommandImpl(totalCount, importContext, records,
                                taskNo, taskContext.getJobNo(), taskInfo, false, lastValue, currentStatus, reachEOFClosed, taskContext);
                    }
                    taskCommand.setForceFlush(forceFlush);
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed,boolean forceFlush){
				List<CommonRecord> records = convertDatas( datas);
				if(records != null && records.size() > 0)  {
					taskNo++;
                    TaskCommand taskCommand = null;
                    if(!dbOutputConfig.isMultiSQLConf()) {
                        taskCommand = new Base2DBTaskCommandImpl(totalCount, importContext, records,
                                taskNo, taskContext.getJobNo(), taskInfo, false, lastValue, currentStatus, reachEOFClosed, taskContext);
                    }
                    else{
                        taskCommand = new MultiSQLConf2DBTaskCommandImpl(totalCount, importContext, records,
                                taskNo, taskContext.getJobNo(), taskInfo, false, lastValue, currentStatus, reachEOFClosed, taskContext);
                    }
                    taskCommand.setForceFlush(forceFlush);
					TaskCall.call(taskCommand);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record,boolean forceFlush) {
				return action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed,  forceFlush);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, LastValueWrapper lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed,false);
				return taskNo;

			}


		};
	}

	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}



}
