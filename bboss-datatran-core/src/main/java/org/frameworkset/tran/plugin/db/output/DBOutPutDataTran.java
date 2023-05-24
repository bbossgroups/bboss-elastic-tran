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

		List<VariableHandler.Variable> vars = null;
		Object temp = null;
		Param param = null;


		TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(context.getTaskContext());
		TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(context.getTaskContext());
		TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(context.getTaskContext());

		if(context.isInsert()) {

			vars = insertSqlinfo.getVars();
		}
		else if(context.isUpdate()) {
			vars = updateSqlinfo.getVars();
		}
		else {
			vars = deleteSqlinfo.getVars();
		}
		super.buildRecord(dbRecord,context);
		String varName = null;
		List<Param> record = new ArrayList<>();
		for(int i = 0;i < vars.size(); i ++)
		{
			PersistentSQLVariable var = (PersistentSQLVariable)vars.get(i);
			varName = var.getVariableName();
			temp = dbRecord.getData(varName);
			if(temp == null) {
				if(logger.isDebugEnabled())
					logger.debug("未指定绑定变量的值：{}",varName);
			}
			param = new Param();
			param.setVariable(var);
			param.setIndex(var.getPosition()  +1);
			param.setData(temp);
			param.setName(varName);
			param.setMethod(var.getMethod());

			record.add(param);

		}
		dbRecord.setParams(record);
		return dbRecord;

	}

	@Override
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed,
										  CommonRecord record,ExecutorService service, List<Future> tasks, TranErrorWrapper tranErrorWrapper) {
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
					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskNo;
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed){
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

					TaskCall.call(taskCommand);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				return action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
			}

			@Override
			public int endSerialActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
				taskNo = action(totalCount, dataSize, taskNo, lastValue, datas, reachEOFClosed);
				return taskNo;

			}


		};
	}

	@Override
	protected void initTranJob(){
		tranJob = new CommonRecordTranJob();
	}



}
