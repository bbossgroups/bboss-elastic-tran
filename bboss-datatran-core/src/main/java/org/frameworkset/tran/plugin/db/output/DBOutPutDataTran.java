package org.frameworkset.tran.plugin.db.output;

import org.frameworkset.tran.BaseCommonRecordDataTran;
import org.frameworkset.tran.DBConfig;
import org.frameworkset.tran.TranResultSet;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.*;

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
	protected void initTranTaskCommand(){
		parrelTranCommand = new BaseParrelTranCommand(){

			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
//				List<CommonRecord> records = convertDatas( datas);
				if(taskCommandContext.containData())  {
					taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
                    TaskCommand taskCommand = null;
                    if(!dbOutputConfig.isMultiSQLConf()) {
                        taskCommand = new Base2DBTaskCommandImpl(taskCommandContext, false);
                    }
                    else{
                        taskCommand = new MultiSQLConf2DBTaskCommandImpl(taskCommandContext, false);
                    }
                    taskCommandContext.addTask(taskCommand);
//					tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

				}
				return taskCommandContext.getTaskNo();
			}


		};
		serialTranCommand = new BaseSerialTranCommand() {
			private int action(TaskCommandContext taskCommandContext){
//				List<CommonRecord> records = convertDatas( datas);
				if(taskCommandContext.containData())  {
					taskCommandContext.increamentTaskNo();
                    initTaskCommandContext(taskCommandContext);
                    TaskCommand taskCommand = null;
                    if(!dbOutputConfig.isMultiSQLConf()) {
                        taskCommand = new Base2DBTaskCommandImpl(taskCommandContext, false);
                    }
                    else{
                        taskCommand = new MultiSQLConf2DBTaskCommandImpl(taskCommandContext, false);
                    }
					TaskCall.call(taskCommand);

				}
				return taskCommandContext.getTaskNo();
			}
			@Override
			public int hanBatchActionTask(TaskCommandContext taskCommandContext) {
				return action(  taskCommandContext);
			}

			@Override
			public int endSerialActionTask(TaskCommandContext taskCommandContext) {
				return action(  taskCommandContext);

			}


		};
	}

//	@Override
//	protected void initTranJob(){
//		tranJob = new CommonRecordTranJob();
//	}



}
