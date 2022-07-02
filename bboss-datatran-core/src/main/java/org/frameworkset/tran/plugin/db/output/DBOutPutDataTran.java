package org.frameworkset.tran.plugin.db.output;

import com.frameworkset.common.poolman.Param;
import com.frameworkset.util.VariableHandler;
import org.frameworkset.persitent.util.PersistentSQLVariable;
import org.frameworkset.tran.*;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.db.input.DBRecord;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.task.BaseParrelTranCommand;
import org.frameworkset.tran.task.BaseSerialTranCommand;
import org.frameworkset.tran.task.CommonRecordTranJob;
import org.frameworkset.tran.task.TaskCall;

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
			builder.append("Import data to db[").append(dbConfig.getDbUrl())
					.append("] dbuser[").append(dbConfig.getDbUser()).append("]");
		}
		else{
			String targetDBName = dbOutputConfig.getTargetDBName(taskContext);
			if(targetDBName == null){
				targetDBName = dbOutputConfig.getTargetDbname();
			}
			builder.append("Import data to db[").append(targetDBName)
					.append("]");
		}



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
		}
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
			dbRecord.setAction(DBRecord.INSERT);
			vars = insertSqlinfo.getVars();
		}
		else if(context.isUpdate()) {
			dbRecord.setAction(DBRecord.UPDATE);
			vars = updateSqlinfo.getVars();
		}
		else {
			dbRecord.setAction(DBRecord.DELETE);
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
		/**
		Object  keys = jdbcResultSet.getKeys();
		String[] splitColumns = null;
		if(keys != null) {
			boolean isSplitKeys = keys instanceof SplitKeys;
			if(isSplitKeys) {
				SplitKeys splitKeys = (SplitKeys) keys;

				splitColumns = splitKeys.getSplitKeys();
			}

		}
		List<Param> record = new ArrayList<Param>();
		Map<String,Object> addedFields = new HashMap<String,Object>();
		//context优先级高于splitColumns,splitColumns高于全局配置，全局配置高于数据源级别字段值
		List<FieldMeta> fieldValueMetas = context.getFieldValues();

		appendFieldValues( record, vars,    fieldValueMetas,  addedFields);
		//计算记录切割字段
		appendSplitFieldValues(record, vars,
				splitColumns,
				addedFields,context);
		fieldValueMetas = context.getESJDBCFieldValues();
		appendFieldValues(  record, vars,   fieldValueMetas,  addedFields);
		String varName = null;
		for(int i = 0;i < vars.size(); i ++)
		{
			VariableHandler.Variable var = vars.get(i);

			varName = var.getVariableName();
			FieldMeta fieldMeta = context.getMappingName(varName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				varName = fieldMeta.getTargetFieldName();
			}
			if(addedFields.get(varName) != null)
				continue;
			temp = jdbcResultSet.getValue(varName);
			if(temp == null) {
				if(logger.isWarnEnabled())
					logger.warn("未指定绑定变量的值：{}",var.getVariableName());
			}
			param = new Param();
			param.setVariable(var);
			param.setIndex(var.getPosition()  +1);
			param.setValue(temp);
			param.setName(var.getVariableName());
			record.add(param);

		}

		dbRecord.setParams(record);
		return dbRecord;*/
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
					Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl( totalCount, importContext, records,
							taskNo, totalCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed,taskContext);
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
					Base2DBTaskCommandImpl taskCommand = new Base2DBTaskCommandImpl(totalCount, importContext, records,
							taskNo, totalCount.getJobNo(),taskInfo,false,lastValue,  currentStatus,reachEOFClosed,taskContext);

					TaskCall.call(taskCommand);
//						importContext.flushLastValue(lastValue);

				}
				return taskNo;
			}
			@Override
			public int hanBatchActionTask(ImportCount totalCount, long dataSize, int taskNo, Object lastValue, Object datas, boolean reachEOFClosed, CommonRecord record) {
//				List<CommonRecord> records = convertDatas( datas);
//				if(records != null && records.size() > 0)  {
//					ExcelFileFtpTaskCommandImpl taskCommand = new ExcelFileFtpTaskCommandImpl(totalCount, importContext,targetImportContext,
//							dataSize, taskNo, totalCount.getJobNo(), (ExcelFileTransfer) fileTransfer,lastValue,  currentStatus,reachEOFClosed,taskContext);
//					taskCommand.setDatas(records);
//					TaskCall.call(taskCommand);
//					taskNo++;
//				}
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
