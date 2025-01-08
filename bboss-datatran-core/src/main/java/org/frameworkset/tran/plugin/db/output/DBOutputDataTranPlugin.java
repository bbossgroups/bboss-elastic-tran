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

import com.frameworkset.common.poolman.Param;
import com.frameworkset.util.VariableHandler;
import org.frameworkset.persitent.util.PersistentSQLVariable;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.plugin.OutputPlugin;
import org.frameworkset.tran.plugin.db.BaseDBPlugin;
import org.frameworkset.tran.plugin.db.TranSQLInfo;
import org.frameworkset.tran.record.RecordOutpluginSpecialConfig;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.util.TranUtil;

import java.util.ArrayList;
import java.util.List;

import static org.frameworkset.tran.plugin.db.output.DBOutputConfig.SPECIALCONFIG_RECORDPARAMS_NAME;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 14:35
 * @author biaoping.yin
 * @version 1.0
 */
public class DBOutputDataTranPlugin extends BaseDBPlugin implements OutputPlugin {
	/**
	 * 包含所有启动成功的db数据源
	 */
	private DBOutputConfig dbOutputConfig;
	public DBOutputDataTranPlugin(OutputConfig pluginOutputConfig, ImportContext importContext){
		super(  pluginOutputConfig, importContext);
		dbOutputConfig = (DBOutputConfig) pluginOutputConfig;

	}
    @Override
    public String getJobType(){
        return "DBOutputDataTranPlugin";
    }
	@Override
	public void afterInit(){
        dbOutputConfig.initSQLConf();
	}
	@Override
	public void beforeInit(){

//		super.init(importContext,  targetImportContext);
	}
	protected void initTargetDS2ndOtherDSes(){
		if(dbOutputConfig != null) {
			DBConfig targetDBConfig = dbOutputConfig.getTargetDBConfig();
			if (targetDBConfig != null) {
				DataTranPluginImpl.initDS(dbStartResult,targetDBConfig);
			}
		}
	}
	protected void initDSAndTargetSQLInfo(){
		DBConfig dbConfig = dbOutputConfig.getTargetDBConfig();
		String targetDBName = null;
		if(dbConfig != null) {

			targetDBName = dbConfig.getDbName();

		}
		if(targetDBName == null){
			targetDBName = dbOutputConfig.getTargetDbname();
		}

		TranUtil.initTargetSQLInfo(dbOutputConfig, targetDBName);
	}
//	protected void initDSAndTargetSQLInfo(DBOutPutContext db2DBContext){
//		DBConfig dbConfig = db2DBContext.getTargetDBConfig(null);
//		String targetDBName = null;
//		if(dbConfig != null) {
//			this.initDS(dbConfig);
//			targetDBName = dbConfig.getDbName();
//
//		}
//		else{
//			targetDBName =  db2DBContext.getTargetDBName(null);
//			if(targetDBName == null){
//				targetDBName = importContext.getTargetDBName();
//			}
//		}
//		TranUtil.initTargetSQLInfo(db2DBContext, targetDBName);
//	}

	@Override
	public void init() {
		initTargetDS2ndOtherDSes( );
		initDSAndTargetSQLInfo();
	}

	@Override
	public BaseDataTran createBaseDataTran(TaskContext taskContext, TranResultSet tranResultSet, JobCountDownLatch countDownLatch, Status currentStatus){
		if(countDownLatch == null) {
			DBOutPutDataTran db2DBDataTran = new DBOutPutDataTran(taskContext, tranResultSet, importContext, currentStatus);
			db2DBDataTran.initTran();
			return db2DBDataTran;
		}
		else{
			AsynDBOutPutDataTran asynDBOutPutDataTran = new AsynDBOutPutDataTran(  taskContext,tranResultSet,importContext,   countDownLatch,  currentStatus);
			asynDBOutPutDataTran.initTran();
			return asynDBOutPutDataTran;
		}
	}

    /**
     * 创建内部转换器
     * @param baseDataTran
     * @return
     */
    @Override
    public BaseDataTran createBaseDataTran(BaseDataTran baseDataTran) {
        BaseCommonRecordDataTran baseCommonRecordDataTran = new AsynDBOutPutDataTran(baseDataTran);
        return baseCommonRecordDataTran;
    }
    @Override
    public void buildRecordOutpluginSpecialConfig(CommonRecord dbRecord,Context context){
        RecordOutpluginSpecialConfig recordOutpluginSpecialConfig = context.getRecordSpecialConfigsContext().getRecordOutpluginSpecialConfig(this);
        if(recordOutpluginSpecialConfig != null && !context.isDDL()) {
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
                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked insert,but insert sql not setted. See document to set insert sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
                }
            } else if (context.isUpdate()) {
                if(updateSqlinfo != null) {
                    vars = updateSqlinfo.getVars();
                }
                else{
                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked update,but update sql not setted. See document to set update sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
                }

            } else {
                if(deleteSqlinfo != null) {
                    vars = deleteSqlinfo.getVars();
                }
                else{
                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked delete,but delete sql not setted. See document to set delete sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
                }
            }
            String varName = null;
            List<Param> records = new ArrayList<>();
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

                records.add(param);

            }
            recordOutpluginSpecialConfig.addRecordSpecialConfigOnly(SPECIALCONFIG_RECORDPARAMS_NAME,records);
        }
    }
//    @Override
//    public CommonRecord buildRecord(Context context){
//        DBRecord dbRecord = new DBRecord();
//        super.buildRecord(dbRecord,context);
//        if(!context.isDDL()) {
//            List<VariableHandler.Variable> vars = null;
//            Object temp = null;
//            Param param = null;
//
//
//            TranSQLInfo insertSqlinfo = dbOutputConfig.getTargetSqlInfo(context.getTaskContext(), dbRecord);
//            TranSQLInfo updateSqlinfo = dbOutputConfig.getTargetUpdateSqlInfo(context.getTaskContext(), dbRecord);
//            TranSQLInfo deleteSqlinfo = dbOutputConfig.getTargetDeleteSqlInfo(context.getTaskContext(), dbRecord);
//
//            if (context.isInsert()) {
//                if(insertSqlinfo != null) {
//                    vars = insertSqlinfo.getVars();
//                }
//                else{
//                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked insert,but insert sql not setted. See document to set insert sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
//                }
//            } else if (context.isUpdate()) {
//                if(updateSqlinfo != null) {
//                    vars = updateSqlinfo.getVars();
//                }
//                else{
//                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked update,but update sql not setted. See document to set update sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
//                }
//
//            } else {
//                if(deleteSqlinfo != null) {
//                    vars = deleteSqlinfo.getVars();
//                }
//                else{
//                    throw ImportExceptionUtil.buildDataImportException(importContext,"Record is marked delete,but delete sql not setted. See document to set delete sql：https://esdoc.bbossgroups.com/#/datatran-plugins?id=_21-db%e8%be%93%e5%87%ba%e6%8f%92%e4%bb%b6");
//                }
//            }
//            String varName = null;
//            List<Param> record = new ArrayList<>();
//            for (int i = 0; i < vars.size(); i++) {
//                PersistentSQLVariable var = (PersistentSQLVariable) vars.get(i);
//                varName = var.getVariableName();
//                temp = dbRecord.getData(varName);
//                if (temp == null) {
//                    if (logger.isDebugEnabled())
//                        logger.debug("未指定绑定变量的值：{}", varName);
//                }
//                param = new Param();
//                param.setVariable(var);
//                param.setIndex(var.getPosition() + 1);
//                param.setData(temp);
//                param.setName(varName);
//                param.setMethod(var.getMethod());
//
//                record.add(param);
//
//            }
//            dbRecord.setParams(record);
//        }
//        return dbRecord;
//
//    }

}
