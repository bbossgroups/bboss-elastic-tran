package org.frameworkset.tran.plugin;
/**
 * Copyright 2022 bboss
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

import com.frameworkset.orm.annotation.BatchContext;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.*;
import org.frameworkset.tran.config.OutputConfig;
import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ContextImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.RecordSpecialConfigsContext;
import org.frameworkset.tran.exception.ImportExceptionUtil;
import org.frameworkset.tran.metrics.JobTaskMetrics;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.SplitKeys;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/6/19
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BasePlugin {

    private static Logger logger = LoggerFactory.getLogger(BasePlugin.class);
	protected DataTranPlugin dataTranPlugin;
	protected ImportContext importContext;
    protected OutputConfig pluginOutputConfig;
	/**
	 * 通知输入插件停止采集数据
	 */
	private volatile boolean stopCollectData;
	public BasePlugin(OutputConfig pluginOutputConfig,ImportContext importContext){
		this.importContext = importContext;
        this.pluginOutputConfig = pluginOutputConfig;
        pluginOutputConfig.setOutputPlugin((OutputPlugin) this);
	}

    public BasePlugin(ImportContext importContext){
        this.importContext = importContext;
    }

    public OutputConfig getOutputConfig(){
        return pluginOutputConfig;
    }
	public abstract void afterInit();
	public abstract void beforeInit();
	public abstract void init();
	public boolean isMultiTran(){
		return false;
	}
	/**
	 * 通知输入插件停止采集数据
	 */
	public void stopCollectData(){
		stopCollectData = true;
	}
	public boolean isStopCollectData(){
		return stopCollectData;
	}
	public ImportContext getImportContext() {
		return importContext;
	}

	public Object formatLastDateValue(Date date){
		return date;
	}
    public Object formatLastLocalDateTimeValue(LocalDateTime localDateTime){
        return localDateTime;
    }

	public void setDataTranPlugin(DataTranPlugin dataTranPlugin) {
		this.dataTranPlugin = dataTranPlugin;
	}

	public boolean isEnablePluginTaskIntercept() {
		return true;
	}

	public String getLastValueVarName() {
		return importContext.getLastValueColumn();
	}
	public boolean isEnableAutoPauseScheduled(){
		return true;
	}
	public Context buildContext(TaskContext taskContext,  Record record, BatchContext batchContext){
		return new ContextImpl(  taskContext,importContext,    record,batchContext);
	}
	public Long getTimeRangeLastValue(){
		return null;
	}
	public JobTaskMetrics createJobTaskMetrics(){
//		return getOutputPlugin().createJobTaskMetrics();
		return new JobTaskMetrics();
	}

    ///////输出插件公共方法
    protected static Object dummy = new Object();
    private void logColumnsInfo(){
        if(logger.isDebugEnabled())
            logger.debug("Export Columns is null,you can set Export Columns in importconfig or not.");
//				throw new DataImportException("Export Columns is null,Please set Export Columns in importconfig.");
    }
    protected CommonRecord buildRecord(CommonRecord dbRecord , Context context){
        dbRecord.setAction(context.getAction());
        dbRecord.setTempDatas(context.getTempDatas());
        dbRecord.setMetaDatas(context.getMetaDatas());
        dbRecord.setTableMapping(context.getTableMapping());
        dbRecord.setRecordKeyField(context.getRecordKeyField());
        dbRecord.setRecordKey(context.getMessageKey());
        dbRecord.setKeys(context.getKeys());
        ImportContext importContext = context.getImportContext();
        String[] columns = importContext.getExportColumns();
        //如果采用结果集中的字段集，就需要考虑全局添加或者记录级别添加的字段，通过配置设置导出的字段集不需要考虑全局添加或者记录级别添加的字段
        boolean useResultKeys = false;
        //记录切割时，每条记录对应切割出来的字段信息，切割记录
        String[] splitColumns = null;
        if (columns == null){
            Object  keys = context.getKeys();
            if(keys != null) {
                boolean isSplitKeys = keys instanceof SplitKeys;
                if(!isSplitKeys) {
                    useResultKeys = true;
                    if (keys instanceof Set) {
                        Set<String> _keys = (Set<String>) keys;
                        columns = _keys.toArray(new String[_keys.size()]);
                    } else {
                        columns = (String[]) keys;
                    }
                }
                else{
                    SplitKeys splitKeys = (SplitKeys)keys;
                    if(splitKeys.getBaseKeys() != null){
                        Object baseKeys = splitKeys.getBaseKeys();
                        useResultKeys = true;
                        if (baseKeys instanceof Set) {
                            Set<String> _keys = (Set<String>) baseKeys;
                            columns = _keys.toArray(new String[_keys.size()]);
                        } else {
                            columns = (String[]) baseKeys;
                        }
                    }
                    splitColumns = splitKeys.getSplitKeys();

                }
            }
            else{
                logColumnsInfo();
            }
        }
        else{
            Object  keys = context.getKeys();
            if(keys != null) {
                boolean isSplitKeys = keys instanceof SplitKeys;
                if(isSplitKeys) {
                    SplitKeys splitKeys = (SplitKeys) keys;

                    splitColumns = splitKeys.getSplitKeys();
                }

            }
        }
        TranMeta metaData = context.getMetaData();
        Boolean useJavaName = context.getUseJavaName();

        Boolean useLowcase = context.getUseLowcase();


        if(useJavaName == null) {
            useJavaName = false;
        }
        if(useLowcase == null)
        {
            useLowcase = false;
        }
        Object temp = null;


        Map<String,Object> addedFields = new HashMap<String,Object>();
        //计算记录级别字段配置值
        List<FieldMeta> fieldValueMetas = context.getFieldValues();//context优先级高于splitColumns,splitColumns高于全局配置，全局配置高于数据源级别字段值

        appendFieldValues( dbRecord, columns,    fieldValueMetas,  addedFields, useResultKeys,context);
        //计算记录切割字段，如果存在忽略字段，则需要通过补偿方法，对数据进行忽略处理
        appendSplitFieldValues(dbRecord,
                splitColumns,
                addedFields,context);
        //计算全局级别字段配置值
        fieldValueMetas = context.getGlobalFieldValues();//全局配置
        appendFieldValues(  dbRecord, columns,   fieldValueMetas,  addedFields,  useResultKeys,context);
        //计算数据源级别字段值
        String varName = null;
        String colName = null;
//		String splitFieldName = context.getImportContext().getSplitFieldName();
        for(int i = 0;columns !=null && i < columns.length; i ++)
        {
            colName = columns[i];
            /** 不能忽略，记录可能被切割，也可能不会被切割，切割后的记录也会包含切割字段同样的名称
             if(splitFieldName != null && !splitFieldName.equals("") && splitFieldName.equals(colName)){ //忽略切割字段
             continue;
             }
             */

//			if(addedFields.get(colName) != null)
//				continue;
            //变量名称处理
            FieldMeta fieldMeta = context.getMappingName(colName);
            if(fieldMeta != null) {
                if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
                    continue;
                varName = fieldMeta.getTargetFieldName();
                if(varName == null || varName.equals(""))
                    throw ImportExceptionUtil.buildDataImportException(importContext,"fieldName["+colName+"]名称映射配置错误：varName="+varName);
            }
            else if(useResultKeys && metaData != null && metaData.getColumnCount() > 0 ){
                if(useJavaName) {
                    varName = metaData.getColumnJavaNameByIndex(i);
                }
                else{
                    varName =  !useLowcase ?colName:metaData.getColumnLabelLowerByIndex(i);
                }
            }
            else{
                varName = colName;
            }
            if(addedFields.get(varName) != null)
                continue;
            int sqlType = useResultKeys && metaData != null?metaData.getColumnTypeByIndex(i):-1;
            temp = context.getValue(colName,sqlType);
            RecordColumnInfo recordColumnInfo = null;
            if(temp == null) {
                if(logger.isDebugEnabled())
                    logger.debug("字段值[目标列{},源列{}]的值为null！",varName,colName);
            }
            else if (temp instanceof Date){
//                recordColumnInfo = resolveRecordColumnInfo(  temp,  fieldMeta,  context);
                context.resolveRecordColumnInfo( varName, temp,  fieldMeta);
            }
            dbRecord.addData(varName,temp);


        }

        if(splitColumns !=  null && splitColumns.length > 0){
            handleIgnoreFieldsAgain();
        }
        return dbRecord;
    }
    private static void handleIgnoreFieldsAgain(){

    }

    public RecordColumnInfo resolveRecordColumnInfo(Object value,FieldMeta fieldMeta,Context context){
        return null;
    }

    private void appendSplitFieldValues(CommonRecord record,
                                        String[] splitColumns,
                                        Map<String, Object> addedFields, Context context) {
        if(splitColumns ==  null || splitColumns.length == 0){
            return;
        }

        String varName = null;
        for (String fieldName : splitColumns) {
            FieldMeta fieldMeta = context.getMappingName(fieldName);//获取字段映射或者忽略配置

            if(fieldMeta != null) {
                if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)//忽略字段
                    continue;
                varName = fieldMeta.getTargetFieldName();//获取映射字段
                if(varName == null || varName.equals(""))
                    throw ImportExceptionUtil.buildDataImportException(context.getImportContext(),"fieldName["+fieldName+"]名称映射配置错误：varName="+varName);
            }
            else{
                varName = fieldName;
            }
            if (addedFields.containsKey(varName))
                continue;
            addRecordValue( record, varName, context.getCurrentRecord().getValue(fieldName),fieldMeta ,context);
            addedFields.put(varName, dummy);

        }

    }
    private void addRecordValue(CommonRecord record,String fieldName,Object value,FieldMeta fieldMeta,Context context){
//        RecordColumnInfo recordColumnInfo = resolveRecordColumnInfo(  value,  fieldMeta,  context);
        context.resolveRecordColumnInfo( fieldName, value,  fieldMeta);
        record.addData(fieldName, value);
    }
    private void appendFieldValues(CommonRecord record,
                                   String[] columns ,
                                   List<FieldMeta> fieldValueMetas,
                                   Map<String, Object> addedFields, boolean useResultKeys,Context context) {
        if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
            return;
        }

        if(columns != null && columns.length > 0) {
            for (FieldMeta fieldMeta : fieldValueMetas) {
                String fieldName = fieldMeta.getTargetFieldName();
                if (addedFields.containsKey(fieldName))
                    continue;
                boolean matched = false;
                for (String name : columns) {
                    if (name.equals(fieldName)) {
                        addRecordValue( record,name, fieldMeta.getValue(), fieldMeta, context);
//						record.addData(name, fieldMeta.getValue());
                        addedFields.put(name, dummy);
                        matched = true;
                        break;
                    }
                }
                if (useResultKeys && !matched) {
                    addRecordValue( record,fieldName, fieldMeta.getValue(), fieldMeta, context);
//					record.addData(fieldName, fieldMeta.getValue());
                    addedFields.put(fieldName, dummy);
                }
            }
        }
        else{ //hbase之类的数据同步工具，数据都是在datarefactor接口中封装处理，columns信息不存在，直接用fieldValueMetas即可
            for (FieldMeta fieldMeta : fieldValueMetas) {
                String fieldName = fieldMeta.getTargetFieldName();
                if (addedFields.containsKey(fieldName))
                    continue;
                addRecordValue( record,fieldName, fieldMeta.getValue(), fieldMeta, context);
//				record.addData(fieldName, fieldMeta.getValue());
                addedFields.put(fieldName, dummy);

            }
        }
    }
    public void buildRecordOutpluginSpecialConfig(CommonRecord dataRecord,Context context) throws Exception {
    }

    protected void buildRecordOutpluginSpecialConfigs(CommonRecord dataRecord, Context context) throws Exception {
        buildRecordOutpluginSpecialConfig(  dataRecord,  context);
        
    }
    public CommonRecord buildRecord(Context context) throws Exception{
        CommonRecord dataRecord = new CommonRecord();
       
        buildRecord(dataRecord,context);
        buildRecordOutpluginSpecialConfigs( dataRecord,  context);
        RecordSpecialConfigsContext recordSpecialConfigsContext = context.getRecordSpecialConfigsContext();
        dataRecord.setRecordOutpluginSpecialConfigs(recordSpecialConfigsContext.getRecordOutpluginSpecialConfigs());

        dataRecord.setRecordOutpluginSpecialConfig(recordSpecialConfigsContext.getRecordOutpluginSpecialConfig());
        return dataRecord;
    }
}
