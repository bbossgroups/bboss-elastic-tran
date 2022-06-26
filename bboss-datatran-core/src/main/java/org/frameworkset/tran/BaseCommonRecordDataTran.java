package org.frameworkset.tran;

import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.RecordColumnInfo;
import org.frameworkset.tran.record.SplitKeys;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.annotations.DateFormateMeta;

import java.text.DateFormat;
import java.util.*;

public abstract class BaseCommonRecordDataTran extends BaseDataTran{

	public BaseCommonRecordDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext,   currentStatus);
	}

	protected void logColumnsInfo(){
		if(logger.isDebugEnabled())
			logger.debug("Export Columns is null,you can set Export Columns in importconfig or not.");
//				throw new DataImportException("Export Columns is null,Please set Export Columns in importconfig.");
	}
	public CommonRecord buildRecord(Context context){
		CommonRecord dataRecord = new CommonRecord();
		buildRecord(dataRecord,context);
		return dataRecord;
	}

	protected List<CommonRecord> convertDatas(Object datas){
		if(datas == null)
			return null;
		List<CommonRecord> records = null;
		if(datas instanceof List){
			records = (List<CommonRecord>)datas;
		}
		else{
			records = new ArrayList<>(1);
			records.add((CommonRecord)datas);
		}
		return records;

	}
	/**
	 * 并行批处理导入，ftp上传，不支持并行生成文件

	 * @return
	 */
	@Override
	public String parallelBatchExecute( ) {
		logger.info("parallel batch import data Execute started.");
		return tranJob.parallelBatchExecute(parrelTranCommand,currentStatus,importContext, tranResultSet,this);
	}

	/**
	 * 串行批处理导入

	 * @return
	 */
	@Override
	public String batchExecute(  ){

		logger.info("batch import data Execute started.");
		return tranJob.batchExecute(serialTranCommand,currentStatus,importContext, tranResultSet,this);
	}
	/**
	 * 串行处理导入

	 * @return
	 */
	@Override
	public String serialExecute(){
		logger.info("serial import data Execute started.");
		return tranJob.serialExecute(serialTranCommand,currentStatus,importContext, tranResultSet,this);
	}

	protected CommonRecord buildRecord(CommonRecord dbRecord ,Context context){

		String[] columns = importContext.getExportColumns();
		//如果采用结果集中的字段集，就需要考虑全局添加或者记录级别添加的字段，通过配置设置导出的字段集不需要考虑全局添加或者记录级别添加的字段
		boolean useResultKeys = false;
		//记录切割时，每条记录对应切割出来的字段信息，切割记录
		String[] splitColumns = null;
		if (columns == null){
			Object  keys = tranResultSet.getKeys();
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
			Object  keys = tranResultSet.getKeys();
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
					throw new DataImportException("fieldName["+colName+"]名称映射配置错误：varName="+varName);
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
			temp = tranResultSet.getValue(colName,sqlType);
			RecordColumnInfo recordColumnInfo = null;
			if(temp == null) {
				if(logger.isDebugEnabled())
					logger.debug("未指定[绑定变量：{},数据源列名：{}]的值！",varName,colName);
			}
			else if (temp instanceof Date){
				DateFormat dateFormat = null;
				if(fieldMeta != null){
					DateFormateMeta dateFormateMeta = fieldMeta.getDateFormateMeta();
					if(dateFormateMeta != null){
						dateFormat = dateFormateMeta.toDateFormat();
					}
				}
				if(dateFormat == null)
					dateFormat = context.getDateFormat();
				recordColumnInfo = new RecordColumnInfo();
				recordColumnInfo.setDateTag(true);
				recordColumnInfo.setDateFormat(dateFormat);
			}
			dbRecord.addData(varName,temp,recordColumnInfo);


		}

		if(splitColumns !=  null && splitColumns.length > 0){
			handleIgnoreFieldsAgain();
		}
		return dbRecord;
	}

	protected void handleIgnoreFieldsAgain(){

	}



}
