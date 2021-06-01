package org.frameworkset.tran;

import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class BaseCommonRecordDataTran extends BaseDataTran{

	public BaseCommonRecordDataTran(TaskContext taskContext, TranResultSet jdbcResultSet, ImportContext importContext, ImportContext targetImportContext, Status currentStatus) {
		super(taskContext, jdbcResultSet, importContext, targetImportContext,  currentStatus);
	}

	protected void logColumnsInfo(){
		if(logger.isDebugEnabled())
			logger.debug("Export Columns is null,you can set Export Columns in importconfig or not.");
//				throw new DataImportException("Export Columns is null,Please set Export Columns in importconfig.");
	}
	protected CommonRecord buildRecord(Context context){
		String[] columns = targetImportContext.getExportColumns();
		//如果采用结果集中的字段集，就需要考虑全局添加或者记录级别添加的字段，通过配置设置导出的字段集不需要考虑全局添加或者记录级别添加的字段
		boolean useResultKeys = false;
		if (columns == null){
			Object  keys = jdbcResultSet.getKeys();
			if(keys != null) {
				useResultKeys = true;
				if(keys instanceof Set) {
					Set<String> _keys = (Set<String>) keys;
					columns = _keys.toArray(new String[_keys.size()]);
				}
				else{
					columns = (String[])keys;
				}
			}
			else{
				logColumnsInfo();
			}
		}
		Object temp = null;
		CommonRecord dbRecord = new CommonRecord();

		Map<String,Object> addedFields = new HashMap<String,Object>();
		//计算记录级别字段配置值
		List<FieldMeta> fieldValueMetas = context.getFieldValues();//context优先级高于全局配置，全局配置高于数据源级别字段值

		appendFieldValues( dbRecord, columns,    fieldValueMetas,  addedFields, useResultKeys);
		//计算全局级别字段配置值
		fieldValueMetas = context.getESJDBCFieldValues();//全局配置
		appendFieldValues(  dbRecord, columns,   fieldValueMetas,  addedFields,  useResultKeys);
		//计算数据源级别字段值
		String varName = null;
		for(int i = 0;columns !=null && i < columns.length; i ++)
		{
			varName = columns[i];
			if(addedFields.get(varName) != null)
				continue;
			FieldMeta fieldMeta = context.getMappingName(varName);
			if(fieldMeta != null) {
				if(fieldMeta.getIgnore() != null && fieldMeta.getIgnore() == true)
					continue;
				varName = fieldMeta.getEsFieldName();
			}
			temp = jdbcResultSet.getValue(varName);
			if(temp == null) {
				if(logger.isWarnEnabled())
					logger.warn("未指定绑定变量{}的值！",varName);
			}
			dbRecord.addData(varName,temp);


		}


		return dbRecord;
	}
	protected void appendFieldValues(CommonRecord record,
								   String[] columns ,
								   List<FieldMeta> fieldValueMetas,
								   Map<String, Object> addedFields, boolean useResultKeys) {
		if(fieldValueMetas ==  null || fieldValueMetas.size() == 0){
			return;
		}

		if(columns != null && columns.length > 0) {
			for (FieldMeta fieldMeta : fieldValueMetas) {
				String fieldName = fieldMeta.getEsFieldName();
				if (addedFields.containsKey(fieldName))
					continue;
				boolean matched = false;
				for (String name : columns) {
					if (name.equals(fieldName)) {
						record.addData(name, fieldMeta.getValue());
						addedFields.put(name, dummy);
						matched = true;
						break;
					}
				}
				if (useResultKeys && !matched) {
					record.addData(fieldName, fieldMeta.getValue());
					addedFields.put(fieldName, dummy);
				}
			}
		}
		else{ //hbase之类的数据同步工具，数据都是在datarefactor接口中封装处理，columns信息不存在，直接用fieldValueMetas即可
			for (FieldMeta fieldMeta : fieldValueMetas) {
				String fieldName = fieldMeta.getEsFieldName();
				if (addedFields.containsKey(fieldName))
					continue;
				record.addData(fieldName, fieldMeta.getValue());
				addedFields.put(fieldName, dummy);

			}
		}
	}
}
