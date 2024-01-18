package org.frameworkset.tran.context;
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

import com.frameworkset.orm.annotation.BatchContext;
import com.frameworkset.orm.annotation.ESIndexWrapper;
import org.frameworkset.elasticsearch.client.ResultUtil;
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.*;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.cdc.TableMapping;
import org.frameworkset.tran.config.*;
import org.frameworkset.tran.plugin.es.ESField;
import org.frameworkset.tran.plugin.db.input.DBInputConfig;
import org.frameworkset.tran.plugin.db.output.DBOutputConfig;
import org.frameworkset.tran.plugin.es.output.ElasticsearchOutputConfig;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.frameworkset.util.ClassUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/11 17:48
 * @author biaoping.yin
 * @version 1.0
 */
public class ContextImpl implements Context {
    private static Logger logger = LoggerFactory.getLogger(ContextImpl.class);
	private TableMapping tableMapping;
	private String recordKeyField;
	protected List<FieldMeta> fieldValues ;
	protected Map<String,FieldMeta> valuesIdxByName;
	protected Map<String,FieldMeta> fieldMetaMap;
	private boolean useBatchContextIndexName = false;
	protected Map<String,String> newfieldNames;
	protected Map<String,ColumnData> newfieldName2ndColumnDatas;
	protected BaseImportConfig baseImportConfig;
	protected TranResultSet tranResultSet;
	protected BatchContext batchContext;
	protected boolean drop;
	protected int action = Record.RECORD_INSERT;
	protected String index;
	protected String indexType;
	protected ESIndexWrapper esIndexWrapper;
	protected ClientOptions clientOptions;
	protected ImportContext importContext;
	protected TaskContext taskContext;
	protected ElasticsearchOutputConfig elasticsearchOutputConfig;
	protected DBInputConfig dbInputConfig;
	protected DBOutputConfig dbOutputConfig;
	protected CommonRecord commonRecord;
	/**
	 * 在记录处理过程中，使用的临时数据，不会进行持久化处理
	 */
	private Map<String,Object> tempDatas;
	public ContextImpl(TaskContext taskContext, ImportContext importContext, TranResultSet tranResultSet, BatchContext batchContext){
		this.baseImportConfig = importContext.getImportConfig();
		this.importContext = importContext;
		OutputConfig outputConfig = importContext.getOutputConfig();
		if(outputConfig instanceof ElasticsearchOutputConfig)
			elasticsearchOutputConfig = (ElasticsearchOutputConfig)outputConfig;
		else if(outputConfig instanceof DBOutputConfig){
			dbOutputConfig = (DBOutputConfig)outputConfig;
		}
		InputConfig inputConfig = importContext.getInputConfig();
		if(inputConfig instanceof DBInputConfig){
			dbInputConfig = (DBInputConfig)inputConfig;
		}
		this.tranResultSet = tranResultSet;
		this.batchContext = batchContext;
		if(taskContext != null) {
			this.taskContext = taskContext;
		}
		else{
			this.taskContext = tranResultSet.getRecordTaskContext();
		}
	}
	public TranMeta getMetaData(){
		return tranResultSet.getMetaData();

	}

	public TaskContext getTaskContext() {
		return taskContext;
	}

	@Override
	public void setIndex(String index) {
		this.index = index;
	}

	@Override
	public void setIndexType(String indexType) {
		this.indexType = indexType;
	}

	@Override
	public String getIndex() {
		return index;
	}

	@Override
	public String getIndexType() {
		return indexType;
	}
	public Map<String,Object> getGeoipConfig(){
		return baseImportConfig.getGeoipConfig();
	}
    /**
     * 获取记录元数据信息，目前暂时先提供mysqlbinlog的元数据信息，其他类型数据源为返回
     * @return
     */
    public Map<String, Object> getMetaDatas(){
        return tranResultSet.getMetaDatas();
    }

    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    public Map<String, Object> getUpdateFromDatas(){
        return tranResultSet.getUpdateFromDatas();
    }
	public void afterRefactor(){

		if(elasticsearchOutputConfig != null) {

			if (index != null && !index.equals("")) {
				if (indexType == null) {
					esIndexWrapper = new ESIndexWrapper(index, elasticsearchOutputConfig.getEsIndexWrapper().getType());
				}
				else {
					esIndexWrapper = new ESIndexWrapper(index, indexType);
				}
//			esIndexWrapper.setUseBatchContextIndexName(this.useBatchContextIndexName);
			}
		}
	}

	@Override
	public void setClientOptions(ClientOptions clientOptions) {

		this.clientOptions = clientOptions;
		if(elasticsearchOutputConfig.getClientOptions() != null && clientOptions != null){
			clientOptions.setParentClientOptions(elasticsearchOutputConfig.getClientOptions());
		}
	}

	public ClientOptions getClientOptions(){
		if(clientOptions != null){
			return clientOptions;
		}
		else{
			return elasticsearchOutputConfig != null ?elasticsearchOutputConfig.getClientOptions():null;
		}
	}
	public String getOperation(){
		if(this.isInsert() ){
			return "index";
		}
		else if(this.isUpdate()){
			return "update";
		}
		else if(isDelete()){
			return "delete";
		}
		else{
			return "index";
		}
	}


	@Override
	public List<FieldMeta> getGlobalFieldValues() {
		return baseImportConfig.getFieldValues();
	}
	public Boolean getUseLowcase() {
		return baseImportConfig.getUseLowcase();
	}
	public Object getValue(     int i,String  colName,int sqlType) throws Exception {
		return tranResultSet.getValue(i,colName,sqlType);
	}

	public Object getValue(String fieldName,int sqlType)  throws Exception{
		return tranResultSet.getValue(fieldName,sqlType);
	}
	public Boolean getUseJavaName() {
		return baseImportConfig.getUseJavaName();
	}
	public DateFormat getDateFormat(){
		return baseImportConfig.getFormat();
	}
	public void refactorData() throws Exception{
		DataRefactor dataRefactor = baseImportConfig.getDataRefactor();
		if(dataRefactor != null){

			dataRefactor.refactor(this);

		}
	}
	public ImportContext getImportContext(){
		return importContext;
	}
	public List<FieldMeta> getFieldValues(){
		return this.fieldValues;
	}
	public Map<String,FieldMeta> getFieldMetaMap(){
		return this.fieldMetaMap;
	}
	@Override
	public Context addFieldValue(String fieldName, Object value) {
		if(this.fieldValues == null) {
			fieldValues = new ArrayList<FieldMeta>();
			valuesIdxByName = new LinkedHashMap<>();
		}
		FieldMeta fieldMeta = ImportBuilder.addFieldValue(fieldValues,fieldName,value);
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}
    @Override
    public Context addFieldValues(Object bean){
        return addFieldValues(bean,true);
    }
    @Override
    public Context addFieldValues(Object bean,boolean ignoreNullField) {
        if(bean == null){
            return this;
        }
        if(bean instanceof Map){
            return addMapFieldValues((Map<String,Object>)bean);
        }
        ClassUtil.ClassInfo beanInfo = ClassUtil.getClassInfo(bean.getClass());
        List<ClassUtil.PropertieDescription> attributes = beanInfo.getPropertyDescriptors();
        Object value = null;
        String name = null;
        for(int i = 0; attributes != null && i < attributes.size();i ++ ) {
            ClassUtil.PropertieDescription property = attributes.get(i);
            if(property.canread()) {

                try {
                    value = property.getValue(bean);
                    name = property.getName();
                    if(value != null || !ignoreNullField) {
                        addFieldValue(name, value);
                    }                    
                } catch (InvocationTargetException e1) {
                    logger.error("获取属性[" + beanInfo.getName() + "." + property.getName() + "]值失败：", e1.getTargetException());
                    
                } catch (Exception e1) {
                    logger.error("获取属性[" + beanInfo.getName() + "." + property.getName() + "]值失败：", e1);
                   
                }
               
            }
        }
        return this;
    }

    @Override
    public Context addMapFieldValues(Map<String, Object> values) {
        
        return addMapFieldValues(  values,true);
    }

    /**
     * 将map中的所有键值对作为字段添加到记录中
     *  根据参数ignoreNullField控制是否忽略空值字段 true 忽略  false 不忽略
     * @param values
     * @param ignoreNullField
     * @return
     */
    @Override
    public Context addMapFieldValues( Map<String,Object> values,boolean ignoreNullField){
        if(values == null || values.size() == 0){
            return this;
        }
        Iterator<Map.Entry<String, Object>> iterator = values.entrySet().iterator();
        Object value = null;
        String name = null;
        while (iterator.hasNext()){
            Map.Entry<String, Object> entry = iterator.next();
            name = entry.getKey();
            value = entry.getValue();
            if(value != null || !ignoreNullField) {
                addFieldValue(name, value);
            }
        }
        return this;
    }

    @Override
	public Context addFieldValue(String fieldName, String dateFormat, Object value) {
		if(this.fieldValues == null) {
			fieldValues = new ArrayList<FieldMeta>();
			valuesIdxByName = new LinkedHashMap<>();
		}
		FieldMeta fieldMeta = ImportBuilder.addFieldValue(fieldValues,fieldName,dateFormat,value,baseImportConfig.getLocale(),baseImportConfig.getTimeZone());
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}

	@Override
	public Context addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone) {
		if(this.fieldValues == null) {
			fieldValues = new ArrayList<FieldMeta>();
			valuesIdxByName = new LinkedHashMap<>();
		}
		FieldMeta fieldMeta = ImportBuilder.addFieldValue(fieldValues,fieldName,dateFormat,value,locale,timeZone);
		valuesIdxByName.put(fieldName,fieldMeta);
		return this;
	}

	@Override
	public Context addIgnoreFieldMapping(String sourceFieldName) {
		if(fieldMetaMap == null){
			fieldMetaMap = new HashMap<String,FieldMeta>();
		}
		ImportBuilder.addIgnoreFieldMapping(fieldMetaMap,sourceFieldName);
		return this;
	}


	public String getDBName(){
		if(dbInputConfig != null)
			return dbInputConfig.getDBName();
		else if(dbOutputConfig != null){
			return dbInputConfig.getDBName();
		}
		else
			return null;
	}

	@Override
	public long getLongValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.longValue(value,0l);

	}


	@Override
	public String getStringValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.stringValue(value,null);

	}
	@Override
	public String getStringValue(String fieldName,String defaultValue) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.stringValue(value,defaultValue);

	}

	@Override
	public boolean getBooleanValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.booleanValue(value,false);

	}
	@Override
	public boolean getBooleanValue(String fieldName,boolean defaultValue) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.booleanValue(value,defaultValue);

	}
	@Override
	public double getDoubleValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.doubleValue(value,0d);
	}

	@Override
	public float getFloatValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.floatValue(value,0f);
	}

	@Override
	public int getIntegerValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		return ResultUtil.intValue(value,0);
	}



	@Override
	public Date getDateValue(String fieldName) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;
        else if(value instanceof String){
            LocalDateTime localDateTime = TimeUtil.localDateTime((String)value);
            return TimeUtil.convertLocalDatetime(localDateTime);
        }
		else if(value instanceof Date){
			return (Date)value;

		}
		else if(value instanceof LocalDateTime){
			return TimeUtil.convertLocalDatetime((LocalDateTime)value);

		}
		else if(value instanceof LocalDate){
			return TimeUtil.convertLocalDate((LocalDate)value);

		}
		else if(value instanceof BigDecimal){
			return new Date(((BigDecimal)value).longValue());
		}
		else if(value instanceof Long){
			return new Date(((Long)value).longValue());
		}
		throw new IllegalArgumentException("Convert date value failed:"+value );
	}

    @Override
    public LocalDateTime getLocalDateTime(String fieldName) throws Exception{
        Object value = this.getValue(fieldName);
        if(value == null)
            return null;
        else if(value instanceof String){
            return TimeUtil.localDateTime((String)value);

        }
        else if(value instanceof Date){
            return TimeUtil.date2LocalDateTime((Date)value);

        }
        else if(value instanceof LocalDateTime){
            return (LocalDateTime)value;

        }
        else if(value instanceof LocalDate){
            return TimeUtil.date2LocalDateTime(TimeUtil.convertLocalDate((LocalDate)value));

        }
        else if(value instanceof BigDecimal){
            return TimeUtil.date2LocalDateTime(new Date(((BigDecimal)value).longValue()));
        }
        else if(value instanceof Long){
            return TimeUtil.date2LocalDateTime( new Date(((Long)value).longValue()));
        }

        throw new IllegalArgumentException("Convert date value failed:"+value );
    }
    @Override
    public Date getDateValue(String fieldName, String dateFormat) throws Exception{
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(dateFormat);
        return getDateValue( fieldName, simpleDateFormat);
    }
	@Override
	public Date getDateValue(String fieldName,DateFormat dateFormat) throws Exception {
		Object value = this.getValue(fieldName);
		if(value == null)
			return null;
		else if(value instanceof Date){
			return (Date)value;

		}
		else if(value instanceof LocalDateTime){
			return TimeUtil.convertLocalDatetime((LocalDateTime)value);

		}
		else if(value instanceof LocalDate){
			return TimeUtil.convertLocalDate((LocalDate)value);

		}
		else if(value instanceof BigDecimal){
			return new Date(((BigDecimal)value).longValue());
		}
		else if(value instanceof Long){
			return new Date(((Long)value).longValue());
		}
		else if(value instanceof String){
//			SerialUtil.getDateFormateMeta().toDateFormat();
			return dateFormat.parse((String) value);
		}
		throw new IllegalArgumentException("Convert date value failed:"+value );
	}
	@Override
	public Object getResultSetValue(String fieldName){
		Object value = tranResultSet.getValue(fieldName);
		return TimeUtil.convertLocalDate(value);
	}

	public Object getValue(String fieldName) throws Exception{
		FieldMeta fieldMeta = null;
		if(this.valuesIdxByName != null){
			fieldMeta = valuesIdxByName.get(fieldName);


		}
		if(fieldMeta == null)
			fieldMeta = baseImportConfig.getValueIdxByName(fieldName);

		Object value = null;
		if(fieldMeta != null)
			value = fieldMeta.getValue();
		else
			value = tranResultSet.getValue(fieldName);
		return TimeUtil.convertLocalDate(value);
	}

	@Override
	public Object getMetaValue(String fieldName) throws Exception {
		return tranResultSet.getMetaValue(fieldName) ;
	}
	@Override
	public FieldMeta getMappingName(String sourceFieldName){
		if(fieldMetaMap != null) {
			FieldMeta fieldMeta = this.fieldMetaMap.get(sourceFieldName);
			if (fieldMeta != null) {
				return fieldMeta;
			}
		}
		return baseImportConfig.getMappingName(sourceFieldName);
	}

	@Override
	public Object getEsId() throws Exception {

		return elasticsearchOutputConfig.getEsIdGenerator().genId(this);
	}


	public boolean isDrop() {
		return drop;
	}

	public void setDrop(boolean drop) {
		this.drop = drop;
	}

	@Override
	public IpInfo getIpInfo(String fieldName) throws Exception{
//		Object _ip = tranResultSet.getValue(fieldName);
		Object _ip = getValue(fieldName);
		if(_ip == null){
			return null;
		}
		if(BaseImportConfig.getGeoIPUtil(getGeoipConfig()) != null) {
			return BaseImportConfig.getGeoIPUtil(getGeoipConfig()).getIpInfo(String.valueOf(_ip));
		}
		return null;
	}

	@Override
	public IpInfo getIpInfoByIp(String ip) {
		if(BaseImportConfig.getGeoIPUtil(getGeoipConfig()) != null) {
			return BaseImportConfig.getGeoIPUtil(getGeoipConfig()).getIpInfo(ip);
		}
		return null;
	}

	/**
	 * 重命名字段和并设置修改后字段值
	 * @param fieldName
	 * @param newFieldName
	 * @param newFieldValue
	 * @throws Exception
	 */
	public void newName2ndData(String fieldName, String newFieldName, Object newFieldValue)throws Exception{
		this.addFieldValue(newFieldName,newFieldValue);//将long类型的时间戳转换为Date类型
		//忽略旧的名称
		if(!fieldName.equals(newFieldName))
			this.addIgnoreFieldMapping(fieldName);
	}


	public BatchContext getBatchContext() {
		return batchContext;
	}

	public  Object getParentId() throws Exception {
		ClientOptions clientOptions = getClientOptions();
		ESField esField = clientOptions != null?clientOptions.getParentIdField():null;
		if(esField != null) {
			if(!esField.isMeta())
//				return tranResultSet.getValue(esField.getField());
				return getValue(esField.getField());
			else{
				return tranResultSet.getMetaValue(esField.getField());
			}
		}
		else
			return clientOptions != null?clientOptions.getEsParentIdValue():null;
	}
	/**
	 * 获取原始数据对象,，可能是一个map，jdbcresultset，DBObject,hbaseresult
	 * @return
	 */
	public Object getRecord(){
		return tranResultSet.getRecord();
	}
	/**
	 * 获取记录对象
	 * @return
	 */
	public Record getCurrentRecord(){
		return tranResultSet.getCurrentRecord();
	}


	@Override
	public void markRecoredInsert() {
		this.action = Record.RECORD_INSERT;
	}

	@Override
	public void markRecoredUpdate() {
		this.action = Record.RECORD_UPDATE;
	}
	@Override
	public void markRecoredReplace() {
		this.action = Record.RECORD_REPLACE;
	}

    public void setAction(int action){
        this.action = action;
    }

	@Override
	public void markRecoredDelete() {
		this.action = Record.RECORD_DELETE;
	}

    /**
     * 标识记录状态为ddl操作（DDL）

     */
    @Override
    public void markRecoredDDL(){
        this.action = Record.RECORD_DDL;
    }

	@Override
	public boolean isInsert() {
		return action == Record.RECORD_INSERT;
	}

	@Override
	public boolean isUpdate() {
		return action == Record.RECORD_UPDATE;
	}
	public boolean isReplace(){
		return action ==  Record.RECORD_REPLACE;
	}
    /**
     * 判断记录是否DDL操作
     * @return
     */
    @Override
    public boolean isDDL(){
        return action == Record.RECORD_DDL;
    }

	@Override
	public boolean isDelete() {
		return action == Record.RECORD_DELETE;
	}
    /**
     * 获取记录操作标记：0 -- insert  1 -- update 2 -- delete
     * @return
     */
    public int getAction(){
        return action;
    }

	public Object getRouting() throws Exception{
		ClientOptions clientOptions = getClientOptions();
		ESField esField = clientOptions != null?clientOptions.getRoutingField():null;
		Object routing = null;
		if(esField != null) {
			if(!esField.isMeta())
//				routing = tranResultSet.getValue(esField.getField());
				routing = getValue(esField.getField());

			else{
				routing = tranResultSet.getMetaValue(esField.getField());
			}
		}
		else {
			routing = clientOptions != null? clientOptions.getRouting():null;
		}
		return routing;
	}

	public ESIndexWrapper getESIndexWrapper(){
		if(esIndexWrapper == null)
			return elasticsearchOutputConfig != null?elasticsearchOutputConfig.getEsIndexWrapper():null;
		else
			return esIndexWrapper;
	}

	public Object getVersion() throws Exception {
		ClientOptions clientOptions = getClientOptions();
		ESField esField = clientOptions != null ?clientOptions.getVersionField():null;
		Object version = null;
		if(esField != null) {
			if(!esField.isMeta())
//				version = tranResultSet.getValue(esField.getField());
				version = getValue(esField.getField());
			else{
				version = tranResultSet.getMetaValue(esField.getField());
			}
		}
		else {
			version =  clientOptions != null?clientOptions.getVersion():null;
		}
		return version;
	}


	public boolean isUseBatchContextIndexName() {
		return useBatchContextIndexName;
	}

	public void setUseBatchContextIndexName(boolean useBatchContextIndexName) {
		this.useBatchContextIndexName = useBatchContextIndexName;
	}

	@Override
	public boolean removed() {
		return this.tranResultSet.removed() ;
	}


	public boolean reachEOFClosed(){
		return this.tranResultSet.reachEOFClosed() ;
	}
	public JobContext getJobContext(){
		if(importContext != null) {
			return importContext.getJobContext();
		}
		else{
			return null;
		}
	}
	@Override
	public void setCommonRecord(CommonRecord commonRecord){
		this.commonRecord = commonRecord;
	}
	@Override
	public CommonRecord getCommonRecord() {
		return commonRecord;
	}


	/**
	 * 获取用于指标计算处理等的临时数据
	 * @return
	 */
	public Map<String, Object> getTempDatas() {
		return tempDatas;
	}

	/**
	 * 添加用于指标计算处理等的临时数据到记录，不会对临时数据进行持久化处理，
	 * @param name
	 * @param tmpData
	 */
	@Override
	public void addTempData(String name,Object tmpData){
		if(tempDatas == null){
			tempDatas = new LinkedHashMap<>();
		}
		tempDatas.put(name,tmpData);
	}
	@Override
	public TableMapping getTableMapping() {
		return tableMapping;
	}
	@Override
	public void setTableMapping(TableMapping tableMapping) {
		this.tableMapping = tableMapping;
	}
	@Override
	public String getRecordKeyField() {
		return recordKeyField;
	}
	@Override
	public void setRecordKeyField(String recordKeyField) {
		this.recordKeyField = recordKeyField;
	}

}
