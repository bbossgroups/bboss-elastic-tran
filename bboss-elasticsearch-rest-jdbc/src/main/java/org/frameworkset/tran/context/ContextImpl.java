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
import org.frameworkset.tran.config.BaseImportBuilder;
import org.frameworkset.tran.config.BaseImportConfig;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.db.input.es.DB2ESImportBuilder;
import org.frameworkset.tran.es.ESField;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.schedule.timer.TimeUtil;

import java.math.BigDecimal;
import java.text.DateFormat;
import java.time.*;
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
	protected List<FieldMeta> fieldValues ;
	protected Map<String,FieldMeta> fieldMetaMap;
	private boolean useBatchContextIndexName = false;
	protected Map<String,String> newfieldNames;
	protected Map<String,ColumnData> newfieldName2ndColumnDatas;
	protected BaseImportConfig baseImportConfig;
	protected TranResultSet jdbcResultSet;
	protected BatchContext batchContext;
	protected boolean drop;
	protected int status = 0;
	protected String index;
	protected String indexType;
	protected ESIndexWrapper esIndexWrapper;
	protected ClientOptions clientOptions;
	protected ImportContext importContext;
	protected ImportContext targetImportContext;
	protected TaskContext taskContext;
	public ContextImpl(TaskContext taskContext,ImportContext importContext,ImportContext targetImportContext, TranResultSet jdbcResultSet, BatchContext batchContext){
		this.baseImportConfig = importContext.getImportConfig();
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
		this.jdbcResultSet = jdbcResultSet;
		this.batchContext = batchContext;
		if(taskContext != null) {
			this.taskContext = taskContext;
		}
		else{
			this.taskContext = jdbcResultSet.getRecordTaskContext();
		}
	}
	public TranMeta getMetaData(){
		return jdbcResultSet.getMetaData();

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
	public void afterRefactor(){
		if(index != null && !index.equals("")){
			if(indexType == null)
				esIndexWrapper = new ESIndexWrapper(index,targetImportContext.getEsIndexWrapper().getType());
			else{
				esIndexWrapper = new ESIndexWrapper(index,indexType);
			}
//			esIndexWrapper.setUseBatchContextIndexName(this.useBatchContextIndexName);
		}
	}

	@Override
	public void setClientOptions(ClientOptions clientOptions) {

		this.clientOptions = clientOptions;
		if(targetImportContext.getClientOptions() != null && clientOptions != null){
			clientOptions.setParentClientOptions(targetImportContext.getClientOptions());
		}
	}

	public ClientOptions getClientOptions(){
		if(clientOptions != null){
			return clientOptions;
		}
		else{
			return targetImportContext.getClientOptions();
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



	public List<FieldMeta> getESJDBCFieldValues() {
		return baseImportConfig.getFieldValues();
	}
	public Boolean getUseLowcase() {
		return baseImportConfig.getUseLowcase();
	}
	public Object getValue(     int i,String  colName,int sqlType) throws Exception {
		return jdbcResultSet.getValue(i,colName,sqlType);
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
		if(this.fieldValues == null)
			fieldValues = new ArrayList<FieldMeta>();
		DB2ESImportBuilder.addFieldValue(fieldValues,fieldName,value);
		return this;
	}

	@Override
	public Context addFieldValue(String fieldName, String dateFormat, Object value) {
		if(this.fieldValues == null)
			fieldValues = new ArrayList<FieldMeta>();
		DB2ESImportBuilder.addFieldValue(fieldValues,fieldName,dateFormat,value,baseImportConfig.getLocale(),baseImportConfig.getTimeZone());
		return this;
	}

	@Override
	public Context addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone) {
		if(this.fieldValues == null)
			fieldValues = new ArrayList<FieldMeta>();
		DB2ESImportBuilder.addFieldValue(fieldValues,fieldName,dateFormat,value,locale,timeZone);
		return this;
	}

	@Override
	public Context addIgnoreFieldMapping(String sourceFieldName) {
		if(fieldMetaMap == null){
			fieldMetaMap = new HashMap<String,FieldMeta>();
		}
		BaseImportBuilder.addIgnoreFieldMapping(fieldMetaMap,sourceFieldName);
		return this;
	}


	public String getDBName(){
		return baseImportConfig.getDbConfig().getDbName();
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

	public Object getValue(String fieldName) throws Exception{
		Object value = jdbcResultSet.getValue(fieldName);
		return TimeUtil.convertLocalDate(value);
	}

	@Override
	public Object getMetaValue(String fieldName) throws Exception {
		return jdbcResultSet.getMetaValue(fieldName) ;
	}

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

		return baseImportConfig.getEsIdGenerator().genId(this);
	}


	public boolean isDrop() {
		return drop;
	}

	public void setDrop(boolean drop) {
		this.drop = drop;
	}

	@Override
	public IpInfo getIpInfo(String fieldName) throws Exception{
		Object _ip = jdbcResultSet.getValue(fieldName);
		if(_ip == null){
			return null;
		}
		if(BaseImportConfig.getGeoIPUtil(getGeoipConfig()) != null) {
			return BaseImportConfig.getGeoIPUtil(getGeoipConfig()).getAddressMapResult(String.valueOf(_ip));
		}
		return null;
	}

	@Override
	public IpInfo getIpInfoByIp(String ip) {
		if(BaseImportConfig.getGeoIPUtil(getGeoipConfig()) != null) {
			return BaseImportConfig.getGeoIPUtil(getGeoipConfig()).getAddressMapResult(ip);
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
				return jdbcResultSet.getValue(esField.getField());
			else{
				return jdbcResultSet.getMetaValue(esField.getField());
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
		return jdbcResultSet.getRecord();
	}
	/**
	 * 获取记录对象
	 * @return
	 */
	public Record getCurrentRecord(){
		return jdbcResultSet.getCurrentRecord();
	}


	@Override
	public void markRecoredInsert() {
		this.status = 0;
	}

	@Override
	public void markRecoredUpdate() {
		this.status = 1;
	}

	@Override
	public void markRecoredDelete() {
		this.status = 2;
	}

	@Override
	public boolean isInsert() {
		return status == 0;
	}

	@Override
	public boolean isUpdate() {
		return status == 1;
	}

	@Override
	public boolean isDelete() {
		return status == 2;
	}



	public Object getRouting() throws Exception{
		ClientOptions clientOptions = getClientOptions();
		ESField esField = clientOptions != null?clientOptions.getRoutingField():null;
		Object routing = null;
		if(esField != null) {
			if(!esField.isMeta())
				routing = jdbcResultSet.getValue(esField.getField());
			else{
				routing = jdbcResultSet.getMetaValue(esField.getField());
			}
		}
		else {
			routing = clientOptions != null? clientOptions.getRouting():null;
		}
		return routing;
	}

	public ESIndexWrapper getESIndexWrapper(){
		if(esIndexWrapper == null)
			return targetImportContext.getEsIndexWrapper();
		else
			return esIndexWrapper;
	}

	public Object getVersion() throws Exception {
		ClientOptions clientOptions = getClientOptions();
		ESField esField = clientOptions != null ?clientOptions.getVersionField():null;
		Object version = null;
		if(esField != null) {
			if(!esField.isMeta())
				version = jdbcResultSet.getValue(esField.getField());
			else{
				version = jdbcResultSet.getMetaValue(esField.getField());
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
		return this.jdbcResultSet.removed() ;
	}


	public boolean reachEOFClosed(){
		return this.jdbcResultSet.reachEOFClosed() ;
	}
}
