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
import org.frameworkset.spi.geoip.IpInfo;
import org.frameworkset.tran.FieldMeta;
import org.frameworkset.tran.TranMeta;
import org.frameworkset.tran.config.ClientOptions;

import java.text.DateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/11 17:48
 * @author biaoping.yin
 * @version 1.0
 */
public interface Context {
	public void afterRefactor();
	public void setClientOptions(ClientOptions clientOptions);
	public Context addFieldValue(String fieldName, Object value);
	public Context addFieldValue(String fieldName, String dateFormat, Object value);
	public Context addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone);
	public Context addIgnoreFieldMapping(String dbColumnName);
	public ESIndexWrapper getESIndexWrapper();
	public Object getVersion() throws Exception;
//	public Object getEsVersionType();
	public void refactorData() throws Exception;
	public TranMeta getMetaData();
	public DateFormat getDateFormat();
	public Boolean getUseJavaName();
	public Boolean getUseLowcase();
//	public Boolean getEsDocAsUpsert();
//	public Object getEsDetectNoop();
//	public Boolean getEsReturnSource();
	public List<FieldMeta> getESJDBCFieldValues();
	public Object getValue(int i, String colName, int sqlType) throws Exception;
	public ImportContext getImportContext();
	public String getDBName();
	public Object getValue(String fieldName) throws Exception;
	public Object getMetaValue(String fieldName) throws Exception;
	public String getStringValue(String fieldName) throws Exception;
	public Object getParentId() throws Exception;
	public Object getRouting() throws Exception;
//	public Object getEsRetryOnConflict();
	public long getLongValue(String fieldName) throws Exception;
	public String getStringValue(String fieldName,String defaultValue) throws Exception;
	public boolean getBooleanValue(String fieldName) throws Exception;
	public boolean getBooleanValue(String fieldName,boolean defaultValue) throws Exception;
	public double getDoubleValue(String fieldName) throws Exception;
	public float getFloatValue(String fieldName) throws Exception;
	public int getIntegerValue(String fieldName) throws Exception;
	public Date getDateValue(String fieldName) throws Exception;
	public Date getDateValue(String fieldName,DateFormat dateFormat) throws Exception;
	public List<FieldMeta> getFieldValues();
	public Map<String,FieldMeta> getFieldMetaMap();
	public FieldMeta getMappingName(String colName);
	Object getEsId() throws Exception;
	public ClientOptions getClientOptions();
//	ESField getEsIdField();
	public boolean isDrop();

	/**
	 * 设置是否过滤掉记录，true过滤，false 不过滤（默认值）
	 * @param drop
	 */
	public void setDrop(boolean drop);
	public IpInfo getIpInfo(String fieldName) throws Exception;
	public IpInfo getIpInfoByIp(String ip) ;

	/**
	 * 重命名字段和修改字段值
	 * @param fieldName
	 * @param newFieldName
	 * @param newFieldValue
	 * @throws Exception
	 */
	public void newName2ndData(String fieldName, String newFieldName, Object newFieldValue)throws Exception;

	public BatchContext getBatchContext();

	/**
	 * 获取原始记录对象
	 * @return
	 */
	public Object getRecord();

	/**
	 * 标识记录状态为insert操作（增加），默认值

	 */
	public void markRecoredInsert();
	/**
	 * 标识记录状态为update操作（修改）

	 */
	public void markRecoredUpdate();

	/**
	 * 标识记录状态为delete操作（删除）

	 */
	public void markRecoredDelete();

	/**
	 * 判断记录是否是新增记录(默认值)
	 * @return
	 */
	public boolean isInsert();

	/**
	 * 判断记录是否是更新操作
	 * @return
	 */
	public boolean isUpdate();

	/**
	 * 判断记录是否删除操作
	 * @return
	 */
	public boolean isDelete();

	/**
	 * 进行记录级别索引表名称设置
	 */
	public void setIndex(String indice);

	/**
	 * 进行记录级别索引类型设置
	 */
	public void setIndexType(String indiceType);

	/**
	 * 获取记录对应的索引表名称
	 * @return
	 */
	public String getIndex();

	/**
	 * 获取记录对应的索引类型名称
	 * @return
	 */
	public String getIndexType();

	public String getOperation();


}
