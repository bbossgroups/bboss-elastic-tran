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
import org.frameworkset.tran.Record;
import org.frameworkset.tran.*;
import org.frameworkset.tran.cdc.TableMapping;
import org.frameworkset.tran.config.ClientOptions;
import org.frameworkset.tran.metrics.DataTranPluginMetricsLogAPI;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.record.ValueConvert;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.util.annotations.DateFormateMeta;

import java.text.DateFormat;
import java.time.LocalDateTime;
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
public interface Context extends DataTranPluginMetricsLogAPI {
    public static final String KAFKA_TOPIC_KEY = "_____kafka.topic";
    TaskMetrics getTaskMetrics();
    void setTaskMetrics(TaskMetrics taskMetrics);
    /**
     * 设置记录级别的kafka主题
     * @param topic
     */
    public void setKafkaTopic(String topic);
	public TableMapping getTableMapping() ;

	public void setTableMapping(TableMapping tableMapping) ;

	public String getRecordKeyField() ;

	public void setRecordKeyField(String recordKeyField);

	/**
	 * 获取用于指标计算处理等的临时数据
	 * @return
	 */
	public Map<String, Object> getTempDatas();
	/**
	 * 添加用于指标计算处理等的临时数据到记录，不会对临时数据进行持久化处理，
	 * @param name
	 * @param tmpData
	 */
	public void addTempData(String name,Object tmpData);
    public void setAction(int action);
    /**
     * 获取记录元数据信息，目前暂时先提供mysqlbinlog的元数据信息，其他类型数据源为返回
     * @return
     */
    Map<String, Object> getMetaDatas();
    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    public Map<String, Object> getUpdateFromDatas();

    Map<String, Object> getGeoipConfig();

    /**
     * 获取任务上下文对象TaskContext，可以用于获取任务上下文中的数据
     * Object getTaskData(String name)
     *
     * @return
     */
	public TaskContext getTaskContext();

    /**
     * 获取作业上下文对象，可以用于获取以下数据：
     * Object getJobData(String name)
     * @return
     */
	public JobContext getJobContext();
	void afterRefactor();

    /**
     * Elasticsearch输出插件：用于设置批量操作中每条记录的控制参数
     *
     * 参考文档：
     *
     * https://esdoc.bbossgroups.com/#/development?id=_482-基于clientoptionupdateoption指定添加修改文档控制参数
     *
     *  ClientOptions:主要用于新增/修改操作，可以指定以下属性：
     *
     *     private String parentIdField;
     *     private String idField;
     *     private String esRetryOnConflictField;
     *     private String versionField;
     *     private String versionTypeField;
     *     private String rountField;
     * @param clientOptions
     */
	void setClientOptions(ClientOptions clientOptions);
	void setCommonRecord(CommonRecord commonRecord);

    /**
     * 获取处理后的记录数据封装对象CommonRecord，包含以下数据
     * 来源数据   Object getOringeData()
     *  加工后的数据 Map<String,Object> getDatas()
     * @return
     */
	public CommonRecord getCommonRecord();
	/**
	 * 直接获取数据源记录中对应的字段值
	 * @param fieldName
	 * @return
	 */
	public Object getResultSetValue(String fieldName);

    /**
     * 往记录中添加字段和对应的值
     * @param fieldName
     * @param value
     * @return
     */
	Context addFieldValue(String fieldName, Object value);

    /**
     * 将对象中的所有字段和值作为字段添加到记录中，忽略空值字段
     * @param bean
     * @return
     */
    Context addFieldValues( Object bean);

    /**
     * 将对象中的所有字段和值作为字段添加到记录中
     * 根据参数ignoreNullField控制是否忽略空值字段 true 忽略  false 不忽略
     * @param bean
     * @param ignoreNullField
     * @return
     */
    Context addFieldValues(Object bean,boolean ignoreNullField);

    /**
     * 将map中的所有键值对作为字段添加到记录中，忽略空值字段
     * @param values
     * @return
     */
    Context addMapFieldValues( Map<String,Object> values);

    /**
     * 将map中的所有键值对作为字段添加到记录中
     *  根据参数ignoreNullField控制是否忽略空值字段 true 忽略  false 不忽略
     * @param values
     * @param ignoreNullField
     * @return
     */
    Context addMapFieldValues( Map<String,Object> values,boolean ignoreNullField);

    /**
     * 往记录中添加日期类型字段和对应的值，并指定对应的日期格式
     * @param fieldName
     * @param dateFormat
     * @param value
     * @return
     */
	Context addFieldValue(String fieldName, String dateFormat, Object value);

    /**
     * 往记录中添加日期类型字段和对应的值，并指定对应的日期格式、时区、本地化编码
     * @param fieldName
     * @param dateFormat
     * @param value
     * @param locale
     * @param timeZone
     * @return
     */
	Context addFieldValue(String fieldName, String dateFormat, Object value, String locale, String timeZone);

    /**
     * 忽略记录中的字段
     * @param dbColumnName
     * @return
     */
	Context addIgnoreFieldMapping(String dbColumnName);
	ESIndexWrapper getESIndexWrapper();
	Object getVersion() throws Exception;
//	public Object getEsVersionType();
	void refactorData() throws Exception;

    /**
     * 获取记录元数据信息
     * @return
     */
	TranMeta getMetaData();
    DateFormateMeta getDateFormateMeta();
	Boolean getUseJavaName();
	Boolean getUseLowcase();
//	public Boolean getEsDocAsUpsert();
//	public Object getEsDetectNoop();
//	public Boolean getEsReturnSource();
	List<FieldMeta> getGlobalFieldValues();
	Object getValue(int i, String colName, int sqlType) throws Exception;
	ImportContext getImportContext();
	String getDBName();

	/**
	 * 获取加工字段或者源字段值，优先返回字段
	 * 记录级添加字段优先级>全局添加字段值优先级>数据源级字段优先级
	 * @param fieldName
	 * @return
	 * @throws Exception
	 */
	Object getValue(String fieldName) throws Exception;

    /**
     * 获取元数据（Elasticsearch/HBase/文件中的元数据），参考文档：
     * https://esdoc.bbossgroups.com/#/db-es-tool?id=_28104-获取记录元数据
     * @param fieldName
     * @return
     * @throws Exception
     */
	Object getMetaValue(String fieldName) throws Exception;
	String getStringValue(String fieldName, ValueConvert valueConvert) throws Exception;

    String getStringValue(String fieldName) throws Exception;
	Object getParentId() throws Exception;
	Object getRouting() throws Exception;
//	public Object getEsRetryOnConflict();
	long getLongValue(String fieldName) throws Exception;
	String getStringValue(String fieldName, String defaultValue) throws Exception;
	boolean getBooleanValue(String fieldName) throws Exception;
	boolean getBooleanValue(String fieldName, boolean defaultValue) throws Exception;
	double getDoubleValue(String fieldName) throws Exception;
	float getFloatValue(String fieldName) throws Exception;
	int getIntegerValue(String fieldName) throws Exception;
	Date getDateValue(String fieldName) throws Exception;
	Object getValue(String fieldName,int sqlType)  throws DataImportException;
	Date getDateValue(String fieldName, DateFormat dateFormat) throws Exception;
    Date getDateValue(String fieldName, String dateFormat) throws Exception;
	List<FieldMeta> getFieldValues();
	Map<String,FieldMeta> getFieldMetaMap();

	/**
	 * 记录级字段映射优先级>全局字段映射优先级
	 * @param colName
	 * @return
	 */
	FieldMeta getMappingName(String colName);
	Object getEsId() throws Exception;
	ClientOptions getClientOptions();
//	ESField getEsIdField();
	boolean isDrop();

	/**
	 * 设置是否过滤掉记录，true过滤，false 不过滤（默认值）
	 * @param drop
	 */
	void setDrop(boolean drop);

    /**
     * 获取字段值对应的ip地址信息：国家、地区、运营商、经纬度信息
     * @param fieldName
     * @return
     * @throws Exception
     */
	IpInfo getIpInfo(String fieldName) throws Exception;

    /**
     * 将ip地址转换为ip地址信息：国家、地区、运营商、经纬度信息
     * @param ip
     * @return
     */
	IpInfo getIpInfoByIp(String ip) ;

	/**
	 * 重命名字段和修改字段值
	 * @param fieldName
	 * @param newFieldName
	 * @param newFieldValue
	 * @throws Exception
	 */
	void newName2ndData(String fieldName, String newFieldName, Object newFieldValue)throws Exception;
    public boolean getBooleanValue(String fieldName,ValueConvert valueConvert) throws Exception;
    BatchContext getBatchContext();
    public Object getValue(String fieldName,ValueConvert valueConvert) throws Exception;

	/**
	 * 获取原始数据对象,可能是一个map，jdbcresultset，DBObject,hbaseresult
	 * @return
	 */
	Object getRecord();

	/**
	 * 获取记录对象
	 * @return
	 */
	public Record getCurrentRecord();

	/**
	 * 标识记录状态为insert操作（增加），默认值

	 */
	void markRecoredInsert();
	/**
	 * 标识记录状态为update操作（修改）

	 */
	void markRecoredUpdate();

	/**
	 * 标识记录状态为delete操作（删除）

	 */
	void markRecoredDelete();

    /**
     * 标识记录状态为ddl操作（DDL）

     */
    void markRecoredDDL();
	public void markRecoredReplace();
	/**
	 * 判断记录是否是新增记录(默认值)
	 * @return
	 */
	boolean isInsert();

	/**
	 * 判断记录是否是更新操作
	 * @return
	 */
	boolean isUpdate();

	/**
	 * 判断记录是否删除操作
	 * @return
	 */
	boolean isDelete();

	/**
	 * 判断记录是否删除操作
	 * @return
	 */
	boolean isReplace();

    /**
     * 判断记录是否DDL操作
     * @return
     */
    boolean isDDL();


    /**
     * 获取记录操作标记：0 -- insert  1 -- update 2 -- delete
     * @return
     */
    int getAction();

	/**
	 * 进行记录级别索引表名称设置
	 */
	void setIndex(String indice);

	/**
	 * 进行记录级别索引类型设置
	 */
	void setIndexType(String indiceType);

	/**
	 * 获取记录对应的索引表名称
	 * @return
	 */
	String getIndex();

	/**
	 * 获取记录对应的索引类型名称
	 * @return
	 */
	String getIndexType();

	String getOperation();

	boolean isUseBatchContextIndexName() ;

	void setUseBatchContextIndexName(boolean useBatchContextIndexName) ;

	/**
	 * 数据预校验，true 继续后续处理，false 忽略当前记录处理
	 * @return
	 */
	boolean removed();

	boolean reachEOFClosed();
    LocalDateTime getLocalDateTime(String fieldName) throws Exception;
    Object getKeys();

    /**
     * 记录作业处理过程中的异常日志
     * @param msg
     * @param e
     */
    default public void reportJobMetricErrorLog( String msg, Throwable e){

    }



    /**
     * 记录作业处理过程中的日志
     * @param msg
     */
    default public void reportJobMetricLog(String msg){

    }

    /**
     * 记录作业处理过程中的告警日志
     * @param msg
     */
    default public void reportJobMetricWarn(  String msg){

    }

    /**
     * 记录作业处理过程中的debug日志
     * @param msg
     */
    default public void reportJobMetricDebug(  String msg){

    }

    /**
     * 记录作业任务处理过程中的异常日志
     * @param msg
     * @param e
     */
    default public void reportTaskMetricErrorLog(String msg, Throwable e){

    }



    /**
     * 记录作业任务处理过程中的日志
     * @param msg
     */
    default public void reportTaskMetricLog( String msg){

    }

    /**
     * 记录作业任务处理过程中的告警日志
     * @param msg
     */
    default public void reportTaskMetricWarn(String msg){

    }

    /**
     * 记录作业任务处理过程中的debug日志
     * @param msg
     */
    default public void reportTaskMetricDebug(String msg){

    }
}
