package org.frameworkset.tran.schedule;
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

import com.frameworkset.util.SimpleStringUtil;

/**
 * <p>Description: 定时增量采集数据元数据配置</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/8 17:07
 * @author biaoping.yin
 * @version 1.0
 */
public class ImportIncreamentConfig {
	private String lastValueColumn;
	private String lastValueDateformat;
//	private String numberLastValueColumn;
	public static final int NUMBER_TYPE = 0;
	public static final int TIMESTAMP_TYPE = 1;
    public static final int LOCALDATETIME_TYPE = 2;
	/**
	 * 复合增量状态类型
	 */
	public static final int STRING_TYPE = 3;

    /**
     * 采集中状态
     */
	public static final int STATUS_COLLECTING = 0;
    /**
     * 采集完成状态
     */
	public static final int STATUS_COMPLETE = 1;
    /**
     * 采集文件已经不存在或者被删除状态：任务重启时或者正在采集时，检测到状态为采集中的文件已经被删除了，这样就会被标记为丢失的文件状态
     */
	public static final int STATUS_LOSTFILE = 2;

    /**
     * 设置增量状态ID生成策略，在设置jobId的情况下起作用
     * STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
     * STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用jobType+jobId+作业查询语句hashcode或者文件名称，作为增量id作为增量状态id
     * 默认值STATUSID_POLICY_JOBID_QUERYSTATEMENT 
     */
    public static final int STATUSID_POLICY_JOBID = 1;
    /**
     * 设置增量状态ID生成策略，在设置jobId的情况下起作用
     * STATUSID_POLICY_JOBID 采用jobType+jobId作为增量状态id
     * STATUSID_POLICY_JOBID_QUERYSTATEMENT 采用jobType+jobId+作业查询语句hashcode或者文件名称，作为增量id作为增量状态id
     * 默认值STATUSID_POLICY_JOBID_QUERYSTATEMENT 
     */
    public static final int STATUSID_POLICY_JOBID_QUERYSTATEMENT = 2;
	/**
	 * 设置起始值，如果lastValueType为
	 */
	private Object lastValue;
	/**
	 * 设置其实值类型：0 数字  1 日期 2 LOCALDATETIME_TYPE 3 json
	 */
	private Integer lastValueType = NUMBER_TYPE;
	private String lastValueStorePath;
	private String lastValueStoreTableName;
	private boolean lastValueDateType;
	private boolean fromFirst;//true 每次都重新从开始导入数据


	/**
	 * 根据导入的sql的hashcode决定导入作业的增量导入状态记录主键
	 */
	private String statusTableId = null;
	public String getStatusTableId() {
		return statusTableId;
	}

	public void setStatusTableId(String statusTableId) {
		this.statusTableId = statusTableId;
	}

	public void setLastValueColumn(String lastValueColumn) {
		this.lastValueColumn = lastValueColumn;
	}

    public boolean validate(){
        if(lastValueColumn == null || lastValueColumn.trim().equals(""))
        {
            return false;
        }
        else{            
            return true;
        }
    }



	public String getLastValueStorePath() {
		return lastValueStorePath;
	}

	public void setLastValueStorePath(String lastValueStorePath) {
		this.lastValueStorePath = lastValueStorePath;
	}

	public String getLastValueStoreTableName() {
		return lastValueStoreTableName;
	}

	public void setLastValueStoreTableName(String lastValueStoreTableName) {
		this.lastValueStoreTableName = lastValueStoreTableName;
	}

	public boolean isFromFirst() {
		return fromFirst;
	}

	public void setFromFirst(boolean fromFirst) {
		this.fromFirst = fromFirst;
	}

	/**
	 * 获取增量起始值
	 * @return
	 */
	public Object getLastValue() {
		return lastValue;
	}

	/**
	 * 设置增量起始值
	 * @param lastValue
	 */
	public void setLastValue(Object lastValue) {
		this.lastValue = lastValue;
	}

	public Integer getLastValueType() {
		return lastValueType;
	}

	public void setLastValueType(Integer lastValueType) {
		this.lastValueType = lastValueType;
		this.lastValueDateType = lastValueType == TIMESTAMP_TYPE;
	}

	public boolean isLastValueDateType() {
		return lastValueDateType;
	}

	public void setLastValueDateType(boolean lastValueDateType) {
		this.lastValueDateType = lastValueDateType;
	}

	public String getLastValueColumn() {
		return lastValueColumn;
	}

	public String getLastValueDateformat() {
		return lastValueDateformat;
	}

	public void setLastValueDateformat(String lastValueDateformat) {
		this.lastValueDateformat = lastValueDateformat;
	}
	public String toString(){
		return SimpleStringUtil.object2json(this);
	}
}
