package org.frameworkset.tran;
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

import org.frameworkset.tran.schedule.TaskContext;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 10:29
 * @author biaoping.yin
 * @version 1.0
 */
public interface Record {
    public final int RECORD_INSERT = 0;
    public final int RECORD_UPDATE = 1;
    public final int RECORD_DELETE = 2;
    public final int RECORD_DDL = 5;
	public final int RECORD_REPLACE = 6;
    /**
     * 被标记为跳进指令，只是为了传递数据位置状态信息：lastValue、binglogfile等信息
     */
    public final int RECORD_DIRECT_IGNORE = 3;
	public Object getValue(  int i, String colName,int sqlType) throws DataImportException;
	public Object getValue( String colName,int sqlType) throws DataImportException;
	public Date getDateTimeValue(String colName) throws DataImportException;
    public LocalDateTime getLocalDateTimeValue(String colName) throws DataImportException;
	public Date getDateTimeValue(String colName,String dateformat) throws DataImportException;
	Object getValue(String colName);
	public Object getKeys();
	public Object getData();

	public Object getMetaValue(String metaName);
	default public long getOffset(){
        return 0L;
    }


	boolean removed();
	boolean reachEOFClosed();
    boolean reachEOFRecord();
	TaskContext getTaskContext();
    public Map<String, Object> getMetaDatas();

    /**
     * 获取binlog采集的修改前记录信息
     * @return
     */
    Map<String, Object> getUpdateFromDatas();
    public int getAction();
}
