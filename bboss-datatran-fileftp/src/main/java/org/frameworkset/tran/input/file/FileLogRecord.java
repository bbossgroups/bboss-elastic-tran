package org.frameworkset.tran.input.file;
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.record.CommonMapRecord;
import org.frameworkset.tran.schedule.TaskContext;

import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/19 11:09
 * @author biaoping.yin
 * @version 1.0
 */
public class FileLogRecord extends CommonMapRecord {
	/**
	 * 		   common.put("hostIp", BaseSimpleStringUtil.getIp());
	 *         common.put("hostName",BaseSimpleStringUtil.getHostName());
	 *         common.put("filePath",file.getAbsoluteFile());
	 *         common.put("timestamp",new Date());
	 *         common.put("pointer",pointer);
	 *         common.put("fileId"
	 */


	public FileLogRecord(TaskContext taskContext, ImportContext importContext, Map meta, Object key, Map<String,Object> record, long offset, boolean reachEOFClosed){
		super(  taskContext,  importContext,key,   record,   offset);

		this.reachEOFClosed = reachEOFClosed;
		this.taskContext = taskContext;
        setMetaDatas(meta);

	}

	public FileLogRecord(TaskContext taskContext,ImportContext importContext,Map meta,Map<String,Object> record, long offset,boolean reachEOFClosed){
		super(  taskContext,  importContext,  record,   offset);
        setMetaDatas(meta);
		this.reachEOFClosed = reachEOFClosed;
		this.taskContext = taskContext;
	}
	public FileLogRecord(TaskContext taskContext,ImportContext importContext,boolean removed, long offset,boolean reachEOFClosed,boolean readEOFRecord){
		super(   taskContext,  importContext,  (Map<String,Object>)null,  removed,  offset, reachEOFClosed, readEOFRecord);

	}







}
