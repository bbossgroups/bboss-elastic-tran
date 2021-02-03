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

import org.frameworkset.tran.context.ImportContext;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/10/15 20:56
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskContext {
	private Map<String,Object> taskDatas;
	public TaskContext(ImportContext importContext,ImportContext targetImportContext){
		this.importContext = importContext;
		this.targetImportContext = targetImportContext;
		taskDatas = new HashMap<String, Object>();
	}
	private ImportContext importContext;

	public ImportContext getTargetImportContext() {
		return targetImportContext;
	}
	public TaskContext addTaskData(String name,Object value){
		taskDatas.put(name,value);
		return this;
	}
	public Object getTaskData(String name){
		return taskDatas.get(name);
	}
	public void setTargetImportContext(ImportContext targetImportContext) {
		this.targetImportContext = targetImportContext;
	}

	private ImportContext targetImportContext;
	public ImportContext getImportContext() {
		return importContext;
	}
	public void release(){
		this.taskDatas.clear();
		this.taskDatas = null;
	}
}
