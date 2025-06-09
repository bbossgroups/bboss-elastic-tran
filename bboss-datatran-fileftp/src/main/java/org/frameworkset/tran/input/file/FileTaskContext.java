package org.frameworkset.tran.input.file;
/**
 * Copyright 2020 bboss
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
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/8/4 17:22
 * @author biaoping.yin
 * @version 1.0
 */
public class FileTaskContext extends TaskContext {
	private FileInfo fileInfo;

	public FileTaskContext(ImportContext importContext) {
		super(importContext);
	}

	public void setFileInfo(FileInfo fileInfo) {
		this.fileInfo = fileInfo;
	}

	public FileInfo getFileInfo() {
		return fileInfo;
	}

    /**
     * 是否需要触发工作流的节点完成处理，多转换处理任务（文件采集插件无需触发，每个文件完成时会执行任务完成拦截器，需要所有调度完成后触发节点完成操作）
     * @return
     */
    @Override
    public boolean neadTriggerJobFlowNodeComplete(){
        return false;
    }
}
