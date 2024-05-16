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

import org.frameworkset.tran.context.Context;
import org.frameworkset.tran.schedule.TaskContext;

/**
 * <p>Description: 加工处理数据接口</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/4 9:19
 * @author biaoping.yin
 * @version 1.0
 */
public interface DataRefactor {
	/**
	 * 数据处理第一阶段：串行处理阶段，串行加工处理数据方法,适应于轻量级数据处理逻辑
	 * @param context 包含需要加工数据记录的上下文对象
	 * @return
	 */
	public void refactor(Context context) throws Exception;

    /**
     * 数据处理第二阶段：并行处理阶段，
     * @param taskContext
     * @param commonRecord
     * @throws Exception
     */
    default public void refactorStage2(TaskContext taskContext,CommonRecord commonRecord) throws Exception{
        
    }
}
