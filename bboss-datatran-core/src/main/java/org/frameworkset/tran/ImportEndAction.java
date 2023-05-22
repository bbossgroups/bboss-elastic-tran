package org.frameworkset.tran;
/**
 * Copyright 2022 bboss
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

/**
 * <p>Description: 任务结束处理逻辑</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/6
 * @author biaoping.yin
 * @version 1.0
 */
public interface ImportEndAction {
	/**
	 * 作业任务执行完毕后的处理操作
	 * @param importContext 作业定义配置上下文
	 * @param e  对应作业异常结束时的异常信息
	 */
	void endAction(ImportContext importContext,Exception e);
}
