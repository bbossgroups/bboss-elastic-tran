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
 * <p>Description: 导数据之前处理逻辑</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/7/6
 * @author biaoping.yin
 * @version 1.0
 */
public interface ImportStartAction {
	/**
	 * 初始化之前执行的处理操作，比如后续初始化操作、数据处理过程中依赖的资源初始化
	 * @param importContext
	 */
	void startAction(ImportContext importContext);

	/**
	 * 所有初始化操作完成后，导出数据之前执行的操作
	 * @param importContext
	 */
	void afterStartAction(ImportContext importContext);
}
