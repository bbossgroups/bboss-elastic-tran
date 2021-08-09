package org.frameworkset.tran.util;
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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/4/5 10:45
 * @author biaoping.yin
 * @version 1.0
 */
public class TranConstant {
	/**
	 * 插件停止中
	 */
	public static final int PLUGIN_STOPPING = 1;
	/**
	 * 插件准备停止
	 */
	public static final int PLUGIN_STOPAPPENDING = 2;
	/**
	 * 插件准备停止条件具备，可以停止
	 */
	public static final int PLUGIN_STOPREADY = 3;

	/**
	 * 插件启动
	 */
	public static final int PLUGIN_START = 0;

	public static final int STATUS_STOP = 1;
	public static final int STATUS_STOPTRANONLY = 2;
}
