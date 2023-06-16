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
     * 文件采集扫描初始状态
     */
    public static final int scanInit = -1;
    /**
     * 文件采集扫描开始
     */
    public static final int scanStart = 1;
    /**
     * 文件采集扫描结束
     */
    public static final int scanFinished = 2;
	/**
	 * 插件准备停止
	 */
	public static final int PLUGIN_STOPAPPENDING = 2;
	/**
	 * 插件准备停止条件具备，可以停止,但是不能作为数据采集退出的标记
     * 是所有tran执行完成后自动设置的状态标记，说明次状态可以退出同步作业
     * 作业停止销毁时，需要判断插件状态是否是PLUGIN_STOPREADY状态
	 */
	public static final int PLUGIN_STOPREADY = 3;

    /**
     * 插件准备停止条件具备，可以停止,但是不能作为数据采集退出的标记
     * 是所有tran执行完成后自动设置的状态标记，说明次状态可以退出同步作业
     * 作业停止销毁时，需要判断插件状态是否是PLUGIN_STOPREADY状态
     */
    public static final int PLUGIN_STOPAPPENDING_STOPREADY = 6;


    /**
     * 插件初始状态
     */
    public static final int PLUGIN_INIT = -1;

	/**
	 * 插件Tran启动
	 */
	public static final int PLUGIN_TRAN_START = 0;

	/**
	 * 插件停止
	 */
	public static final int PLUGIN_STOPPED = 5;

    /**
     * 正常停止
     */
	public static final int STATUS_STOP_NORMAL = 1;
    /**
     * 异常停止
     */
    public static final int STATUS_STOP_EXCEPTION = 3;
	public static final int STATUS_STOPTRANONLY = 2;
}
