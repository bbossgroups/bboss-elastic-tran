package org.frameworkset.tran.schedule;
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

/**
 * <p>Description: 定时调度执行作业时，检查是否需要暂停或者急需调度执行本次作业，对kafka消费插件无效</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/5/7
 * @author biaoping.yin
 * @version 1.0
 */
public interface ScheduleAssert {
	/**
	 * 定时调度执行作业时，检查是否需要执行本次作业,如果返回true，
	 * 则继续执行本次调度作业，false，则忽略本次作业，等待下一作业调度周期
	 * @return
	 */
	boolean assertSchedule(boolean autoPause);
	/**
	 * 暂停调度
	 * 如果暂停成功则返回true，否则返回false
	 */
	public  boolean pauseSchedule();

	/**
	 * 继续调度
	 * 如果调度成功，则返回true，否则返回false
	 */
	public  boolean resumeSchedule();
}
