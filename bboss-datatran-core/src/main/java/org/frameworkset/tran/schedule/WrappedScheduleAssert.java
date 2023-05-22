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
 * <p>Description: 作业暂停、继续控制信号器</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/5/7
 * @author biaoping.yin
 * @version 1.0
 */
public class WrappedScheduleAssert implements ScheduleAssert {
	protected ScheduleAssert scheduleAssert;
	public WrappedScheduleAssert(ScheduleAssert scheduleAssert){
		this.scheduleAssert = scheduleAssert;
	}

	/**
	 * 自动暂停对单文件FileConfig线程自动调度有效，对多文件FileConfig线程自动调度无效，需要手动进行暂停才有效
	 * 外部多文件扫描调度自动暂停对单文件FileConfig和多文件FileConfig都有效
	 * @param autoPause
	 * @return
	 */
	@Override
	public synchronized boolean assertSchedule(boolean autoPause) {
			return scheduleAssert.assertSchedule(autoPause);
	}

	/**
	 * 暂停调度
	 */
	public synchronized boolean pauseSchedule(){
		return scheduleAssert.pauseSchedule();

	}

	/**
	 * 继续调度
	 */
	public synchronized boolean resumeSchedule(){
		return scheduleAssert.resumeSchedule();
	}
}
