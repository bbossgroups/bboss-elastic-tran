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
 * <p>Description: 作业启动后持续运行，需要人工手动暂停才能暂停作业，当暂停后需执行resume作业才能继续调度执行</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/5/7
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultScheduleAssert implements ScheduleAssert {
	protected boolean paused = false;

	/**
	 * 自动暂停对单文件FileConfig线程自动调度有效，对多文件FileConfig线程自动调度无效，需要手动进行暂停才有效
	 * 外部多文件扫描调度自动暂停对单文件FileConfig和多文件FileConfig都有效
	 * @param autoPause
	 * @return
	 */
	@Override
	public boolean assertSchedule(boolean autoPause) {
		if(!checkPaused(  autoPause)) {
			return true;
		}
		else{
			return false;
		}
	}
	protected boolean checkPaused(boolean autoPause){
		if(paused){

			return true;
		}
		else{
			return false;
		}
	}

	/**
	 * 暂停调度
	 */
	public boolean pauseSchedule(){
		if(!paused) {
			this.paused = true;
			return true;
		}
		else
			return false;

	}

	/**
	 * 继续调度
	 */
	public boolean resumeSchedule(){
		if(paused) {
			this.paused = false;
			return true;
		}
		else{
			return false;
		}
	}
}
