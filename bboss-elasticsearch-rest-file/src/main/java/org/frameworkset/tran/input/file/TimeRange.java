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

import org.frameworkset.tran.file.util.TimeUtil;

/**
 * <p>Description: 定义扫描文件时间段</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/30 14:37
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeRange {

	/**
	 * 时间段开始时间
	 * 配置样例 11:30
	 */
	private String startTime;
	/**
	 * 时间段结束时间
	 * 配置样例 12:30
	 */
	private String endTime;
	private int startHour;
	private int startMin;
	private int endHour;
	private int endMin;
	public void parser(){
		int[] itime = TimeUtil.parserTime( startTime);
		startHour = itime[0];
		startMin = itime[1];

		itime = TimeUtil.parserTime( endTime);
		endHour = itime[0];
		endMin = itime[1];
		if(endHour < startHour)
			throw new IllegalArgumentException("结束时间"+endTime+"不能早于开始时间"+startTime);

		if(endHour == startHour){
			if(startMin > endMin)
				throw new IllegalArgumentException("结束时间"+endTime+"不能早于开始时间"+startTime);
		}


	}


	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public void setEndTime(String endTime) {
		this.endTime = endTime;
	}

	public String getStartTime() {
		return startTime;
	}

	public String getEndTime() {
		return endTime;
	}

	public int getStartHour() {
		return startHour;
	}

	public int getStartMin() {
		return startMin;
	}

	public int getEndHour() {
		return endHour;
	}

	public int getEndMin() {
		return endMin;
	}
}
