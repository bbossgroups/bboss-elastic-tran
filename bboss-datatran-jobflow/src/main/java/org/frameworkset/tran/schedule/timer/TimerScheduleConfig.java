package org.frameworkset.tran.schedule.timer;
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


import org.frameworkset.tran.schedule.ScheduleConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/7 17:14
 * @author biaoping.yin
 * @version 1.0
 */
public class TimerScheduleConfig extends ScheduleConfig {


	/**
	 * 每天不扫码新文件时间段，如果没有定义扫描段
	 */
	private List<TimeRange> skipScanNewFileTimeRanges;
	/**
	 * 每天扫描新文件时间段，优先级高于不扫码时间段，先计算是否在扫描时间段，如果是则扫描，不是则不扫码
	 */
	private List<TimeRange> scanNewFileTimeRanges;

	public List<TimeRange> getSkipScanNewFileTimeRanges() {
		return skipScanNewFileTimeRanges;
	}

	public void setSkipScanNewFileTimeRanges(List<TimeRange> skipScanNewFileTimeRanges) {
		this.skipScanNewFileTimeRanges = skipScanNewFileTimeRanges;
	}

	public List<TimeRange> getScanNewFileTimeRanges() {
		return scanNewFileTimeRanges;
	}

	public void setScanNewFileTimeRanges(List<TimeRange> scanNewFileTimeRanges) {
		this.scanNewFileTimeRanges = scanNewFileTimeRanges;
	}

	/**
	 * 添加不扫码新文件的时间段
	 * timeRange必须是以下三种类型格式
	 * 11:30-12:30  每天在11:30和12:30之间运行
	 * 11:30-    每天11:30开始执行,到23:59结束
	 * -12:30    每天从00:00开始到12:30
	 * @param timeRange
	 * @return
	 */
	public TimerScheduleConfig addSkipScanNewFileTimeRange(String timeRange){
		if(skipScanNewFileTimeRanges == null){
			skipScanNewFileTimeRanges = new ArrayList<>();
		}
		TimeRange skipTimeRange = TimeUtil.parserTimeRange(  timeRange);
		if(skipTimeRange != null)
			skipScanNewFileTimeRanges.add(skipTimeRange);
		return this;
	}

	/**
	 * 添加扫码新文件的时间段，每天扫描新文件时间段，优先级高于不扫码时间段，先计算是否在扫描时间段，如果是则扫描，不是则不扫码
	 * timeRange必须是以下三种类型格式
	 * 11:30-12:30  每天在11:30和12:30之间运行
	 * 11:30-    每天11:30开始执行,到23:59结束
	 * -12:30    每天从00:00开始到12:30
	 * @param timeRange
	 * @return
	 */
	public TimerScheduleConfig addScanNewFileTimeRange(String timeRange){
		if(scanNewFileTimeRanges == null){
			scanNewFileTimeRanges = new ArrayList<>();
		}

		TimeRange includeTimeRange = TimeUtil.parserTimeRange(  timeRange);
		if(includeTimeRange != null)
			scanNewFileTimeRanges.add(includeTimeRange);

		return this;
	}

}
