package org.frameworkset.tran.schedule.timer;
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

import java.time.*;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/9/30 16:57
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeUtil {
	public static void main(String[] args){
		String time = "12:08";
		int[] itime = TimeUtil.parserTime( time);
		System.out.println(itime);

		Date currentTime = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentTime);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int min = calendar.get(Calendar.MINUTE);
		System.out.println(itime);

		TimerScheduleConfig timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addScanNewFileTimeRange("17:29-18:30");
		boolean result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);

		timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addScanNewFileTimeRange("18:29-18:30");
		result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);

		timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addScanNewFileTimeRange("18:29-");
		result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);

		timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addScanNewFileTimeRange("-18:29");
		result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);

		timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addSkipScanNewFileTimeRange("-18:29");
		result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);

		timerScheduleConfig = new TimerScheduleConfig();
		timerScheduleConfig.addSkipScanNewFileTimeRange("-14:29");
		result = TimeUtil.evalateNeedScan(timerScheduleConfig);
		System.out.println(result);
	}
	public static TimeRange parserTimeRange(String timeRange){
		TimeRange _timeRange = null;
		if(timeRange != null && !timeRange.equals("")){
			String[] times = timeRange.split("-");
			_timeRange = new TimeRange();
			if(times.length == 2) {
				if(times[0].equals(""))
					_timeRange.setStartTime("00:00");
				else{
					_timeRange.setStartTime(times[0]);
				}
				_timeRange.setEndTime(times[1]);
			}
			else {
				if(timeRange.endsWith("-")){
					_timeRange.setEndTime("23:59");
					_timeRange.setStartTime(timeRange.substring(0,timeRange.length() - 1));
				}
				else{
					StringBuilder msg = new StringBuilder();
					msg.append("timeRange:").append(timeRange).append("timeRange必须是以下三种类型格式\r\n")
							.append(" 11:30-12:30  每天在11:30和12:30之间运行\r\n")
							.append(" 11:30-    每天11:30开始执行,到23:59结束\r\n")
							.append(" -12:30    每天从00:00开始到12:30\r\n");
					throw new IllegalArgumentException(msg.toString());
				}
			}
			_timeRange.parser();
		}
		return _timeRange;

	}
	public static int[] parserTime(String time){
		if(time != null && !time.equals("")){
			int[] itime = new int[2];
			String[] starts = time.split(":");
			if(starts.length == 1){
				itime[0] = Integer.parseInt(starts[0]);
				itime[1] = 0;
			}
			else{
				itime[0] = Integer.parseInt(starts[0]);
				itime[1] = Integer.parseInt(starts[1]);
			}
			return itime;
		}
		return null;
	}
	private static boolean inTimeRange(TimeRange timeRange,int hour,int min){
		boolean result = false;
		if(hour == timeRange.getStartHour() && hour == timeRange.getEndHour()){
			if(min >= timeRange.getStartMin() && min <= timeRange.getEndMin())
				result = true;

		}
		else if(hour >= timeRange.getStartHour() && hour < timeRange.getEndHour()){
			result = true;
		}
		else if(hour == timeRange.getEndHour()){
			if(min <= timeRange.getEndMin())
			{
				result = true;
			}
		}
		return result;

	}

	/**
	 * 评估是否需要进行新文件扫描
	 * @param timerScheduleConfig
	 * @return
	 */
	public static boolean evalateNeedScan(TimerScheduleConfig timerScheduleConfig){
		if(timerScheduleConfig == null)
			return true;
		Date currentTime = new Date();
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(currentTime);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		int min = calendar.get(Calendar.MINUTE);
		List<TimeRange> includeTimeRanges = timerScheduleConfig.getScanNewFileTimeRanges();
		List<TimeRange> skipTimeRanges = timerScheduleConfig.getSkipScanNewFileTimeRanges();
		boolean result = false;
		if(includeTimeRanges != null && includeTimeRanges.size() > 0){
			for(TimeRange timeRange:includeTimeRanges){
				if(inTimeRange(  timeRange,  hour,  min)){
					result = true;
					break;
				}
			}

		}
		else if(skipTimeRanges != null && skipTimeRanges.size() > 0){
			result = true;
			for(TimeRange timeRange:skipTimeRanges){
				if(inTimeRange(  timeRange,  hour,  min)){
					result = false;
					break;
				}
			}
		}
		else{
			result = true;
		}
		return result;

	}
	public static Date convertLocalDatetime(LocalDateTime localDateTime){
		if (null == localDateTime) {
			return null;
		}
		ZoneId zoneId = ZoneId.systemDefault();
		ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
		Instant instant = zonedDateTime.toInstant();
		Date date = Date.from(instant);
		return date;
	}

	public static Date convertLocalDate(LocalDate localDate){

		if (null == localDate) {
			return null;
		}
		ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());
		return Date.from(zonedDateTime.toInstant());
	}

	public static Object convertLocalDate(Object localDate){

		if (null == localDate) {
			return null;
		}
		if(localDate instanceof LocalDateTime){
			return convertLocalDatetime((LocalDateTime)localDate);

		}
		else if(localDate instanceof LocalDate){
			return convertLocalDate((LocalDate)localDate);

		}
		return localDate;
	}

}
