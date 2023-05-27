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

import com.frameworkset.util.SimpleStringUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.*;

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

    /**
     * 将字符串类型的日期转换为LocalDateTime类型数据
     *  Stream.of("2015-05-09T00:10:23.934596635Z",
     *                 "2015-05-09 00:10:23.123456789UTC",
     *                 "2015/05/09 00:10:23.123456789",
     *                 "2015-05-09 00:10:23.12345678",
     *                 "2015/05/09 00:10:23.1234567",
     *                 "2015-05-09T00:10:23.123456",
     *                 "2015-05-09 00:10:23.12345",
     *                 "2015/05-09T00:10:23.1234",
     *                 "2015-05-09 00:10:23.123",
     *                 "2015-05-09 00:10:23.12",
     *                 "2015-05-09 00:10:23.1",
     *                 "2015-05-09 00:10:23",
     *                 "2015-05-09 00:10",
     *                 "2015-05-09 01",
     *                 "2015-05-09"
     *         ).forEach(s -> {
     *             LocalDateTime date = LocalDateTime.parse(s, dateTimeFormatter);
     *             System.out.println(s + " localdate==> " + date);
     *
     *             System.out.println(s + " date==> " + par(s));
     *         });
     * @param localDateTime
     * @return
     */

    public static LocalDateTime localDateTime(String localDateTime){


        DateTimeFormatter dateTimeFormatter = getDateTimeFormatter();
        LocalDateTime date = LocalDateTime.parse(localDateTime, dateTimeFormatter);
        return date;
    }
    public static LocalDateTime date2LocalDateTime(Date date){
        Instant instant = date.toInstant();
        ZoneId zoneId = ZoneId.systemDefault();

        LocalDateTime localDateTime = instant.atZone(zoneId).toLocalDateTime();
        return localDateTime;
    }
    private static DateTimeFormatter dateTimeFormatter;
    private static DateTimeFormatter dateTimeFormatterDefault = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSSSS'Z'");
    private static Map<String,DateTimeFormatter> dateTimeFormatterMap = new LinkedHashMap<>();
    private static Object lock = new Object();
    public static DateTimeFormatter getDateTimeFormatter(){
        if(dateTimeFormatter != null){
            return dateTimeFormatter;
        }
        synchronized (lock) {
            if(dateTimeFormatter != null){
                return dateTimeFormatter;
            }
            DateTimeFormatter ISO_LOCAL_DATE = new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
                    .optionalStart().appendLiteral('/').optionalEnd()
                    .optionalStart().appendLiteral('-').optionalEnd()
                    .optionalStart().appendValue(ChronoField.MONTH_OF_YEAR, 2)
                    .optionalStart().appendLiteral('/').optionalEnd()
                    .optionalStart().appendLiteral('-').optionalEnd()
                    .optionalStart().appendValue(ChronoField.DAY_OF_MONTH, 2)
                    .toFormatter();

            DateTimeFormatter ISO_LOCAL_TIME = new DateTimeFormatterBuilder()
                    .appendValue(ChronoField.HOUR_OF_DAY, 2)
                    .optionalStart().appendLiteral(':').appendValue(ChronoField.MINUTE_OF_HOUR, 2)
                    .optionalStart().appendLiteral(':').appendValue(ChronoField.SECOND_OF_MINUTE, 2)
                    .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .optionalStart().appendZoneId()
                    .toFormatter();

            DateTimeFormatter dateTimeFormatter_ = new DateTimeFormatterBuilder()
                    .append(ISO_LOCAL_DATE)
                    .optionalStart().appendLiteral(' ').optionalEnd()
                    .optionalStart().appendLiteral('T').optionalEnd()
                    .optionalStart().appendOptional(ISO_LOCAL_TIME).optionalEnd()
                    .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                    .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
                    .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
                    .toFormatter(Locale.SIMPLIFIED_CHINESE);
            dateTimeFormatter = dateTimeFormatter_;
            return dateTimeFormatter;
        }
    }
    public static String changeLocalDateTime2String(LocalDateTime localDateTime){


//        DateTimeFormatter dateTimeFormatter = getDateTimeFormatter();


        return localDateTime.format(dateTimeFormatterDefault);
    }

    public static DateTimeFormatter getDateTimeFormatter(String dateFormat){
        DateTimeFormatter dateTimeFormatter = dateTimeFormatterMap.get(dateFormat);
        if(dateTimeFormatter != null){
            return dateTimeFormatter;
        }
        synchronized (dateTimeFormatterMap){
            dateTimeFormatter = dateTimeFormatterMap.get(dateFormat);
            if(dateTimeFormatter != null){
                return dateTimeFormatter;
            }
            dateTimeFormatter = DateTimeFormatter.ofPattern(dateFormat);
            dateTimeFormatterMap.put(dateFormat,dateTimeFormatter);
            return dateTimeFormatter;
        }
    }
    public static String changeLocalDateTime2String(LocalDateTime localDateTime,String dateFormat){


//        DateTimeFormatter dateTimeFormatter = getDateTimeFormatter();

        if(SimpleStringUtil.isNotEmpty(dateFormat))
            return localDateTime.format(getDateTimeFormatter( dateFormat));
        else{
            return localDateTime.format(dateTimeFormatterDefault);
        }
    }

	public static Date convertLocalDate(LocalDate localDate){

		if (null == localDate) {
			return null;
		}
		ZonedDateTime zonedDateTime = localDate.atStartOfDay(ZoneId.systemDefault());
		return Date.from(zonedDateTime.toInstant());
	}

	public static Object convertLocalDate(Object localDate){

        return localDate;
        /**
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
         */
	}

	public static Date parserDate(String pattern,String date) throws TimeException {
		SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
		try {
			return dateFormat.parse(date);
		} catch (ParseException e) {
			throw new TimeException("ParserDate(pattern="+pattern+",date="+date+") failed:",e);
		}
	}

}
