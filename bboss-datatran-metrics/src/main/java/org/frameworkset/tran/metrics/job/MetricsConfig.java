package org.frameworkset.tran.metrics.job;
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

import org.frameworkset.util.annotations.DateFormateMeta;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/6/1 9:09
 * @author biaoping.yin
 * @version 1.0
 */
public class MetricsConfig {
    public static final long DEFAULT_metricsInterval =10 * 60 * 1000L;
	public final static int TIME_WINDOW_TYPE_SECOND = 1;//秒时间窗口
	public final static int TIME_WINDOW_TYPE_MINUTE = 2;//分钟时间窗口，默认值
	public final static int TIME_WINDOW_TYPE_HOUR = 3;//小时时间窗口
	public final static int TIME_WINDOW_TYPE_DAY = 4;//天时间窗口
	public final static int TIME_WINDOW_TYPE_WEEK = 5;//周时间窗口
	public final static int TIME_WINDOW_TYPE_MONTH = 6;//月时间窗口
    public final static int TIME_WINDOW_TYPE_YEAR = 7;//年时间窗口
	public static String dayDateFormatStr = "yyyyMMdd";
	public static String monthDateFormatStr = "yyyyMM";
	public static String yearDateFormatStr = "yyyy";
	private final static DateFormateMeta dateFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMddHHmm");
	private final static DateFormateMeta yearFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy");
	private final static DateFormateMeta monthFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMM");
	private final static DateFormateMeta zhCnMonthFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy年MM月");
	private final static DateFormateMeta mmFormatMeta = DateFormateMeta.buildDateFormateMeta("MM");
	private final static DateFormateMeta hh00FormatMeta = DateFormateMeta.buildDateFormateMeta("HH:00");
	private final static DateFormateMeta zhCnFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy-MM-dd HH:mm:ss");
	private final static DateFormateMeta wFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMW");

	private final static DateFormateMeta dayFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMdd");
	private final static DateFormateMeta zhCnDayFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyy年MM月dd日");
	private final static DateFormateMeta hourFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMddHH");
	private final static DateFormateMeta minuteFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMddHHmm");
	private final static DateFormateMeta secondFormateMeta = DateFormateMeta.buildDateFormateMeta("yyyyMMddHHmmss");

	private final static DateFormateMeta utcFormatMetaWithoutTimeZone = DateFormateMeta.buildDateFormateMeta("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private final static DateFormateMeta utcFormatMeta = DateFormateMeta.buildDateFormateMeta("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",null,"Etc/UTC");
	private final static DateFormateMeta monthIndiceFormatMeta = DateFormateMeta.buildDateFormateMeta("yyyy.MM");
	private final static DateFormateMeta dayIndiceFormatMeta = DateFormateMeta.buildDateFormateMeta("yyyy.MM.dd");
	public static DateFormat getDayIndiceFormat(){
		return dayIndiceFormatMeta.toDateFormat();
	}
	public static DateFormat getZhCnFormat(){
		return zhCnFormateMeta.toDateFormat();
	}
	public static DateFormat getMinuteFormateMeta(){
		return minuteFormateMeta.toDateFormat();
	}
	public static DateFormat getSecondFormateMeta(){
		return secondFormateMeta.toDateFormat();
	}

	public static DateFormat getMinuteFormat(){
		return dateFormateMeta.toDateFormat();
	}
	public static DateFormat getYearFormat(){
		return yearFormateMeta.toDateFormat();
	}
	public static DateFormat getWeekFormat(){
		return wFormateMeta.toDateFormat();
	}
	public static DateFormat getMonthFormat(){
		return monthFormateMeta.toDateFormat();
	}
	public static DateFormat getMonthIndiceFormat(){
		return monthIndiceFormatMeta.toDateFormat();
	}

	public static DateFormat getUTCFormat(){
		return utcFormatMeta.toDateFormat();
	}
	public static DateFormat getUTCFormatWithoutTimeZone(){
		return utcFormatMetaWithoutTimeZone.toDateFormat();
	}
	public static DateFormat getDayFormat(){
		return dayFormateMeta.toDateFormat();
	}

	public static DateFormat getMmFormatMeta() {
		return mmFormatMeta.toDateFormat();
	}

	public static DateFormat getHh00Format() {
		return hh00FormatMeta.toDateFormat();
	}

	public static DateFormat getZhCnMonthFormat() {
		return zhCnMonthFormateMeta.toDateFormat();
	}

	public static DateFormat getZhCnDayFormat() {
		return zhCnDayFormateMeta.toDateFormat();
	}

	/**
	 * 日期格式转换yyyy-MM-dd'T'HH:mm:ss.SSSXXX  (yyyy-MM-dd'T'HH:mm:ss.SSSZ) TO  yyyy-MM-dd HH:mm:ss
	 * @throws ParseException
	 */
	public static Date dealDateFormat(DateFormat utcdf,String oldDateStr) throws ParseException{
		if(oldDateStr == null || "".equals(oldDateStr)){
			return  null;
		}
		Date  date = utcdf.parse(oldDateStr);
		return  date;
	}
	public static DateFormat getHourFormat(){
		return hourFormateMeta.toDateFormat();
	}
	/**
	 * 单位：毫秒，扫描入库时间间隔
	 */
	private long scanInterval;
	/**
	 * 移动时间窗口：窗口内的数据驻留内存进行实时计算，超过时间窗口的
	 * 单位：秒
	 */
	private int timeWindows;

	public long getScanInterval() {
		return scanInterval;
	}

	public void setScanInterval(long scanInterval) {
		this.scanInterval = scanInterval;
	}

	public int getTimeWindows() {
		return timeWindows;
	}

	public void setTimeWindows(int timeWindows) {
		this.timeWindows = timeWindows;
	}

	public static void main(String[] args) throws ParseException {
		Date date = dateFormateMeta.toDateFormat().parse("202006031055");
		System.out.println(date);
	}



}
