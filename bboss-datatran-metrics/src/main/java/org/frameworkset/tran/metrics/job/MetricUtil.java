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

import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;

import java.text.DateFormat;
import java.util.Date;
import java.util.Map;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/9/21 11:54
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class MetricUtil {
	public static String handleChannelId(Map data){
//渠道编码
		String chanId = (String) data.get("chanId");
		if(chanId == null || "null".equals(chanId) || "".equals(chanId)){
			return "其他";
		}
		return chanId;
	}

    public static DateFormat getMetricsTimeKeyFormat(int timeWindowType,MapData data){
        if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MINUTE)
            return data.getMinuteFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_SECOND)
            return data.getSecondFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_HOUR)
            return data.getHourFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_DAY)
            return data.getDayFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_WEEK)
            return data.getWeekFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_MONTH)
            return data.getMonthFormat();
        else if(timeWindowType == MetricsConfig.TIME_WINDOW_TYPE_YEAR)
            return data.getYearFormat();
        return data.getMinuteFormat();
    }

    public static void buildMetricTimeField(TimeMetric metric, MapData data, Date time){
        DateFormat dateFormat = data.getDayFormat();
        if(dateFormat != null) {
            String day = dateFormat.format(time);
            metric.setDay(day);


        }
        dateFormat = data.getHourFormat();
        if(dateFormat != null) {
            String hour = dateFormat.format(time);
            metric.setDayHour(hour);
        }
        if(dateFormat != null) {
            dateFormat = data.getMinuteFormat();
            String min = dateFormat.format(time);
            metric.setMinute(min);
        }
        dateFormat = data.getSecondFormat();
        if(dateFormat != null) {
            String second = dateFormat.format(time);
            metric.setSecond(second);
        }
        dateFormat = data.getMonthFormat();
        if(dateFormat != null) {
            String month = dateFormat.format(time);
            metric.setMonth(month);
        }
        dateFormat = data.getWeekFormat();
        if(dateFormat != null) {
            String week = dateFormat.format(time);
            metric.setWeek(week);
        }
        dateFormat = data.getYearFormat();
        if(dateFormat != null) {
            String year = dateFormat.format(time);
            metric.setYear(year);
        }

    }
}
