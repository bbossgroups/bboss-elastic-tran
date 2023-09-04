package org.frameworkset.tran.metrics.job;
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

import java.text.DateFormat;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2023/2/13
 * @author biaoping.yin
 * @version 1.0
 */
public class BuildMapDataContext {
	private Date currentTime;
	private DateFormat yearFormat;
	private DateFormat monthFormat ;
	private DateFormat weekFormat;
	private DateFormat dayFormat;
	private DateFormat hourFormat;
	private DateFormat minuteFormat;
    private DateFormat secondFormat;



	private String dataTimeField;
    private Integer timeWindowType;
	public BuildMapDataContext(){
		yearFormat = MetricsConfig.getYearFormat();
		monthFormat = MetricsConfig.getMonthFormat();
		weekFormat = MetricsConfig.getWeekFormat();
		dayFormat = MetricsConfig.getDayFormat();
		hourFormat = MetricsConfig.getHourFormat();
		minuteFormat = MetricsConfig.getMinuteFormat();
        secondFormat = MetricsConfig.getSecondFormat();
		currentTime = new Date();
	}

    public void setSecondFormat(DateFormat secondFormat) {
        this.secondFormat = secondFormat;
    }

    public DateFormat getSecondFormat() {
        return secondFormat;
    }

    public Date getCurrentTime() {
		return currentTime;
	}

	public void setCurrentTime(Date currentTime) {
		this.currentTime = currentTime;
	}
	public DateFormat getYearFormat() {
		return yearFormat;
	}

	public void setYearFormat(DateFormat yearFormat) {
		this.yearFormat = yearFormat;
	}

	public DateFormat getMonthFormat() {
		return monthFormat;
	}

	public void setMonthFormat(DateFormat monthFormat) {
		this.monthFormat = monthFormat;
	}

	public DateFormat getWeekFormat() {
		return weekFormat;
	}

	public void setWeekFormat(DateFormat weekFormat) {
		this.weekFormat = weekFormat;
	}

	public DateFormat getDayFormat() {
		return dayFormat;
	}

	public void setDayFormat(DateFormat dayFormat) {
		this.dayFormat = dayFormat;
	}

	public DateFormat getHourFormat() {
		return hourFormat;
	}

	public void setHourFormat(DateFormat hourFormat) {
		this.hourFormat = hourFormat;
	}

	public DateFormat getMinuteFormat() {
		return minuteFormat;
	}

	public void setMinuteFormat(DateFormat minuteFormat) {
		this.minuteFormat = minuteFormat;
	}


	public String getDataTimeField() {
		return dataTimeField;
	}

	/**
	 * 设置指标维度字段，不是设置默认采用当前时间，否则采用字段对应的时间值
	 * @param dataTimeField
	 */
	public void setDataTimeField(String dataTimeField) {
		this.dataTimeField = dataTimeField;
	}

    public Integer getTimeWindowType() {
        return this.timeWindowType;
    }

    public void setTimeWindowType(int timeWindowType) {
        this.timeWindowType = timeWindowType;
    }
}
