package org.frameworkset.tran.metrics.entity;
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

import java.text.DateFormat;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2020/6/2 14:19
 * @author biaoping.yin
 * @version 1.0
 */
public class MapData<T> {
	protected T data;
	protected MapContext mapContext;

	protected Date dataTime;
	protected DateFormat dayFormat ;

	protected DateFormat hourFormat ;
	protected DateFormat minuteFormat ;

	public DateFormat getWeekFormat() {
		return weekFormat;
	}

	public void setWeekFormat(DateFormat weekFormat) {
		this.weekFormat = weekFormat;
	}

	protected DateFormat weekFormat ;
	protected DateFormat monthFormat ;
	protected DateFormat yearFormat ;

	public DateFormat getSecondFormat() {
		return secondFormat;
	}

	public DateFormat getMonthFormat() {
		return monthFormat;
	}

	public void setMonthFormat(DateFormat monthFormat) {
		this.monthFormat = monthFormat;
	}

	public DateFormat getYearFormat() {
		return yearFormat;
	}

	public void setYearFormat(DateFormat yearFormat) {
		this.yearFormat = yearFormat;
	}

	public void setSecondFormat(DateFormat secondFormat) {
		this.secondFormat = secondFormat;
	}

	protected DateFormat secondFormat ;

//	public List<ExceptionInfo> getSpecialExceptions() {
//		return specialExceptions;
//	}
//
//	public void setSpecialExceptions(List<ExceptionInfo> specialExceptions) {
//		this.specialExceptions = specialExceptions;
//	}


	public T getData() {
		return data;
	}

	public void setData(T data) {
		this.data = data;
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

	/**
	 * 根据指标标识，获取指标的时间统计维度字段，默认返回dataTime字段值，不同的指标需要指定不同的时间维度统计字段
	 * 分析处理作业可以覆盖本方法，自定义获取时间维度字段值
	 * @param metricsKey
	 * @return
	 */
	public Date metricsDataTime(String metricsKey) {
		return dataTime;
	}

	public Date getDataTime() {
		return dataTime;
	}

	public void setDataTime(Date dataTime) {
		this.dataTime = dataTime;
	}


	public void setMapContext(MapContext mapContext) {
		this.mapContext = mapContext;
	}

	public MapContext getMapContext() {
		return mapContext;
	}


}
