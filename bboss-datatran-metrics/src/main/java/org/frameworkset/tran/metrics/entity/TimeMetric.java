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

/**
 * <p>Description: 基于时间维度的指标统计计算</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/5/6 10:14
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class TimeMetric extends KeyMetric {

	protected String metricTimeKey;
	protected String metricSlotTimeKey;
    protected String second;
	protected String minute;
	protected String dayHour;
	protected String day;
	protected String week;
	protected String month;
	protected String year;
	/**
	 * 独立ip数量
	 */
	protected long ips;

	/**
	 * 独立用户数
	 */
	protected long pv;
	/**
	 * 总耗时
	 */
	protected float totalElapsed;
	final public float getTotalElapsed() {
		return totalElapsed;
	}

	final public void setTotalElapsed(float totalElapsed) {
		this.totalElapsed = totalElapsed;
	}

	final public long getIps() {
		return ips;
	}

	final public void setIps(long ips) {
		this.ips = ips;
	}

	final public long getPv() {
		return pv;
	}

	final public void setPv(long pv) {
		this.pv = pv;
	}
	public String getWeek() {
		return week;
	}

	public void setWeek(String week) {
		this.week = week;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public void setMetric(String metric) {
		this.metric = metric;
	}

	public String getMinute() {
		return minute;
	}

	public void setMinute(String minute) {
		this.minute = minute;
	}

	public String getDayHour() {
		return dayHour;
	}

	public void setDayHour(String dayHour) {
		this.dayHour = dayHour;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public void setMetricTimeKey(String metricTimeKey) {
		this.metricTimeKey = metricTimeKey;
	}

	public String getMetricTimeKey() {
		return metricTimeKey;
	}

	public String getMetricSlotTimeKey() {
		return metricSlotTimeKey;
	}

	public void setMetricSlotTimeKey(String metricSlotTimeKey) {
		this.metricSlotTimeKey = metricSlotTimeKey;
	}
    public String getSecond() {
        return second;
    }

    public void setSecond(String second) {
        this.second = second;
    }
}
