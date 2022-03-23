package org.frameworkset.tran.metrics;
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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: 作业定时执行时的监控指标数据</p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/22
 * @author biaoping.yin
 * @version 1.0
 */
public class JobTaskMetrics {
	private Date jobStartTime;
	private Date jobEndTime;
	private long totalRecords;
	private long totalFailedRecords;
	private long totalIgnoreRecords;
	private long totalSuccessRecords;
	private int tasks;
	private String jobNo;

	public Date getJobStartTime() {
		return jobStartTime;
	}

	public void setJobStartTime(Date jobStartTime) {
		this.jobStartTime = jobStartTime;
	}

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public void increamentRecords(long records){
		totalRecords = totalRecords + records;
	}

	public long getTotalFailedRecords() {
		return totalFailedRecords;
	}
	public void increamentFailedRecords(long failedRecords){
		totalFailedRecords = totalFailedRecords + failedRecords;
	}
	public void setTotalFailedRecords(long totalFailedRecords) {
		this.totalFailedRecords = totalFailedRecords;
	}

	public long getTotalIgnoreRecords() {
		return totalIgnoreRecords;
	}
	public void increamentIgnoreRecords(long ignoreRecords){
		totalIgnoreRecords = totalIgnoreRecords + ignoreRecords;
	}
	public void setTotalIgnoreRecords(long totalIgnoreRecords) {
		this.totalIgnoreRecords = totalIgnoreRecords;
	}

	public long getTotalSuccessRecords() {
		return totalSuccessRecords;
	}
	public void increamentSuccessRecords(long successRecords){
		totalSuccessRecords = totalSuccessRecords + successRecords;
	}
	public void setTotalSuccessRecords(long totalSuccessRecords) {
		this.totalSuccessRecords = totalSuccessRecords;
	}

	public int getTasks() {
		return tasks;
	}

	public void setTasks(int tasks) {
		this.tasks = tasks;
	}

	public String getJobNo() {
		return jobNo;
	}

	public void setJobNo(String jobNo) {
		this.jobNo = jobNo;
	}

	public int increamentTasks() {
		tasks ++;
		return tasks;
	}

	public Date getJobEndTime() {
		return jobEndTime;
	}

	public void setJobEndTime(Date jobEndTime) {
		this.jobEndTime = jobEndTime;
	}
	@Override
	public String toString(){
		StringBuilder builder = new StringBuilder();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		builder.append("JobNo:").append(jobNo);
		if(jobStartTime != null)
			builder.append(",JobStartTime:").append(dateFormat.format(jobStartTime));
		else
			builder.append(",JobStartTime:-");
		if(jobEndTime != null)
			builder.append(",JobEndTime:").append(dateFormat.format(jobEndTime));
		else
			builder.append(",JobEndTime:-");
		builder.append(",Total Records:").append(totalRecords);
		builder.append(",Total Success Records:").append(totalSuccessRecords);
		builder.append(",Total Failed Records:").append(totalFailedRecords);
		builder.append(",Total Ignore Records:").append(totalIgnoreRecords);
		builder.append(",Total Tasks:").append(tasks);
		return builder.toString();

	}
}
