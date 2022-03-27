package org.frameworkset.tran.metrics;
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

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * <p>Description: 作业被拆分为多个任务，每个任务执行监控指标数据</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/4 17:29
 * @author biaoping.yin
 * @version 1.0
 */
public class TaskMetrics {
	private Date jobStartTime;
	private Date taskStartTime;
	private Date taskEndTime;
	private long totalRecords;
	private long totalFailedRecords;
	private long totalIgnoreRecords;
	private long totalSuccessRecords;
	private long successRecords;
	private long failedRecords;
	private long ignoreRecords;
	private long records;
	private int taskNo;
	private String jobNo;

	public long getTotalRecords() {
		return totalRecords;
	}

	public void setTotalRecords(long totalRecords) {
		this.totalRecords = totalRecords;
	}

	public long getSuccessRecords() {
		return successRecords;
	}

	public void setSuccessRecords(long successRecords) {
		this.successRecords = successRecords;
	}

	public long getTotalFailedRecords() {
		return totalFailedRecords;
	}

	public void setTotalFailedRecords(long totalFailedRecords) {
		this.totalFailedRecords = totalFailedRecords;
	}

	public long getTotalIgnoreRecords() {
		return totalIgnoreRecords;
	}

	public void setTotalIgnoreRecords(long totalIgnoreRecords) {
		this.totalIgnoreRecords = totalIgnoreRecords;
	}

	public long getFailedRecords() {
		return failedRecords;
	}

	public void setFailedRecords(long failedRecords) {
		this.failedRecords = failedRecords;
	}

	public long getIgnoreRecords() {
		return ignoreRecords;
	}

	public void setIgnoreRecords(long ignoreRecords) {
		this.ignoreRecords = ignoreRecords;
	}


	public int getTaskNo() {
		return taskNo;
	}

	public void setTaskNo(int taskNo) {
		this.taskNo = taskNo;
	}

	public String getJobNo() {
		return jobNo;
	}

	public void setJobNo(String jobNo) {
		this.jobNo = jobNo;
	}
	public long getTotalSuccessRecords() {
		return totalSuccessRecords;
	}

	public void setTotalSuccessRecords(long totalSuccessRecords) {
		this.totalSuccessRecords = totalSuccessRecords;
	}

	public Date getJobStartTime() {
		return jobStartTime;
	}

	public void setJobStartTime(Date jobStartTime) {
		this.jobStartTime = jobStartTime;
	}

	public Date getTaskStartTime() {
		return taskStartTime;
	}

	public void setTaskStartTime(Date taskStartTime) {
		this.taskStartTime = taskStartTime;
	}

	public Date getTaskEndTime() {
		return taskEndTime;
	}

	public void setTaskEndTime(Date taskEndTime) {
		this.taskEndTime = taskEndTime;
	}
	public String toString(){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		StringBuilder builder = new StringBuilder();
		builder.append("{taskNo:").append(taskNo)
				.append(", jobNo:").append(jobNo);
		if(jobStartTime != null) {
			builder.append(", jobStartTime:").append(dateFormat.format(jobStartTime));
		}
		if(taskStartTime != null) {
			builder.append(", taskStartTime:").append(dateFormat.format(taskStartTime));
		}
		if(taskEndTime != null) {
			builder.append(", taskEndTime:").append(dateFormat.format(taskEndTime));
		}
		builder.append(", totalRecords:").append(totalRecords)
				.append(", totalSuccessRecords:").append(totalSuccessRecords)
				.append(", totalIgnoreRecords:").append(totalIgnoreRecords)
				.append(", totalFailedRecords:").append(totalFailedRecords)
				.append(", successRecords:").append(successRecords)
//				.append(", ignoreRecords:").append(ignoreRecords)
				.append(", failedRecords:").append(failedRecords)
				.append(", elapsedTime:").append(getElapsed()).append("ms}");
		return builder.toString();
	}
	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	public long getElapsed(){
		if (getTaskStartTime() != null && getTaskEndTime() != null)
			return getTaskEndTime().getTime() - getTaskStartTime().getTime();
		return -1;
	}
	public long getRecords() {
		return records;
	}

	public void setRecords(long records) {
		this.records = records;
	}
}
