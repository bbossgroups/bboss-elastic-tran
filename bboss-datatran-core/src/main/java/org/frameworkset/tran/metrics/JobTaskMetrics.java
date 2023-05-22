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

import org.frameworkset.tran.status.BaseStatusManager;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

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
	protected long totalRecords;
	private long totalFailedRecords;
	private long totalIgnoreRecords;
	private long totalSuccessRecords;
	protected int tasks;


	/**
	 * 进入错误方法的任务数
	 */
	protected volatile int errorTasks;
	/**
	 * 进入异常方法的任务数
	 */
	protected volatile int exceptionTasks;
	private String jobNo;
	private Object lastValue;
	private String jobId;
	private String jobName;
	private Map<String,Object> jobExecutorDatas;
	private ReentrantLock tasksLock = new ReentrantLock();
	private ReentrantLock errtasksLock = new ReentrantLock();
	private ReentrantLock exceptiontasksLock = new ReentrantLock();
	public JobTaskMetrics putJobExecutorData(String name, Object value){
		if(jobExecutorDatas == null){
			jobExecutorDatas = new LinkedHashMap<>();
		}
		jobExecutorDatas.put(name,value);
		return this;
	}

	public Object readJobExecutorData(String name){
		if(jobExecutorDatas != null){
			return jobExecutorDatas.get(name);
		}
		return null;


	}

	public Map<String, Object> getJobExecutorDatas() {
		return jobExecutorDatas;
	}

	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

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

	public void await(){

	}

	public void await(long waitime){

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
		tasksLock.lock();
		try {
			tasks++;
			return tasks;
		}
		finally {
			tasksLock.unlock();
		}
	}

	public int increamentErrorTasks() {
		errtasksLock.lock();
		try {
			errorTasks++;
			return errorTasks;
		}
		finally {
			errtasksLock.unlock();
		}

	}

	public int increamentExceptionTasks() {
		exceptiontasksLock.lock();
		try {
			exceptionTasks++;
			return exceptionTasks;
		}
		finally {
			exceptiontasksLock.unlock();
		}
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
		buildString( builder);
		return builder.toString();

	}
	protected void buildString(StringBuilder builder){
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		builder.append("JobId:").append(jobId);
		builder.append(",JobName:").append(jobName);
		if(jobNo != null)
			builder.append(",JobNo:").append(jobNo);
		else
			builder.append(",JobNo:-");
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
		builder.append(",Total lastValue:").append(lastValue);
		builder.append(",Elapsed Time:").append(getElapsed()).append("ms");
	}
	/**
	 * 获取任务执行耗时
	 * -1 表示没有执行耗时
	 * @return
	 */
	public long getElapsed(){
		if (getJobStartTime() != null && getJobEndTime() != null)
			return getJobEndTime().getTime() - getJobStartTime().getTime();
		return -1;
	}

	public Object getLastValue() {
		return lastValue;
	}

	public void setLastValue(Object lastValue) {
		this.lastValue = lastValue;
	}
	public void putLastValue(Integer lastValueType,Object lastValue){
		this.lastValue = BaseStatusManager.max(lastValueType,this.lastValue,lastValue);
	}
	public int getErrorTasks() {
		return errorTasks;
	}

	public void setErrorTasks(int errorTasks) {
		this.errorTasks = errorTasks;
	}

	public int getExceptionTasks() {
		return exceptionTasks;
	}

	public void setExceptionTasks(int exceptionTasks) {
		this.exceptionTasks = exceptionTasks;
	}
}
