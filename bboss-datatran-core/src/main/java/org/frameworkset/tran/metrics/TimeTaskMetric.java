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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.context.JobContext;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.TimeMetric;
import org.frameworkset.tran.schedule.ImportIncreamentConfig;
import org.frameworkset.tran.status.LastValueWrapper;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * <p>Description: 作业被拆分为多个任务，每个任务执行监控指标数据</p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/4 17:29
 * @author biaoping.yin
 * @version 1.0
 */
public class TimeTaskMetric extends TimeMetric {
	private TimeWindowExportResultHandler timeWindowExportResultHandler;


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
	private String jobId;
	private String jobName;
	private LastValueWrapper lastValue;
	private Object result;
	private JobContext jobContext;
	private ImportContext importContext;
	public TimeTaskMetric(TimeWindowExportResultHandler timeWindowExportResultHandler){
		this.timeWindowExportResultHandler = timeWindowExportResultHandler;
	}
	public String getJobId() {
		return jobId;
	}

	public ImportContext getImportContext() {
		return importContext;
	}

	public JobContext getJobContext() {
		return jobContext;
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
		if(jobNo != null) {
			builder.append("{jobNo:").append(jobNo);
		}
		else{
			builder.append("{jobNo:-");
		}
		builder.append(", taskNo:").append(taskNo);
		if(jobStartTime != null) {
			builder.append(", jobStartTime:").append(dateFormat.format(jobStartTime));
		}
		else{
			builder.append(", jobStartTime:-");
		}
		if(taskStartTime != null) {
			builder.append(", taskStartTime:").append(dateFormat.format(taskStartTime));
		}
		else{
			builder.append(", taskStartTime:-");
		}
		if(taskEndTime != null) {
			builder.append(", taskEndTime:").append(dateFormat.format(taskEndTime));
		}
		else{
			builder.append(", taskEndTime:-");
		}
		builder.append(", totalRecords:").append(totalRecords)
				.append(", totalSuccessRecords:").append(totalSuccessRecords)
				.append(", totalIgnoreRecords:").append(totalIgnoreRecords)
				.append(", totalFailedRecords:").append(totalFailedRecords)
				.append(", successRecords:").append(successRecords)
//				.append(", ignoreRecords:").append(ignoreRecords)
				.append(", failedRecords:").append(failedRecords)
				.append(", lastValue:").append(lastValue)
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



	public LastValueWrapper getLastValue() {
		return lastValue;
	}

	public void setLastValue(LastValueWrapper lastValue) {
		this.lastValue = lastValue;
	}

	@Override
	public void init(MapData firstData) {

		MetricsMapData metricsMapData = (MetricsMapData)firstData;
		TaskMetrics taskMetrics = (TaskMetrics)firstData.getData();
		jobId = taskMetrics.getJobId();
		jobNo = taskMetrics.getJobNo();
		jobName = taskMetrics.getJobName();
		this.taskStartTime = taskMetrics.getTaskStartTime();
		this.jobStartTime = taskMetrics.getJobStartTime();
		this.taskEndTime = taskMetrics.getTaskEndTime();
		this.jobContext = metricsMapData.getJobContext();
		this.importContext = metricsMapData.getImportContext();
		this.taskNo = taskMetrics.getTaskNo();
		this.lastValue = taskMetrics.getLastValue();
		this.result = metricsMapData.getResult();
	}

	@Override
	public void incr(MapData data) {

		TaskMetrics taskMetrics = (TaskMetrics)data.getData();
		MetricsMapData metricsMapData = (MetricsMapData)data;

		successRecords = taskMetrics.getSuccessRecords() + successRecords;
		failedRecords = taskMetrics.getFailedRecords() + failedRecords;
		ignoreRecords = taskMetrics.getIgnoreRecords() + ignoreRecords;
		records = taskMetrics.getRecords() + records;
		compareAndSet( taskMetrics,metricsMapData);
	}
	private void compareAndSet(TaskMetrics taskMetrics,MetricsMapData metricsMapData){
		if(this.totalRecords < taskMetrics.getTotalRecords())
			totalRecords = taskMetrics.getTotalRecords();
		if(this.totalFailedRecords < taskMetrics.getTotalFailedRecords())
			totalFailedRecords = taskMetrics.getTotalFailedRecords();
		if(this.totalIgnoreRecords < taskMetrics.getTotalIgnoreRecords())
			totalIgnoreRecords = taskMetrics.getTotalIgnoreRecords();
		if(this.totalSuccessRecords < taskMetrics.getTotalSuccessRecords())
			totalSuccessRecords = taskMetrics.getTotalSuccessRecords();

//		if(this.records < taskMetrics.getRecords())
//			records = taskMetrics.getRecords();
		boolean setResult = false;
		if(taskNo >= 0) {
			if (this.taskNo < taskMetrics.getTaskNo()) {
				taskNo = taskMetrics.getTaskNo();
				this.result = metricsMapData.getResult();

			}
			setResult = true;
		}


		if(taskEndTime != null && taskMetrics.getTaskEndTime() != null && taskEndTime.before(taskMetrics.getTaskEndTime())){
			taskEndTime = taskMetrics.getTaskEndTime();
		}

		if(taskStartTime != null && taskMetrics.getTaskStartTime() != null && taskStartTime.after(taskMetrics.getTaskStartTime())){
			taskStartTime = taskMetrics.getTaskStartTime();
			if(!setResult){
				this.result = metricsMapData.getResult();
			}
		}







		if(lastValue != null) {

			if(importContext.getLastValueType() == ImportIncreamentConfig.NUMBER_TYPE) {

//				Number ts = (Number)lastValue.getLastValue();
//				Number nts = (Number)taskMetrics.getLastValue().getLastValue();
//				if(nts.longValue() > ts.longValue())
//					this.lastValue = taskMetrics.getLastValue();
                this.lastValue = importContext.getDataTranPlugin().maxNumberLastValue(lastValue,taskMetrics.getLastValue());
				//to fixed...

			}
			else if(importContext.getLastValueType() == ImportIncreamentConfig.TIMESTAMP_TYPE) {
                Object _lastValue = lastValue.getLastValue();
				if(_lastValue instanceof Number){
					Number ts = (Number)_lastValue;
					Number nts = (Number)taskMetrics.getLastValue().getLastValue();
					if(nts.longValue() > ts.longValue())
						this.lastValue = taskMetrics.getLastValue();
				}
				else {
					Date ts = (Date)_lastValue;
					Date nts = (Date)taskMetrics.getLastValue().getLastValue();
					if(nts.after(ts))
						this.lastValue = taskMetrics.getLastValue();
				}
			}
            else if(importContext.getLastValueType() == ImportIncreamentConfig.LOCALDATETIME_TYPE) {
                    LocalDateTime ts = (LocalDateTime)lastValue.getLastValue();
                    LocalDateTime nts = (LocalDateTime)taskMetrics.getLastValue().getLastValue();
                    if(nts.isAfter(ts))
                        this.lastValue = taskMetrics.getLastValue();
            }
			else if(importContext.getLastValueType() == ImportIncreamentConfig.STRING_TYPE) {
//				LocalDateTime ts = (LocalDateTime)lastValue.getLastValue();
//				LocalDateTime nts = (LocalDateTime)taskMetrics.getLastValue().getLastValue();
//				if(nts.isAfter(ts))
//					this.lastValue = taskMetrics.getLastValue();
				//to fixed...
			}
		}


	}



	public Object getResult() {
		return result;
	}

	public void setResult(Object result) {
		this.result = result;
	}
}
