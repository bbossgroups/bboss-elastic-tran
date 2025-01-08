package org.frameworkset.tran.task;
/**
 * Copyright 2024 bboss
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.Record;
import org.frameworkset.tran.TranErrorWrapper;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.ImportCount;
import org.frameworkset.tran.metrics.TaskMetrics;
import org.frameworkset.tran.schedule.Status;
import org.frameworkset.tran.schedule.TaskContext;
import org.frameworkset.tran.status.LastValueWrapper;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/5/15
 */
public class TaskCommandContext {
    
    private ImportCount totalCount;
    private int dataSize;
    private int taskNo;
    private LastValueWrapper lastValue;
    private List<Record> records;
    private List<CommonRecord> commonRecords;
    private CommonRecord commonRecord;
    private ExecutorService service;
    private List<Future> tasks;
    private TranErrorWrapper tranErrorWrapper;
    private String jobNo;
    private String taskInfo;
    private Status currentStatus;
    private TaskContext taskContext;
    private ImportContext importContext ;
    private int ignoreCount;
    private int droped;
    private TaskMetrics taskMetrics;
    public String getJobNo() {
        return jobNo;
    }

    public void setDroped(int droped) {
        this.droped = droped;
    }

    public int getDroped() {
        return droped;
    }

    public void setJobNo(String jobNo) {
        this.jobNo = jobNo;
    }

    public String getTaskInfo() {
        return taskInfo;
    }

    public void setTaskInfo(String taskInfo) {
        this.taskInfo = taskInfo;
    }

    public Status getCurrentStatus() {
        return currentStatus;
    }

    public void setCurrentStatus(Status currentStatus) {
        this.currentStatus = currentStatus;
    }

    public TaskContext getTaskContext() {
        return taskContext;
    }

    public void setTaskContext(TaskContext taskContext) {
        this.taskContext = taskContext;
    }


    public int increamentTaskNo(){
        taskNo ++;
        return taskNo;
    }
    public int increamentIgnoreCount(){
        ignoreCount ++;
        return ignoreCount;
    }
    public int increamentDataSize(int incr){
        dataSize = dataSize + incr;
        return dataSize;
    }
    public void addTask(TaskCommand taskCommand){
        this.addTask(  service,  tasks,  taskCommand);
        
    }

    public void addTask(ExecutorService service,List<Future> tasks,TaskCommand taskCommand){
        tasks.add(service.submit(new TaskCall(taskCommand, tranErrorWrapper)));

    }

    public void addMultiOutputTask(ExecutorService service,List<Future> tasks,TaskCommand taskCommand){
        tasks.add(service.submit(new MultiOutputTaskCall(taskCommand, tranErrorWrapper)));

    }
    public int evalDataSize(){
        if(records != null && records.size() > 0){
            dataSize = records.size();
        }
        else if(commonRecords != null && commonRecords.size() > 0){
            dataSize = commonRecords.size();
        }
        else if(commonRecord != null){
            dataSize = 1;
        }
        return dataSize;
    }
    public boolean containData(){
        if(records != null && records.size() > 0){
            return true;
        }
        else if(commonRecords != null && commonRecords.size() > 0){
            return true;
        }
        else if(commonRecord != null){
            return true;
        }
        return false;
    }
    public int getIgnoreCount() {
        return ignoreCount;
    }

    public void setIgnoreCount(int ignoreCount) {
        this.ignoreCount = ignoreCount;
    }



    public ImportCount getTotalCount() {
        return totalCount;
    }

    public void setTotalCount(ImportCount totalCount) {
        this.totalCount = totalCount;
    }

    public int getDataSize() {
        return dataSize;
    }

    public void setDataSize(int dataSize) {
        this.dataSize = dataSize;
    }

    public int getTaskNo() {
        return taskNo;
    }

    public void setTaskNo(int taskNo) {
        this.taskNo = taskNo;
    }

    public LastValueWrapper getLastValue() {
        return lastValue;
    }

    public void setLastValue(LastValueWrapper lastValue) {
        this.lastValue = lastValue;
    }

    public List<Record> getRecords() {
        return records;
    }
    

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public ExecutorService getService() {
        return service;
    }

    public void setService(ExecutorService service) {
        this.service = service;
    }

    public List<Future> getTasks() {
        return tasks;
    }

    public void setTasks(List<Future> tasks) {
        this.tasks = tasks;
    }

    public TranErrorWrapper getTranErrorWrapper() {
        return tranErrorWrapper;
    }

    public void setTranErrorWrapper(TranErrorWrapper tranErrorWrapper) {
        this.tranErrorWrapper = tranErrorWrapper;
    }

    public List<CommonRecord> getCommonRecords() {
        return commonRecords;
    }

    public void setCommonRecords(List<CommonRecord> commonRecords) {
        this.commonRecords = commonRecords;
    }

    public CommonRecord getCommonRecord() {
        return commonRecord;
    }

    public void setCommonRecord(CommonRecord commonRecord) {
        this.commonRecord = commonRecord;
    }

    public ImportContext getImportContext() {
        return importContext;
    }

    public void setImportContext(ImportContext importContext) {
        this.importContext = importContext;
    }

    public TaskMetrics getTaskMetrics() {
        return taskMetrics;
    }

    public void setTaskMetrics(TaskMetrics taskMetrics) {
        this.taskMetrics = taskMetrics;
    }
}
