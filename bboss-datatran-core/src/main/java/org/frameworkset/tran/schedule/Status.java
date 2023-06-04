package org.frameworkset.tran.schedule;
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

import com.frameworkset.orm.annotation.Column;
import org.frameworkset.tran.status.LastValueWrapper;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/8 17:37
 * @author biaoping.yin
 * @version 1.0
 */
public class Status {
	private String id;
    @Column(name = "lasttime")
	private long time;
	/**
	 * 0 数字类型
	 * 1 日期类型
	 */
	private int lastValueType;
//	private Object lastValue;




    private String strLastValue;


    private LastValueWrapper currentLastValueWrapper;
	private String filePath = "";
	private String realPath = "";



	private String relativeParentDir = "";
	private String fileId = "";
	private int status = ImportIncreamentConfig.STATUS_COLLECTING;



	private String jobType;

	private String jobId;
    public Status(){

    }
	public long getTime() {
		return time;
	}

	public void setTime(long time) {
		this.time = time;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}
	public String toString(){
		StringBuilder ret = new StringBuilder();
		ret.append("id:").append(id)
				.append(",time:").append(new Date(time))
//				.append(",lastValue:").append(this.lastValue)
                .append(",strLastValue:").append(this.strLastValue)
				.append(",filePath:").append(filePath)
				.append(",relativeParentDir:").append(relativeParentDir)
                .append(",currentLastValueWrapper:").append(currentLastValueWrapper.toString())

				.append(",realPath:").append(realPath)
				.append(",fileId:").append(fileId)
				.append(",type:").append(lastValueType)
				.append(",status:").append(status);
		return ret.toString();
	}


//	public synchronized Object getLastValue() {
//		return lastValue;
//	}

//	public synchronized void setLastValue(Object lastValue) {
//		this.lastValue = lastValue;
//	}

	public Status copy(){
		Status copy = new Status();
		copy.fileId = this.fileId;
		copy.filePath = this.filePath;
		copy.relativeParentDir = this.relativeParentDir;
		copy.realPath = this.realPath;
		copy.id = this.id;
//		copy.lastValue = this.lastValue;
		copy.lastValueType = this.lastValueType;
		copy.status = this.status;
		copy.time = this.time;
		copy.jobId = this.jobId;
		copy.jobType = this.jobType;
        copy.strLastValue = strLastValue;
        copy.currentLastValueWrapper = currentLastValueWrapper.copy();
		return copy;
	}

	public int getLastValueType() {
		return lastValueType;
	}

	public void setLastValueType(int lastValueType) {
		this.lastValueType = lastValueType;
	}


	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		if(filePath != null)
			this.filePath = filePath;
	}

	public String getFileId() {
		return fileId;
	}

	public void setFileId(String fileId) {
		if(fileId != null)
			this.fileId = fileId;
	}

	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getRealPath() {
		return realPath;
	}

	public void setRealPath(String realPath) {
		this.realPath = realPath;
	}
	public String getRelativeParentDir() {
		return relativeParentDir;
	}

	public void setRelativeParentDir(String relativeParentDir) {
		this.relativeParentDir = relativeParentDir;
	}
	public String getJobId() {
		return jobId;
	}

	public void setJobId(String jobId) {
		this.jobId = jobId;
	}
	public String getJobType() {
		return jobType;
	}

	public void setJobType(String jobType) {
		this.jobType = jobType;
	}
    public String getStrLastValue() {
        return strLastValue;
    }

    public void setStrLastValue(String strLastValue) {
        this.strLastValue = strLastValue;
    }
    public synchronized LastValueWrapper getCurrentLastValueWrapper() {
        return currentLastValueWrapper;
    }

    public synchronized void setCurrentLastValueWrapper(LastValueWrapper currentLastValueWrapper) {
        this.currentLastValueWrapper = currentLastValueWrapper;
    }

}
