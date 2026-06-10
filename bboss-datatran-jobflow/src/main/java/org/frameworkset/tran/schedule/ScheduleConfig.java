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

import com.fasterxml.jackson.annotation.JsonFormat;
import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.jobflow.schedule.ScheduleConfigInterface;

import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2018/9/7 17:14
 * @author biaoping.yin
 * @version 1.0
 */
public class ScheduleConfig implements ScheduleConfigInterface {


	private Long period;
	private Boolean fixedRate;



    /**
     * 一次性执行数据导入操作
     */
    private boolean executeOneTime;

    /**
     * 一次性同步执行数据导入操作标记：工作流使用，当executeOneTime为true时起作用
     */
    private boolean executeOneTimeSyn = true;


	public boolean isExternalTimer() {
		return externalTimer;
	}

	public void setExternalTimer(boolean externalTimer) {
		this.externalTimer = externalTimer;
	}

	/**
	 * 采用外部定时任务引擎执行定时任务控制变量：
	 * false 内部引擎，默认值
	 * true 外部引擎
	 */
	protected boolean externalTimer;


	public Long getPeriod() {
		return period;
	}

	public void setPeriod(Long period) {
		this.period = period;
	}

	public Boolean getFixedRate() {
		return fixedRate;
	}

	public void setFixedRate(Boolean fixedRate) {
		this.fixedRate = fixedRate;
	}

	public String toString(){
		return SimpleStringUtil.object2json(this);
	}
    public boolean isExecuteOneTime() {
        return executeOneTime;
    }

    public void setExecuteOneTime(Boolean executeOneTime) {
        this.executeOneTime = executeOneTime;
    }

    public boolean isExecuteOneTimeSyn() {
        return executeOneTimeSyn;
    }

    public void setExecuteOneTimeSyn(boolean executeOneTimeSyn) {
        this.executeOneTimeSyn = executeOneTimeSyn;
    }

    /**
     * 任务延迟执行时间
     */
    private Long delay;

    /**
     * 任务开始时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date scheduleDate;
    /**
     * 任务结束时间
     */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date scheduleEndDate;

    private boolean continueOnError = true;

    public Long getDelay() {
        return delay;
    }

    public void setDelay(Long delay) {
        this.delay = delay;
    }

    public Date getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(Date scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    public Date getScheduleEndDate() {
        return scheduleEndDate;
    }

    public void setScheduleEndDate(Date scheduleEndDate) {
        this.scheduleEndDate = scheduleEndDate;
    }

    public void setContinueOnError(boolean continueOnError) {
        this.continueOnError = continueOnError;
    }

    public boolean isContinueOnError() {
        return continueOnError;
    }
}
