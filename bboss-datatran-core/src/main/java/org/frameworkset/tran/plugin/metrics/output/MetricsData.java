package org.frameworkset.tran.plugin.metrics.output;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.metrics.job.MetricsConfig;
import org.frameworkset.tran.metrics.job.MetricsException;
import org.frameworkset.util.TimeUtil;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Date;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2023/2/13
 * @author biaoping.yin
 * @version 1.0
 */
public class MetricsData {
    private BuildMapDataContext buildMapDataContext;
    private CommonRecord commonRecord;
    public void setData(MapData mapData,ETLMetrics etlMetrics){
        String dataTimeField = etlMetrics == null? buildMapDataContext.getDataTimeField()
                                    : etlMetrics.getDataTimeField(this);
        Date dateTime = null;
        if(dataTimeField != null && !dataTimeField.equals("")){
            Object value =  commonRecord.getData(dataTimeField);
            if(value instanceof Date){
                dateTime = (Date)value;
            }
            else if(value instanceof Long){
                dateTime = new Date((Long)value);
            }
            else if(value instanceof LocalDateTime){
                dateTime = (Date) TimeUtil.convertLocalDate(value);
//                dateTime = new Date((Long)value);
            }
            else if(value instanceof LocalDate){
                dateTime = (Date) TimeUtil.convertLocalDate(value);
//                dateTime = new Date((Long)value);
            }
            else{
                throw new MetricsException("dataTimeField["+dataTimeField+"]="+dateTime +"不是Date类型，请处理为Date类型!");
            }
        }
        else {
            dateTime = buildMapDataContext.getCurrentTime();
        }
        mapData.setDataTime(dateTime);
        mapData.setData(getCommonRecord());
        mapData.setYearFormat(buildMapDataContext.getYearFormat());
        if(etlMetrics.getTimeWindowType() == MetricsConfig.TIME_WINDOW_TYPE_YEAR){
            return;
        }
        mapData.setMonthFormat(buildMapDataContext.getMonthFormat());
        if(etlMetrics.getTimeWindowType() == MetricsConfig.TIME_WINDOW_TYPE_MONTH){
            return;
        }
        mapData.setWeekFormat(buildMapDataContext.getWeekFormat());
        if(etlMetrics.getTimeWindowType() == MetricsConfig.TIME_WINDOW_TYPE_WEEK){
            return;
        }
        mapData.setDayFormat(buildMapDataContext.getDayFormat());
        if(etlMetrics.getTimeWindowType() == MetricsConfig.TIME_WINDOW_TYPE_DAY){
            return;
        }
        mapData.setHourFormat(buildMapDataContext.getHourFormat());
        if(etlMetrics.getTimeWindowType() == MetricsConfig.TIME_WINDOW_TYPE_HOUR){
            return;
        }
        mapData.setMinuteFormat(buildMapDataContext.getMinuteFormat());

    }

    public BuildMapDataContext getBuildMapDataContext() {
        return buildMapDataContext;
    }

    public void setBuildMapDataContext(BuildMapDataContext buildMapDataContext) {
        this.buildMapDataContext = buildMapDataContext;
    }


    public CommonRecord getCommonRecord() {
        return commonRecord;
    }

    public void setCommonRecord(CommonRecord commonRecord) {
        this.commonRecord = commonRecord;
    }

}
