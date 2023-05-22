package org.frameworkset.tran.task;
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
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.job.BuildMapDataContext;
import org.frameworkset.tran.plugin.metrics.output.ETLMetrics;
import org.frameworkset.tran.plugin.metrics.output.MetricsData;

import java.util.List;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/3/22
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class BaseTranJob implements TranJob {
    protected void metricsMap(CommonRecord commonRecord, BuildMapDataContext buildMapDataContext, ImportContext importContext){
        if(buildMapDataContext == null){
            return;
        }
        map(  commonRecord,   buildMapDataContext,   importContext.getMetrics(),  importContext.isUseDefaultMapData());

    }

    public BuildMapDataContext buildMapDataContext(ImportContext importContext){
        List<ETLMetrics> etlMetrics = importContext.getMetrics();
        BuildMapDataContext buildMapDataContext = null;
        if(etlMetrics != null) {
            buildMapDataContext = new BuildMapDataContext();
            String dataTimeField = importContext.getDataTimeField();
            buildMapDataContext.setDataTimeField(dataTimeField);
        }
        return buildMapDataContext;
    }

    public static void map(CommonRecord commonRecord, BuildMapDataContext buildMapDataContext, List<ETLMetrics> etlMetrics,boolean isUseDefaultMapData){

        MetricsData metricsData = new MetricsData();
        metricsData.setBuildMapDataContext(buildMapDataContext);
        metricsData.setCommonRecord(commonRecord);

        MapData defaultMapData = null;
        if(isUseDefaultMapData){

            defaultMapData = new MapData();
            metricsData.setData(defaultMapData,null);
        }
        for (ETLMetrics metrics : etlMetrics) {
            if(metrics.getDataTimeField() != null || metrics.getBuildMapData() != null || defaultMapData == null){
                metrics.map(metrics.buildMapData(metricsData));
            }
            else {

                metrics.map(defaultMapData);
            }

        }

    }
}
