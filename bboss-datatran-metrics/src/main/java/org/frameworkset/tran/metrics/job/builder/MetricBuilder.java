package org.frameworkset.tran.metrics.job.builder;
/**
 * Copyright 2023 bboss
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

import org.frameworkset.tran.metrics.entity.MapData;
import org.frameworkset.tran.metrics.entity.MetricKey;
import org.frameworkset.tran.metrics.job.KeyMetricBuilder;

/**
 * <p>Description: 指标构建器</p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/2/14
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class MetricBuilder {
    private KeyMetricBuilder keyMetricBuilder;

    /**
     * 构建记录对应的指标key值
     * @param mapData
     * @return
     */
    public abstract MetricKey buildMetricKey(MapData mapData);

    /**
     * 构建指标计算器对象
     * @return
     */
    public abstract KeyMetricBuilder metricBuilder();

    public KeyMetricBuilder innerMetricBuilder(){
        if(keyMetricBuilder != null){
            return keyMetricBuilder;
        }
        synchronized (this){
            if(keyMetricBuilder != null){
                return keyMetricBuilder;
            }
            keyMetricBuilder = metricBuilder();
        }
        return keyMetricBuilder;
    }


}
