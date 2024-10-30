package org.frameworkset.tran.metrics;
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

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/10/25
 */
public class MetricsLogLevel {
    public static final int DEBUG = 1;
    public static final int INFO = 2;
    public static final int WARN = 3;
    public static final int ERROR = 4;

    /**
     * 忽略所有日志
     */
    public static final int NO_LOG = 5;
    
    public static boolean isDebugEnabled(int metricsLogLevel){
        return metricsLogLevel == DEBUG;
    }

    public static boolean isInfoEnabled(int metricsLogLevel){
        return DEBUG <= metricsLogLevel && metricsLogLevel <= INFO;
    }

    public static boolean isWarnEnabled(int metricsLogLevel){
        return DEBUG <= metricsLogLevel && metricsLogLevel <= WARN;
    }

    public static boolean isErrorEnabled(int metricsLogLevel){
        return DEBUG <= metricsLogLevel && metricsLogLevel <= ERROR;
    }
}
