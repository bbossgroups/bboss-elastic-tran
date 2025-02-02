package org.frameworkset.tran.plugin.multi.output;
/**
 * Copyright 2025 bboss
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
import org.frameworkset.tran.config.OutputConfig;

import java.util.List;

/**
 * <p>Description: 用于多输出插件过滤记录，将过滤后的记录加个特定输出插件进行输出</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/2/2
 */
public interface OutputRecordsFilter {
    /**
     * 根据不同的插件对记录集进行过滤，返回过滤后的记录集，如果不需要过滤记录集，直接返回记录集即可
     * 注意不能修改记录集中的记录
     * @param config
     * @param records
     * @return
     */
    List<CommonRecord> filter(OutputConfig config, List<CommonRecord> records);

}
