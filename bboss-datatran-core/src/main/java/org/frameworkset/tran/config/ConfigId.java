package org.frameworkset.tran.config;
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

/**
 * <p>Description: 插件配置唯一标识，用于区分不同的输出插件或者输入插件，一般在多输出插件过滤记录集时使用</p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2025/2/2
 */
public interface ConfigId<T extends ConfigId> {
    /**
     * 设置插件配置唯一标识，用于区分不同的输出插件或者输入插件，一般在多输出插件过滤记录集时使用
     * @param id
     */
     T setId(String id);

    /**
     * 获取插件配置唯一标识，用于区分不同的输出插件或者输入插件，一般在多输出插件过滤记录集时使用
     * @return
     */
     String getId();
}
