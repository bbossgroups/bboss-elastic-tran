package org.frameworkset.tran.plugin.feishu.input;
/**
 * Copyright 2026 bboss
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

import java.util.Map;

/**
 * 字段级别值转换器，优先级高于全局值转换器
 * @author biaoping.yin
 * @Date 2026/3/24
 */
public interface FieldValueConvertor {
    Object handleItem(Map<String,Object> fields,String field,Object value);

}
