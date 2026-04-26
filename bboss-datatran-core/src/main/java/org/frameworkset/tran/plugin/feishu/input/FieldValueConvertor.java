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

import org.frameworkset.tran.schedule.TaskContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 字段级别值转换器，优先级高于全局值转换器
 * @author biaoping.yin
 * @Date 2026/3/24
 */
public class FieldValueConvertor {
    public Object handleItem(TaskContext taskContext,Map<String, Object> fields, String field, Object value){
        if (value instanceof List) {
            List v = (List) value;
            int size = v.size();
            if (v != null && size > 0) {
                Object o = v.get(0);

                if(o instanceof Map){
                    if(size == 1) {
                        return ((Map) o).get("text");
                    }
                    else{
                        List texts = new ArrayList(size);
                        for(Object o1:v){
                            texts.add(((Map) o1).get("text"));
                        }
                        return texts;
                    }
                }
                

            }
        }
        else if(value instanceof Map){
            Map map = (Map)value;
            Integer type = (Integer) map.get("type");
            if(type != null){
                //公式值处理：{"type":1,"value":[{"text":"[100%]","type":"text"}]}
                List functionValues = (List) map.get("value");
                if(functionValues != null && functionValues.size() > 0){
                    Object functionValue = functionValues.get(0);
                    if(functionValue instanceof Map) {
                        return ((Map) functionValue).get("text");
                    }
                    else{
                        return functionValue;
                    }
                }
                
            }
            
        }         
        return value;
    }

}
