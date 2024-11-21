/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.frameworkset.tran.rocketmq.codec;

import org.apache.rocketmq.common.message.MessageExt;
import org.frameworkset.rocketmq.RockemqException;
import org.frameworkset.rocketmq.codec.CodecDeserial;
import org.frameworkset.rocketmq.codec.RocketmqMessage;
import org.frameworkset.tran.record.CellMapping;
import org.frameworkset.tran.record.FieldMappingUtil;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 *  String encoding defaults to UTF8 and can be customized by setting the property key.deserializer.encoding,
 *  value.deserializer.encoding or deserializer.encoding. The first two take precedence over the last.
 */
public class StringSplitDeserializer implements CodecDeserial<Map> {
    private String encoding = StandardCharsets.UTF_8.name();
    private String splitChar = ",";
    private List<CellMapping> cellMappingList = null;
    @Override
    public void configure(Map<String, Object> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;

        String splitCharKey = isKey ? "key.deserializer.splitChar" : "value.deserializer.splitChar";
        Object splitCharValue = configs.get(splitCharKey);
        if(splitCharValue != null){
            this.splitChar = (String)splitCharValue;
        }

        String cellMappingListKey = isKey ? "key.deserializer.cellMappingList" : "value.deserializer.cellMappingList";
        Object cellMappingList_ = configs.get(cellMappingListKey);
        if(cellMappingList_ != null){
            cellMappingList = (List<CellMapping>)cellMappingList_;
        }

    }

    @Override
    public RocketmqMessage<Map> deserial(MessageExt data) {
        try {
            if (data == null)
                return null;
            else {
               String value = new String(data.getBody(), encoding);
//               String[] values = value.split(splitChar);
               Map<String,Object> ret = new LinkedHashMap<>();
                FieldMappingUtil.buildRecord(ret, value,cellMappingList,splitChar);
                RocketmqMessage<Map> rocketmqMessage = new RocketmqMessage<>(ret,data);
               return rocketmqMessage;
            }
        } catch (Exception e) {
            throw new RockemqException("Error when deserializing byte[] to string due to unsupported encoding " + encoding);
        }
    }

}
