package org.frameworkset.tran.kafka.codec;
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

import org.frameworkset.tran.kafka.KafkaImportConfig;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/11/20 20:31
 * @author biaoping.yin
 * @version 1.0
 */
public abstract class CodecUtil {
	public static String getDeserializer(String codecType){
		//key.deserializer","org.apache.kafka.common.serialization.LongDeserializer
		if(KafkaImportConfig.CODEC_JSON.equals(codecType)){
			return "org.frameworkset.tran.kafka.codec.JsonDeserializer";
		}
		else if(KafkaImportConfig.CODEC_TEXT.equals(codecType)) {
			return "org.apache.kafka.common.serialization.StringDeserializer";
		}
		else if(KafkaImportConfig.CODEC_LONG.equals(codecType)) {
			return  "org.apache.kafka.common.serialization.LongDeserializer";
		}
		else if(KafkaImportConfig.CODEC_INTEGER.equals(codecType)) {
			return "org.apache.kafka.common.serialization.IntegerDeserializer";
		}
		else{
			return codecType;
		}
	}
}
