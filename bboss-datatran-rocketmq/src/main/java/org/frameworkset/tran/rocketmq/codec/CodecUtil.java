package org.frameworkset.tran.rocketmq.codec;
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

import static org.frameworkset.tran.plugin.rocketmq.input.RocketmqInputConfig.*;

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
		if(CODEC_JSON.equals(codecType)){
			return "org.frameworkset.rocketmq.codec.JsonMapCodecDeserial";
		}
		else if(CODEC_TEXT.equals(codecType)) {
			return "org.frameworkset.rocketmq.codec.StringCodecDeserial";
		}
		else if(CODEC_TEXT_SPLIT.equals(codecType)) {
			return "org.frameworkset.tran.rocketmq.codec.StringSplitDeserializer";
		}
//
//		else if(CODEC_LONG.equals(codecType)) {
//			return  "org.apache.kafka.common.serialization.LongDeserializer";
//		}
//		else if(CODEC_INTEGER.equals(codecType)) {
//			return "org.apache.kafka.common.serialization.IntegerDeserializer";
//		}
		else if(CODEC_BYTE.equals(codecType)) {
			return "org.frameworkset.rocketmq.codec.StringCodecDeserial";
		}
		else{
			return codecType;
		}
	}
}
