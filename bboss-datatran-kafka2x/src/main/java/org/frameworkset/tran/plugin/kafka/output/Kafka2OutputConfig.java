package org.frameworkset.tran.plugin.kafka.output;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.plugin.OutputPlugin;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/2/23 11:42
 * @author biaoping.yin
 * @version 1.0
 */
public class Kafka2OutputConfig extends KafkaOutputConfig{

	@Override
	public OutputPlugin getOutputPlugin(ImportContext importContext) {
		return new Kafka2OutputDataTranPlugin(importContext,this);
	}
}
