package org.frameworkset.tran.util;
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

import org.frameworkset.tran.CommonRecord;
import org.frameworkset.tran.schedule.TaskContext;

import java.io.Writer;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/1/28 16:52
 * @author biaoping.yin
 * @version 1.0
 * use RecordGeneratorV1
 */
@Deprecated
public interface RecordGenerator {
	public final TranDummyWriter tranDummyWriter = new TranDummyWriter();
	public void buildRecord(TaskContext taskContext,  CommonRecord record, Writer builder) throws Exception;
//    public void buildRecord(Context context, CommonRecord record, Writer builder) throws Exception ;
 
}
