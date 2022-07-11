package org.frameworkset.tran.plugin.kafka.input;
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

import org.frameworkset.tran.DataImportException;
import org.frameworkset.tran.DataTranPluginImpl;
import org.frameworkset.tran.context.ImportContext;
import org.frameworkset.tran.schedule.ScheduleEndCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/9 16:55
 * @author biaoping.yin
 * @version 1.0
 */
public class KafkaDataTranPluginImpl extends DataTranPluginImpl {

	protected static Logger logger = LoggerFactory.getLogger(KafkaDataTranPluginImpl.class);


	public KafkaDataTranPluginImpl(ImportContext importContext){
		super(importContext);


	}

	@Override
	public void importData(ScheduleEndCall scheduleEndCall) throws DataImportException {


		long importStartTime = System.currentTimeMillis();
		this.doImportData(null);
		long importEndTime = System.currentTimeMillis();
		if( isPrintTaskLog())
			logger.info(new StringBuilder().append("Execute job Take ").append((importEndTime - importStartTime)).append(" ms").toString());


	}
	@Override
	public void initSchedule(){
		logger.info("Ignore initSchedule for plugin {}",this.getClass().getName());
	}
	@Override
	public void initLastValueClumnName(){
		setIncreamentImport(false);
	}
}
