package org.frameworkset.tran.util;
/**
 * Copyright 2022 bboss
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

import org.frameworkset.tran.DataTranPlugin;
import org.frameworkset.tran.context.DefaultReInitAction;
import org.frameworkset.tran.schedule.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2022/10/12
 * @author biaoping.yin
 * @version 1.0
 */
public class EventListenStoppedThread extends StoppedThread{
	private static Logger logger = LoggerFactory.getLogger(EventListenStoppedThread.class);
    private TaskContext taskContext;
    private DataTranPlugin dataTranPlugin;
    private long metricsInterval;
	public EventListenStoppedThread(TaskContext taskContext, DataTranPlugin dataTranPlugin, long metricsInterval) {
        this.taskContext = taskContext;
        this.dataTranPlugin = dataTranPlugin;
        this.metricsInterval = metricsInterval;
	}

	 
    @Override
    public void run() {
        do {
            if (stopped) {
                break;
            }
            try {
                taskContext.reInitContext(new DefaultReInitAction(dataTranPlugin));

            } catch (Exception e) {
                logger.error(this.getName() +" afterCall Exception", e);
            }
            if (stopped) {
                break;
            }
            try {
                sleep(metricsInterval);
            } catch (InterruptedException e) {
                logger.error(this.getName() +" InterruptedException", e);
                break;
            }

        } while (true);
    }
}
