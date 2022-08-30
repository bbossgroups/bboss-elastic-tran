package org.frameworkset.tran.schedule.timer;
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
import org.frameworkset.tran.schedule.ScheduleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/10/15 21:49
 * @author biaoping.yin
 * @version 1.0
 */
public class ScheduleTimer implements Runnable{
	private TimerScheduleConfig timerScheduleConfig;
	private ScheduleService scheduleService;
	private static Logger logger = LoggerFactory.getLogger(ScheduleTimer.class);
	private Thread thread = null;
	protected volatile boolean running = false;
	public ScheduleTimer(TimerScheduleConfig timerScheduleConfig,ScheduleService scheduleService ){
		this.timerScheduleConfig = timerScheduleConfig;
		this.scheduleService = scheduleService;
	}

	/**
	 * Starts monitoring.
	 *
	 * @throws Exception if an error occurs initializing the observer
	 */
	public synchronized void start() throws IllegalStateException {
		if (running) {
			throw new IllegalStateException("BBossScheduleTimer is already running");
		}

		running = true;

		thread = new Thread(this,"BBossScheduleTimer-"+scheduleService.getImportContext().getJobName());
		thread.setDaemon(false);
		thread.start();
	}

	/**
	 * Stops monitoring.
	 *
	 * @throws Exception if an error occurs initializing the observer
	 */
	public synchronized void stop() throws Exception {
		stop(timerScheduleConfig.getPeriod());
	}

	/**
	 * Stops monitoring.
	 *
	 * @param stopInterval the amount of time in milliseconds to wait for the thread to finish.
	 * A value of zero will wait until the thread is finished (see {@link Thread#join(long)}).
	 * @throws Exception if an error occurs initializing the observer
	 * @since 2.1
	 */
	public synchronized void stop(final long stopInterval) throws Exception {
		if (running == false) {
			throw new IllegalStateException("Monitor is not running");
		}
		running = false;
		try {
			thread.interrupt();
			thread.join(stopInterval);
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}
	/**
	 * Runs this monitor.
	 */
	@Override
	public void run() {
		Long interval = timerScheduleConfig.getPeriod();
		if(interval == null){
			interval = 100000l;
		}
		ImportContext importContext = scheduleService.getImportContext();
		Long deyLay = importContext.getDeyLay();
		if(deyLay != null){
			try {
				Thread.sleep(deyLay.longValue());
			} catch (final InterruptedException ignored) {
				return;
			}
		}
		while (running) {
			/**
			 * 如果没有到达执行时间点，则定时检查直到命中扫描时间点
			 */
			do {

				if (TimeUtil.evalateNeedScan(timerScheduleConfig)) {
					if(scheduleService.isSchedulePaused(scheduleService.isEnableAutoPauseScheduled())){
						if(logger.isInfoEnabled()){
							logger.info("Ignore  Paussed Schedule Task,waiting for next resume schedule sign to continue.");
						}
						break;
					}
					scheduleService.externalTimeSchedule();
					break;
				}
				else {
					try {
						Thread.sleep(30000l);
					} catch (final InterruptedException ignored) {
						// ignore
						break;
					}
				}
			}while(true);
			if (!running) {
				break;
			}
			try {
				Thread.sleep(interval);
			} catch (final InterruptedException ignored) {
				// ignore
			}
		}
	}
}
