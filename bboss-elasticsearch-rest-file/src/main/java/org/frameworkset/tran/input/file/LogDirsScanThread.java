package org.frameworkset.tran.input.file;

import org.frameworkset.tran.schedule.timer.TimeUtil;
import org.frameworkset.tran.schedule.timer.TimerScheduleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 扫描新增数据文件
 * @author biaoping.yin
 * @description
 * @create 2021/3/23
 */
public class LogDirsScanThread implements Runnable{
    private Logger logger = LoggerFactory.getLogger(LogDirsScanThread.class);
    protected final long interval;
    private Thread thread = null;
    protected volatile boolean running = false;
    protected Runnable scan;
    protected TimerScheduleConfig timerScheduleConfig;
    protected FileImportContext fileImportContext;
    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param fileImportContext The amount of time in milliseconds to wait between
     * checks of the file system
     */
    public LogDirsScanThread(Runnable scan,FileImportContext fileImportContext) {
        this.scan = scan;
        //The amount of time in milliseconds to wait between
//        checks of the file system
        this.interval = fileImportContext.getFileImportConfig().getScanNewFileInterval();
        this.timerScheduleConfig = fileImportContext.getFileImportConfig().getTimerScheduleConfig();
        this.fileImportContext = fileImportContext;
    }

    public boolean isRunning() {
        return running;
    }

    /**
     * Returns the interval.
     *
     * @return the interval
     */
    public long getInterval() {
        return interval;
    }


    public synchronized void statusRunning(){
        if (running) {
            throw new IllegalStateException("Monitor is already running");
        }

        running = true;
    }
    /**
     * Starts monitoring.
     *
     * @throws Exception if an error occurs initializing the observer
     */
    public synchronized void start() throws IllegalStateException {
        statusRunning();

        thread = new Thread(this,"NewORModifyFiles-Scan");
        thread.setDaemon(false);
        thread.start();
    }

    /**
     * Stops monitoring.
     *
     * @throws Exception if an error occurs initializing the observer
     */
    public synchronized void stop() throws Exception {
        stop(interval);
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
//        TimerScheduleConfig timerScheduleConfig = fileConfig.getTimerScheduleConfig();
        while (running) {
            /**
             * 如果没有到达执行时间点，则定时检查直到命中扫描时间点
             */
            do {

                if (TimeUtil.evalateNeedScan(timerScheduleConfig)) {
                    if (!running) {
                        break;
                    }
                    try {
//                        boolean schedulePaused = this.fileListenerService.isSchedulePaussed(autoSchedulePaused);
//                        if(!schedulePaused) {
//                            scanNewFile();
//                        }
//                        else{
//                            if(logger.isInfoEnabled()){
//                                logger.info("Ignore  Paussed Schedule Task,waiting for next resume schedule sign to continue.");
//                            }
//                        }
                        scan.run();
                    } catch (Exception e) {
                        logger.error("扫描新文件失败", e);
                    }
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
