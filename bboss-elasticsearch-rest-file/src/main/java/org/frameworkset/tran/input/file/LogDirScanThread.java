package org.frameworkset.tran.input.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;

/**
 * Refactor from commons-io
 * @author xutengfei
 * @description
 * @create 2021/3/23
 */
public class LogDirScanThread implements Runnable{
    private Logger logger = LoggerFactory.getLogger(LogDirScanThread.class);
    private final long interval;
    private FileConfig fileConfig;
    private Thread thread = null;
    private volatile boolean running = false;

    private FileListenerService fileListenerService;


    /**
     * Constructs a monitor with the specified interval and set of observers.
     *
     * @param interval The amount of time in milliseconds to wait between
     * checks of the file system
     * @param fileListenerService .
     */
    public LogDirScanThread(final long interval, FileConfig fileConfig,FileListenerService fileListenerService) {
        this.interval = interval;
        this.fileConfig = fileConfig;
        this.fileListenerService = fileListenerService;
    }


    /**
     * Returns the interval.
     *
     * @return the interval
     */
    public long getInterval() {
        return interval;
    }


    /**
     * Starts monitoring.
     *
     * @throws Exception if an error occurs initializing the observer
     */
    public synchronized void start() throws IllegalStateException {
        if (running) {
            throw new IllegalStateException("Monitor is already running");
        }

        running = true;

        thread = new Thread(this,"LogFile-Change-monitor");
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
        while (running) {
            checkAndNotify();
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

    /**
     * 识别新增的文件，如果有新增文件，将启动新的文件采集作业
     */
    private void checkAndNotify(){
        if(logger.isDebugEnabled()){
            logger.debug("scan new log file in dir {} with filename regex {}.",fileConfig.getLogDir(),fileConfig.getFileNameRegular());
        }
        File logDir = fileConfig.getLogDir();
        FilenameFilter filter = fileConfig.getFilter();
        if(logDir.isDirectory() && logDir.exists()){
            File[] files = logDir.listFiles(filter);
            File file = null;
            for(int i = 0; files != null && i < files.length; i ++){
                file = files[i];
                if(file.isFile() && file.exists()) {
                    fileListenerService.checkNewFile(file);
                }
            }
        }
        else{
            logger.info("{} must be a directory or must be exists.",fileConfig.getSourcePath() );
        }
    }



}
