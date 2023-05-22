package org.frameworkset.tran.schedule.xxjob;

import org.frameworkset.util.shutdown.ShutdownUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class XXJobApplication {
    private static Logger logger = LoggerFactory.getLogger(XXJobApplication.class);

    public static void main(String[] args) {

        try {
            // start
            SyndataXXJobConfig.getInstance().initXxlJobExecutor();
            ShutdownUtil.addShutdownHook(new Runnable() {
                @Override
                public void run() {
                        // destory
                        SyndataXXJobConfig.getInstance().destoryXxlJobExecutor();
                }
            });
            while (true) {
                TimeUnit.HOURS.sleep(1);
            }

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {


        }

    }

}
