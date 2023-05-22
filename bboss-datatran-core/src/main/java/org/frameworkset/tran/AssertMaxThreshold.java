package org.frameworkset.tran;

/**
 * Copyright 2023 bboss
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

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/6
 * @author biaoping.yin
 * @version 1.0
 */
public class AssertMaxThreshold {

    private int integerCount;
    private int maxThreshold;
    private long awaitTime = 5000L;
    private boolean stopped;
    public AssertMaxThreshold(int maxThreshold){
        this.maxThreshold = maxThreshold;
    }

    public AssertMaxThreshold(int maxThreshold,long awaitTime){
        this.maxThreshold = maxThreshold;
        if(awaitTime <= 0 )
            throw new IllegalArgumentException("awaitTime must > 0");
        this.awaitTime = awaitTime;
    }
    private Object lock = new Object();
    public void stop(){
        if(stopped )
            return;
        synchronized (lock) {
            if(stopped )
                return;
            this.stopped = true;
            lock.notifyAll();
        }
    }


    /**
     * 检查是否允许增加新的任务，如果任务数量累计达到指定的数量，则等待任务执行完释放工作位后，再继续添加新的任务
     * @return
     */
    public boolean assertEnableNext(){
        boolean result = true;
        synchronized (lock) {
            do {
                if (!reachMaxFileThreshold()) {
                    if (stopped) {
                        result = false;
                    }
                    increament();
                    break;
                } else {
                    try {
//                        System.out.println("lock start!");
//                        long s = System.currentTimeMillis();
                        lock.wait(awaitTime);
//                        long e = System.currentTimeMillis();
//                        System.out.println("lock end! time:"+(e-s));
                    } catch (InterruptedException e) {
                        result = false;
                        break;
                    }

                    if (stopped) {
                        result = false;
                        break;
                    }
                }
            } while (true);
        }
        return result;
    }
    public int increament(){
//        assertEnableNext(false);
        synchronized (lock) {
            integerCount++;
        }
        return integerCount;
    }
    public int increament(int incr){
//        assertEnableNext(false);
        synchronized (lock) {
            integerCount = integerCount + incr;
        }
        return integerCount;
    }
    public boolean isStopped(){
        return stopped;
    }

    public int decreament(){
        synchronized (lock) {
            integerCount --;
            lock.notifyAll();
        }
        return integerCount;
    }



    public int decreament(int incr){

        synchronized (lock){
            integerCount = integerCount - incr;
            lock.notifyAll();
        }
        return integerCount;
    }
    private boolean reachMaxFileThreshold(){
        if(maxThreshold > 0 ){
//            System.out.println("integerCount.getCount()："+integerCount.getCount());
            if(integerCount <= maxThreshold){
                return false;
            }
            return true;
        }
        return false;
    }
}
