package org.frameworkset.elasticsearch.client;
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

import org.frameworkset.tran.AssertMaxThreshold;

import static java.lang.Thread.sleep;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/4/7
 * @author biaoping.yin
 * @version 1.0
 */
public class TestAssertMaxThresholdNotify {
    public static void main(String[] args){
        AssertMaxThreshold assertMaxThreshold = new AssertMaxThreshold(3,50000L);
        assertMaxThreshold.increament();


        Thread t2 = new Thread(new Runnable() {
            @Override
            public void run() {
                do {
                    System.out.println("assertMaxThreshold.decreamentFiles()");
                    int i = assertMaxThreshold.decreament();

                    try {
                        sleep(1000L);
                    } catch (InterruptedException e) {
                        break;
                    }

                    if(assertMaxThreshold.isStopped())
                        break;
                }while (true);
            }
        });
        t2.start();


        do{
            if(assertMaxThreshold.assertEnableNext()){
                System.out.println("assertMaxThreshold.assertEnableTranNext");
                try {
                    sleep(1000l);
                } catch (InterruptedException e) {
                  break;
                }
            }
            else{
                break;
            }
        }while (true);
    }
}
