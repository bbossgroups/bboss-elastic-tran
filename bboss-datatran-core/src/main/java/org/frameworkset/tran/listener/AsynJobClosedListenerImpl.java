package org.frameworkset.tran.listener;
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

import org.frameworkset.tran.context.ImportContext;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/14
 * @author biaoping.yin
 * @version 1.0
 */
public class AsynJobClosedListenerImpl implements JobClosedListener{
    private JobClosedListener jobClosedListener;
    public AsynJobClosedListenerImpl(JobClosedListener jobClosedListener){
        this.jobClosedListener = jobClosedListener;
    }
    @Override
    public void jobClosed(final ImportContext importContext, final Throwable throwable) {
        JobClosedListenerThread jobClosedListenerThread = new JobClosedListenerThread(new Runnable() {
            @Override
            public void run() {
                jobClosedListener.jobClosed(importContext,throwable);
            }
        });
        jobClosedListenerThread.start();
    }
}
