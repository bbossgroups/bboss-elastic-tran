package org.frameworkset.tran.status;
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
 * @Date 2023/6/1
 * @author biaoping.yin
 * @version 1.0
 */

public class LastValueWrapper {
    private Object lastValue;
    private String strLastValue;
    private long timeStamp;

    public LastValueWrapper copy(){
        LastValueWrapper copy = new LastValueWrapper();
        copy.lastValue = this.lastValue;
        copy.strLastValue = this.strLastValue;
        copy.timeStamp = this.timeStamp;

        return copy;
    }
    public synchronized Object getLastValue() {
        return lastValue;
    }

    public synchronized void setLastValue(Object lastValue) {
        this.lastValue = lastValue;
    }

    public synchronized  String getStrLastValue() {
        return strLastValue;
    }

    public synchronized  void setStrLastValue(String strLastValue) {
        this.strLastValue = strLastValue;
    }
    public String toString(){
        StringBuilder builder = new StringBuilder();
        if(strLastValue == null){
            return String.valueOf(lastValue);
        }
        builder.append("[timeStamp=").append(timeStamp).append(",lastValue=").append(lastValue).append(",strLastValue=").append(strLastValue).append("]");
        return builder.toString();
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
