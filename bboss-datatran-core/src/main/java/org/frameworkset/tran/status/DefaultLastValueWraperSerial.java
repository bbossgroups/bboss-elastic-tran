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

import com.frameworkset.util.SimpleStringUtil;
import org.frameworkset.tran.schedule.Status;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2023</p>
 * @Date 2023/6/2
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultLastValueWraperSerial implements LastValueWraperSerial{
    @Override
    public void serial(Status status) {
        if(status == null)
            return;
        if(status.getCurrentLastValueWrapper() != null) {
            String data = SimpleStringUtil.object2json(status.getCurrentLastValueWrapper());
            status.setStrLastValue(data);
        }
    }

    @Override
    public void deserial(Status status) {
        if (status == null)
            return;
        if(status.getStrLastValue() != null) {
            LastValueWrapper data = SimpleStringUtil.json2Object(status.getStrLastValue(),LastValueWrapper.class);
            status.setCurrentLastValueWrapper(data);
        }
    }
}
