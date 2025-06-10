package org.frameworkset.tran.output.s3;
/**
 * Copyright 2024 bboss
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

import java.io.File;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/8/9
 */
public class DefaultOSSInfoBuilder implements OSSInfoBuilder{

    @Override
    public OSSFileInfo buildOSSFileInfo(OSSFileConfig OSSFileConfig, File file) {
        OSSFileInfo ossFileInfo = new OSSFileInfo();
        if(OSSFileConfig.getBucket() != null)
            ossFileInfo.setBucket(OSSFileConfig.getBucket());
        else{
            ossFileInfo.setBucket(file.getParentFile().getName());
        }
        ossFileInfo.setId(file.getName());
        return ossFileInfo;
    }
}
