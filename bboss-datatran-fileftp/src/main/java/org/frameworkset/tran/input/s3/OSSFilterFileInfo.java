package org.frameworkset.tran.input.s3;
/**
 * Copyright 2025 bboss
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

import org.frameworkset.nosql.s3.OSSFile;
import org.frameworkset.tran.input.file.FilterFileInfo;

/**
 * @author biaoping.yin
 * @Date 2025/8/11
 */
public class OSSFilterFileInfo implements FilterFileInfo {
    private final OSSFile ossFile;

    public OSSFilterFileInfo(OSSFile ossFile){
        this.ossFile = ossFile;
         
    }

    @Override
    public String getParentDir() {
        
        return ossFile.getParentDir();
    }

    @Override
    public String getFileName() {
        return ossFile.getObjectName();
    }

    @Override
    public boolean isDirectory() {
        return ossFile.isDir();
    }

    @Override
    public Object getFileObject() {
        return ossFile;
    }


    @Override
    public long getLastModified(){
        return ossFile.getLastModified().getTime();
    }
}
