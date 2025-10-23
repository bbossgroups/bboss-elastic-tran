package org.frameworkset.tran.input.file;
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

import java.io.File;

/**
 * 获取文件编码
 * @author biaoping.yin
 * @Date 2025/10/23
 */
public interface ChatsetFunction {
    /**
     * 获取文件编码
     * @param dataFile
     * @param fileConfig
     * @return
     */
    String getCharset(File dataFile,FileConfig fileConfig);

}
