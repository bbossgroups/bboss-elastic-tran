package org.apache.commons.net.examples.ftp;
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

import org.frameworkset.tran.input.file.buffer.BufferedLogReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/6/24
 */
public class BufferedLogReaderTest {
    private static Logger logger = LoggerFactory.getLogger(BufferedLogReader.class);
    public static void main(String[] args){
        BufferedLogReader bufferedLogReader = new BufferedLogReader("C:\\workdir\\exception\\t_psie_ipu_app_crash_list_24_202405_20240619_153123.json");
        String line = null;
        while (true) {
            line = bufferedLogReader.readLine();
            if(line != null)
                logger.info(line);
            else {
                break;
            }
        }
    }

}
