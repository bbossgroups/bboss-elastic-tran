package org.frameworkset.tran;
/**
 * Copyright 2020 bboss
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

import org.frameworkset.elasticsearch.ESVersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2020</p>
 * @Date 2021/7/27 15:06
 * @author biaoping.yin
 * @version 1.0
 */
public class TranVersion {
    private static final String VERSION = "7.5.6";
    private static final String RELEASEDATE = "20251127";
    private static Logger logger = LoggerFactory.getLogger(TranVersion.class);
    static {
        logger.info(getVersionDescription());
    }
    public static String getVersion756(){
        return VERSION+"_"+RELEASEDATE;
    }

    public static String getVersion(){
        return VERSION;
    }

    /**
     * Returns the catenation of the description and cvs fields.
     * @return String with description
     */
    public static String getVersionDescription() {
        return "bboss datatran client Version: \t" + VERSION + ",Release Date:\t" + RELEASEDATE ;
    }
}
