package org.frameworkset.tran;
/**
 * Copyright 2008 biaoping.yin
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


import org.frameworkset.tran.record.TranMetaDataLazeLoad;

import java.util.Set;

/**
 * <p>Description: </p>
 * <p></p>
 * <p>Copyright (c) 2018</p>
 * @Date 2019/10/27 12:13
 * @author biaoping.yin
 * @version 1.0
 */
public class DefaultTranMetaData implements TranMeta {
    private TranMetaDataLazeLoad tranMetaDataLazeLoad ;
	public DefaultTranMetaData(Object keys) {
 
        this.keys = convert(keys);
        loaded = true;
	}
    public static String[] convert(Object keys){
        String[] tmp = null;
        if(keys != null) {
            if (keys instanceof Set )
                tmp = ((Set<String> )keys).toArray(new String[0]);
            else
                tmp = ((String[])keys);
        }
        else {
            tmp = new String[0];
        }
        return tmp;
    }

    public DefaultTranMetaData(TranMetaDataLazeLoad tranMetaDataLazeLoad) {
        if(tranMetaDataLazeLoad != null) {
            this.tranMetaDataLazeLoad = tranMetaDataLazeLoad;
//            this.keys = tranMetaDataLazeLoad.lazeLoad();
        }
        else {
            this.keys = new String[0];
            loaded = true;
        }
        
    }

	private String[] keys;
	@Override
	public int getColumnCount()  throws DataImportException {
        lazeLoaded();
        return keys.length;

	}

    private boolean loaded;
    private Object lock = new Object();
    private void lazeLoaded(){
        if(loaded)
            return;
        synchronized (lock){
            if(loaded)
                return;
            String[] keys = tranMetaDataLazeLoad.lazeLoad();
            if(keys != null){
                this.keys = keys;
            }
            else{
                this.keys = new String[0];
            }
        }
    }
	@Override
	public String getColumnLabelByIndex(int i)  throws DataImportException {
        lazeLoaded();
		return keys[i];
	}

	@Override
	public int getColumnTypeByIndex(int i)  throws DataImportException {
		return -1;
	}

	@Override
	public String getColumnJavaNameByIndex(int i)  throws DataImportException {
        lazeLoaded();
		return keys[i];

	}

	@Override
	public String getColumnLabelLowerByIndex(int i)  throws DataImportException {
        lazeLoaded();
		return keys[i].toLowerCase();

	}
}
