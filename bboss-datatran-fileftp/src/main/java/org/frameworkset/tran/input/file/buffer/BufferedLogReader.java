package org.frameworkset.tran.input.file.buffer;
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

/**
 * <p>Description: </p>
 * <p></p>
 *
 * @author biaoping.yin
 * @Date 2024/6/24
 */
import org.frameworkset.tran.input.file.FilelogPluginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;

public class BufferedLogReader extends LogReader implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(BufferedLogReader.class);
    public static final int DEFAULT_BUFFER_CAPACITY = 8192;

    private byte[] buffer;

    private int position;

    private int limit = -1;

    private RandomAccessFile raf;

    public BufferedLogReader(String pathName) {
        init(new File(pathName), DEFAULT_BUFFER_CAPACITY, DEFAULT_CHARSET);
    }

    public BufferedLogReader(String pathName, String charsetName) {
        init(new File(pathName), DEFAULT_BUFFER_CAPACITY, charsetName);
    }

    public BufferedLogReader(String pathName, int bufferCapacity) {
        init(new File(pathName), bufferCapacity, DEFAULT_CHARSET);
    }

    public BufferedLogReader(String pathName, int bufferCapacity, String charsetName) {
        init(new File(pathName), bufferCapacity, charsetName);
    }

    public BufferedLogReader(File file) {
        init(file, DEFAULT_BUFFER_CAPACITY, DEFAULT_CHARSET);
    }

    public BufferedLogReader(File file, String charsetName) {
        init(file, DEFAULT_BUFFER_CAPACITY, charsetName);
    }

    public BufferedLogReader(File file, int bufferCapacity) {
        init(file, bufferCapacity, DEFAULT_CHARSET);
    }

    public BufferedLogReader(File file, int bufferCapacity, String charsetName) {
        init(file, bufferCapacity, charsetName);
    }

    private void init(File file, int bufferCapacity, String charsetName) {
        if (bufferCapacity < 1)
            throw new FilelogPluginException("bufferCapacity setted error:"+bufferCapacity);
        Charset.forName(charsetName); // 检查字符集是否合法
        try {
            raf = new RandomAccessFile(file, "r");
        } catch (FileNotFoundException e) {
            
            throw new FilelogPluginException("Init RandomAccessFile failed:",e);
        }
        buffer = new byte[bufferCapacity];
        this.charsetName = charsetName;
    }

    @Override
    public long getFilePointer() {
        try {
            return raf.getFilePointer();
        } catch (IOException e) {
            throw new FilelogPluginException("getFilePointer:",e);
        }
    }

    @Override
    public void seek(long pos) {
        try {
            raf.seek(pos);
        } catch (IOException e) {
            throw new FilelogPluginException("Seek pos failed:"+pos,e);
        }
    }

    @Override
    public String readLine() {
        try {
            if (position > limit) {
                if (!readMore())
                    return null;
            }
            byte[] arr = new byte[336];
            int arrPos = 0;
            while (position <= limit) {
                byte b = buffer[position++];
                switch (b) {
                    case 10: //Unix or Linux line separator
                        return decode(arr, arrPos);
                    case 13: //Windows or Mac line separator
                        if (position > limit) {
                            if (readMore())
                                judgeMacOrWindows();
                        } else
                            judgeMacOrWindows();
                        return decode(arr, arrPos);
                    default: // not line separator
                        if (arrPos >= arr.length)
                            arr = grow(arr);
                        arr[arrPos++] = b;
                        if (position > limit) {
                            if (!readMore())
                                return decode(arr, arrPos);
                        }
                }
            }
        } catch (IOException e) {
            throw new FilelogPluginException("readLine failed:",e);
        }
        return null;
    }

    private void judgeMacOrWindows() {
        byte b1 = buffer[position++];
        if (b1 != 10) // Mac line separator
            position--;
    }

    private boolean readMore() throws IOException {
        limit = raf.read(buffer) - 1;
        position = 0;
        return limit >= 0;
    }

    @Override
    public void close() {
        try {
            raf.close();
        } catch (IOException e) {
            throw new FilelogPluginException("close raf failed:",e);
        }
    }
}

