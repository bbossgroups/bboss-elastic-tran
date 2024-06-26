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
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;

public abstract class LogReader {

    /**
     * 获取操作系统默认字符编码的方法：System.getProperties().get("sun.jnu.encoding");
     * 获取操作系统文件的字符编码的方法：System.getProperties().get("file.encoding");
     * 获取JVM默认字符编码的方法：Charset.defaultCharset();
     */
    public static final String DEFAULT_CHARSET = Charset.defaultCharset().name();

    protected String charsetName;

    abstract long getFilePointer();

    abstract void seek(long pos);

    abstract String readLine();

    /**
     * 字节数组扩容
     * @param arr
     * @return
     */
    public byte[] grow(byte[] arr) {
        int len = arr.length;
        int half = len >> 1;
        int growSize = Math.max(half, 1);
        byte[] arrNew = new byte[len + growSize];
        System.arraycopy(arr, 0, arrNew, 0, len);
        return arrNew;
    }

    /**
     * 字节数组解码成字符串
     * @param arr
     * @param arrPos
     * @return
     */
    public String decode(byte[] arr, int arrPos) {
        if (arrPos == 0)
            return null;
        try {
            return new String(arr, 0, arrPos, charsetName);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage());
        }
    }
}

