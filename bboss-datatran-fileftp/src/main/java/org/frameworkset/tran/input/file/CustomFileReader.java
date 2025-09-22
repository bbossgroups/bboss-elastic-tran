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


import java.io.*;
import java.nio.charset.Charset;

/**
 * Reads text from character files using a default buffer size. Decoding from bytes
 * to characters uses either a specified {@linkplain Charset charset}
 * or the {@linkplain Charset#defaultCharset() default charset}.
 *
 * <p>
 * The {@code FileReader} is meant for reading streams of characters. For reading
 * streams of raw bytes, consider using a {@code FileInputStream}.
 *
 * @see InputStreamReader
 * @see FileInputStream
 * @see Charset#defaultCharset()
 *
 * @author      Mark Reinhold
 * @since       1.1
 */
public class CustomFileReader extends InputStreamReader {

    /**
     * Creates a new {@code FileReader}, given the name of the file to read,
     * using the {@linkplain Charset#defaultCharset() default charset}.
     *
     * @param      fileName the name of the file to read
     * @throws FileNotFoundException  if the named file does not exist,
     *             is a directory rather than a regular file,
     *             or for some other reason cannot be opened for
     *             reading.
     * @see        Charset#defaultCharset()
     */
    public CustomFileReader(String fileName) throws FileNotFoundException {
        super(new FileInputStream(fileName));
    }

    /**
     * Creates a new {@code FileReader}, given the {@code File} to read,
     * using the {@linkplain Charset#defaultCharset() default charset}.
     *
     * @param      file the {@code File} to read
     * @throws     FileNotFoundException  if the file does not exist,
     *             is a directory rather than a regular file,
     *             or for some other reason cannot be opened for
     *             reading.
     * @see        Charset#defaultCharset()
     */
    public CustomFileReader(java.io.File file) throws FileNotFoundException {
        super(new FileInputStream(file));
    }

    /**
     * Creates a new {@code FileReader}, given the {@code FileDescriptor} to read,
     * using the {@linkplain Charset#defaultCharset() default charset}.
     *
     * @param fd the {@code FileDescriptor} to read
     * @see Charset#defaultCharset()
     */
    public CustomFileReader(FileDescriptor fd) {
        super(new FileInputStream(fd));
    }

    /**
     * Creates a new {@code FileReader}, given the name of the file to read
     * and the {@linkplain Charset charset}.
     *
     * @param      fileName the name of the file to read
     * @param      charset the {@linkplain Charset charset}
     * @throws     IOException  if the named file does not exist,
     *             is a directory rather than a regular file,
     *             or for some other reason cannot be opened for
     *             reading.
     *
     * @since 11
     */
    public CustomFileReader(String fileName, Charset charset) throws IOException {
        super(new FileInputStream(fileName), charset);
    }

    /**
     * Creates a new {@code FileReader}, given the {@code File} to read and
     * the {@linkplain Charset charset}.
     *
     * @param      file the {@code File} to read
     * @param      charset the {@linkplain Charset charset}
     * @throws     IOException  if the file does not exist,
     *             is a directory rather than a regular file,
     *             or for some other reason cannot be opened for
     *             reading.
     *
     * @since 11
     */
    public CustomFileReader(File file, Charset charset) throws IOException {
        super(new FileInputStream(file), charset);
    }
}
