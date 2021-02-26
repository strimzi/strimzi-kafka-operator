/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.api.conversion.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

public class IoUtil {

    private static ByteArrayOutputStream toBaos(InputStream stream) throws IOException {
        ByteArrayOutputStream result = new ByteArrayOutputStream();
        copy(stream, result);
        return result;
    }

    /**
     * Close auto-closeable,
     * unchecked IOException is thrown for any IO exception,
     * IllegalStateException for all others.
     *
     * @param closeable the closeable
     */
    public static void close(AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Close auto-closeable, ignore any exception.
     *
     * @param closeable the closeable
     */
    public static void closeIgnore(AutoCloseable closeable) {
        try {
            close(closeable);
        } catch (Exception ignored) {
        }
    }

    /**
     * Get byte array from stream.
     * Stream is closed at the end.
     *
     * @param stream the stream
     * @return stream as a byte array
     */
    public static byte[] toBytes(InputStream stream) {
        try {
            return toBaos(stream).toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            close(stream);
        }
    }

    /**
     * Get string from stream.
     * Stream is closed at the end.
     *
     * @param stream the stream
     * @return stream as a string
     */
    public static String toString(InputStream stream) {
        try {
            return toBaos(stream).toString(StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } finally {
            close(stream);
        }
    }

    /**
     * Get string from byte array.
     *
     * @param bytes the bytes
     * @return byte array as a string
     */
    public static String toString(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Get byte array from string.
     *
     * @param string the string
     * @return string as byte array
     */
    public static byte[] toBytes(String string) {
        return string == null ? null : string.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * Get stream from content.
     *
     * @param content the content
     * @return content as stream
     */
    public static InputStream toStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    public static InputStream toStream(byte[] content) {
        return new ByteArrayInputStream(content);
    }

    public static long copy(InputStream input, OutputStream output) throws IOException {
        final byte[] buffer = new byte[8192];
        int n;
        long count = 0;
        while ((n = input.read(buffer)) != -1) {
            output.write(buffer, 0, n);
            count += n;
        }
        return count;
    }

}
