package org.apache.beam.sdk.io.astra.db.utils;

/*-
 * #%L
 * Beam SDK for Astra
 * --
 * Copyright (C) 2023 DataStax
 * --
 * Licensed under the Apache License, Version 2.0
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Utility class for Astra Secure Connect Bundle.
 */
public class AstraSecureConnectBundleUtils {

    /**
     * Private constructor.
     */
    private AstraSecureConnectBundleUtils() {}

    /**
     * Read binary content from a file.
     *
     * @param filePath
     *      file Path
     * @return
     *      binary
     */
    public static final byte[] loadFromFilePath(String filePath) {
        try {
            return Files.readAllBytes(Path.of(filePath));
        } catch(Exception e) {
            throw new RuntimeException("Unable to read file " + filePath, e);
        }
    }

    /**
     * Download a file and get binary from URL.
     *
     * @param fileUrl
     *      file URL
     * @return
     *      binary content
     */
    public static final byte[] loadFromURL(String fileUrl) {
        URL url = null;
        try {
            url = new URL(fileUrl);
            try (
                 InputStream inputStream = url.openStream();
                 ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                byte[] buffer = new byte[4096];
                int bytesRead;
                while ((bytesRead = inputStream.read(buffer)) != -1) {
                    outputStream.write(buffer, 0, bytesRead);
                }
                return outputStream.toByteArray();
            }
        } catch (MalformedURLException e) {
            throw new RuntimeException("Invalid URL " + fileUrl, e);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read file " + fileUrl, e);
        }
    }

    /**
     * Get binary content from an inputStream.
     *
     * @param inputStream
     *      inputstream
     * @return
     *     binary content   
     */
    public static final byte[] loadFromInputStream(InputStream inputStream) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = inputStream.read(buffer)) != -1) {
                outputStream.write(buffer, 0, bytesRead);
            }
            return outputStream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read input stream", e);
        }
    }
}
