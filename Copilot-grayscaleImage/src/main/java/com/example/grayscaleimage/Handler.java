
// File: grayscale-image-lambda/src/main/java/com/example/grayscaleimage/Handler.java
package com.example.grayscaleimage;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lambda.SAAMetrics;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * grayscaleImage Lambda:
 * - Reads image from S3 (bucket, key)
 * - Converts to grayscale pixel-by-pixel using luminosity: 0.21R + 0.72G + 0.07B
 * - Preserves file format (PNG/JPG)
 * - Writes to "grayscale-{key}"
 * - Returns metrics + outputKey
 *
 * Fault tolerance:
 * - Validates bucket/key
 * - Handles invalid images
 * - Memory-aware streaming to /tmp for large objects
 */
public class Handler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3 = S3Client.builder().build();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {

        long startTime = System.currentTimeMillis();
        // processing …
        long endTime = System.currentTimeMillis();

        SAAMetrics metrics = new SAAMetrics();

        String bucket = asString(input.get("bucket"));
        String key = asString(input.get("key"));
        if (bucket == null || bucket.isEmpty() || key == null || key.isEmpty()) {
            return error("Missing 'bucket' or 'key' in input.");
        }

        if (input.containsKey("imageBytes") || input.containsKey("imageBase64")) {
            return error("Inline image payloads are not supported. Provide S3 bucket/key only.");
        }

        Map<String, Object> result;
        BufferedImage srcImage = null;
        Path tempInputFile = null;
        Path tempOutputFile = null;
        ResponseInputStream<GetObjectResponse> s3Stream = null;

        try {
            HeadObjectResponse head = s3.headObject(HeadObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build());

            long contentLength = head.contentLength();
            String format = formatFromKey(key);
            String contentType = contentTypeForFormat(format);

            Runtime rt = Runtime.getRuntime();
            long freeMem = rt.freeMemory();
            long threshold = Math.max(5 * 1024 * 1024, freeMem / 2);
            boolean useTmp = contentLength > threshold;

            if (useTmp) {
                tempInputFile = Files.createTempFile("gray-input-", "-" + sanitizeFilename(key));
                try (ResponseInputStream<GetObjectResponse> in = s3.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build());
                     OutputStream out = new BufferedOutputStream(Files.newOutputStream(tempInputFile))) {
                    byte[] buf = new byte[8192];
                    int r;
                    while ((r = in.read(buf)) != -1) {
                        out.write(buf, 0, r);
                    }
                }
                srcImage = ImageIO.read(tempInputFile.toFile());
            } else {
                s3Stream = s3.getObject(GetObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .build());
                srcImage = ImageIO.read(s3Stream);
            }

            if (srcImage == null) {
                return error("Invalid image or unsupported format for key: " + key);
            }

            // Grayscale conversion (pixel-by-pixel), preserve alpha if present
            int w = srcImage.getWidth();
            int h = srcImage.getHeight();
            boolean hasAlpha = srcImage.getColorModel().hasAlpha();
            int type = hasAlpha ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
            BufferedImage gray = new BufferedImage(w, h, type);

            for (int y = 0; y < h; y++) {
                for (int x = 0; x < w; x++) {
                    int argb = srcImage.getRGB(x, y);
                    int a = (argb >>> 24) & 0xFF;
                    int r = (argb >>> 16) & 0xFF;
                    int g = (argb >>> 8) & 0xFF;
                    int b = (argb) & 0xFF;
                    int grayVal = (int)Math.round(0.21 * r + 0.72 * g + 0.07 * b);
                    int newArgb = hasAlpha
                            ? ((a & 0xFF) << 24) | (grayVal << 16) | (grayVal << 8) | grayVal
                            : (0xFF << 24) | (grayVal << 16) | (grayVal << 8) | grayVal;
                    gray.setRGB(x, y, newArgb);
                }
            }

            String newKey = "grayscale-" + key;

            if (useTmp) {
                tempOutputFile = Files.createTempFile("gray-output-", "-" + sanitizeFilename(newKey));
                ImageIO.write(gray, format, tempOutputFile.toFile());
                PutObjectRequest putReq = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(newKey)
                        .contentType(contentType)
                        .build();
                s3.putObject(putReq, RequestBody.fromFile(tempOutputFile));
            } else {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.max(32 * 1024, (int) Math.min(contentLength, Integer.MAX_VALUE)))) {
                    ImageIO.write(gray, format, baos);
                    PutObjectRequest putReq = PutObjectRequest.builder()
                            .bucket(bucket)
                            .key(newKey)
                            .contentType(contentType)
                            .build();
                    s3.putObject(putReq, RequestBody.fromBytes(baos.toByteArray()));
                }
            }

            long end = System.currentTimeMillis();
            metrics.setRuntime(end - startTime);

            LinkedHashMap<String, Object> out = new LinkedHashMap<>(metrics.toMap());
            out.put("outputKey", newKey);
            result = out;

        } catch (NoSuchKeyException e) {
            result = error("S3 object not found: " + e.getMessage());
        } catch (S3Exception e) {
            result = error("S3 error: " + e.awsErrorDetails().errorMessage());
        } catch (IOException e) {
            result = error("I/O error: " + e.getMessage());
        } catch (Exception e) {
            result = error("Unexpected error: " + e.getMessage());
        } finally {
            if (s3Stream != null) {
                try { s3Stream.close(); } catch (IOException ignore) {}
            }
            if (tempInputFile != null) {
                try { Files.deleteIfExists(tempInputFile); } catch (IOException ignore) {}
            }
            if (tempOutputFile != null) {
                try { Files.deleteIfExists(tempOutputFile); } catch (IOException ignore) {}
            }
        }

        return result;
    }

    private static String asString(Object o) {
        return (o == null) ? null : String.valueOf(o);
    }

    private static String formatFromKey(String key) {
        String k = key.toLowerCase(Locale.ROOT);
        if (k.endsWith(".jpeg") || k.endsWith(".jpg")) return "jpg";
        if (k.endsWith(".png")) return "png";
        return "png";
    }

    private static String contentTypeForFormat(String fmt) {
        return "jpg".equalsIgnoreCase(fmt) ? "image/jpeg" : "image/png";
    }

    private static String sanitizeFilename(String s) {
        return s.replaceAll("[^A-Za-z0-9._-]", "_");
    }

    private static Map<String, Object> error(String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", message);
        return m;
    }
}
