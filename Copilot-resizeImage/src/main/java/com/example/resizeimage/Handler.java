
// File: resize-image-lambda/src/main/java/com/example/resizeimage/Handler.java
package com.example.resizeimage;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import lambda.SAAMetrics;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * resizeImage Lambda:
 * - Reads image from S3 (bucket, key)
 * - Resizes to exactly 128x128 using bilinear interpolation
 * - Preserves file format (PNG/JPG)
 * - Writes to "resized-{key}"
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
                tempInputFile = Files.createTempFile("resize-input-", "-" + sanitizeFilename(key));
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

            // Resize to 128x128 with bilinear interpolation
            int type = srcImage.getColorModel().hasAlpha() ? BufferedImage.TYPE_INT_ARGB : BufferedImage.TYPE_INT_RGB;
            BufferedImage resized = new BufferedImage(128, 128, type);
            Graphics2D g2d = resized.createGraphics();
            try {
                g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
                g2d.drawImage(srcImage, 0, 0, 128, 128, null);
            } finally {
                g2d.dispose();
            }

            String newKey = "resized-" + key;

            if (useTmp) {
                tempOutputFile = Files.createTempFile("resize-output-", "-" + sanitizeFilename(newKey));
                ImageIO.write(resized, format, tempOutputFile.toFile());
                PutObjectRequest putReq = PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(newKey)
                        .contentType(contentType)
                        .build();
                s3.putObject(putReq, RequestBody.fromFile(tempOutputFile));
            } else {
                try (ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.max(32 * 1024, (int) Math.min(contentLength, Integer.MAX_VALUE)))) {
                    ImageIO.write(resized, format, baos);
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
