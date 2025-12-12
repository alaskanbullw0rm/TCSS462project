package org.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;

import saaf.Inspector; // <--- ADDED SAAF Inspector import
// Assuming SAAMetrics is no longer needed since Inspector provides its functionality

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.nio.file.Files;

/**
 * Lambda handler for grayscaleImage.
 * - Reads an image from S3 (bucket/key).
 * - Converts to grayscale pixel-by-pixel using standard luminosity:
 * gray = 0.21 * R + 0.72 * G + 0.07 * B
 * - Writes result back to same bucket under key: "grayscale-{key}".
 *
 * NOTE: SAAF Inspector is now fully integrated to capture detailed system,
 * container, CPU, and memory metrics, replacing simple runtime measurement.
 */
public class Handler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final AmazonS3 s3 = AmazonS3ClientBuilder.standard()
            .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
            .build();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        // --- SAAF INSPECTOR INTEGRATION START ---
        Inspector inspector = new Inspector();

        try {
            // Run initial inspections (platform, container, memory, cpu)
            inspector.inspectAll();
            // --- SAAF INSPECTOR INTEGRATION END ---

            if (input == null) {
                // Passed inspector instead of startTime
                return withMetricsError(inspector, "Input map is null");
            }
            Object bucketObj = input.get("bucket");
            Object keyObj = input.get("key");
            if (bucketObj == null || keyObj == null) {
                // Passed inspector instead of startTime
                return withMetricsError(inspector, "Missing bucket or key");
            }
            String bucket = bucketObj.toString();
            String key = keyObj.toString();

            ObjectMetadata metadata = s3.getObjectMetadata(bucket, key);
            if (metadata != null) {
                metadata.getContentLength();
            }
            long contentLength = -1L;

            InputStream imageInputStream = null;
            File tempFile = null;
            try {
                if (shouldUseTmp(contentLength)) {
                    tempFile = Files.createTempFile("s3img-", "-" + new java.io.File(key).getName()).toFile();
                    try (InputStream s3is = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                         OutputStream fos = new FileOutputStream(tempFile)) {
                        copyStream(s3is, fos);
                    }
                    imageInputStream = new FileInputStream(tempFile);
                } else {
                    imageInputStream = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                }

                String formatName = detectFormatName(imageInputStream);
                if (formatName == null) {
                    // Passed inspector instead of startTime
                    return withMetricsError(inspector, "Unsupported or unknown image format for key: " + key);
                }

                if (imageInputStream.markSupported()) {
                    imageInputStream.reset();
                } else {
                    if (tempFile != null) {
                        imageInputStream.close();
                        imageInputStream = new FileInputStream(tempFile);
                    } else {
                        imageInputStream.close();
                        imageInputStream = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                    }
                }

                BufferedImage src = ImageIO.read(imageInputStream);
                if (src == null) {
                    // Passed inspector instead of startTime
                    return withMetricsError(inspector, "Failed to decode image, invalid image data for key: " + key);
                }

                int w = src.getWidth();
                int h = src.getHeight();
                BufferedImage dest = new BufferedImage(w, h, BufferedImage.TYPE_BYTE_GRAY);

                // Pixel-by-pixel grayscale using luminosity formula
                for (int y = 0; y < h; y++) {
                    for (int x = 0; x < w; x++) {
                        int rgb = src.getRGB(x, y);
                        int a = (rgb >> 24) & 0xff;
                        int r = (rgb >> 16) & 0xff;
                        int g = (rgb >> 8) & 0xff;
                        int b = rgb & 0xff;
                        int gray = (int) Math.round(0.21 * r + 0.72 * g + 0.07 * b);
                        int grayRgb = (a << 24) | (gray << 16) | (gray << 8) | gray;
                        dest.setRGB(x, y, grayRgb);
                    }
                }

                String outKey = "grayscale-" + key;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                boolean written = ImageIO.write(dest, formatName, baos);
                if (!written) {
                    baos.reset();
                    ImageIO.write(dest, "png", baos);
                    formatName = "png";
                }
                byte[] outBytes = baos.toByteArray();
                ObjectMetadata outMeta = new ObjectMetadata();
                outMeta.setContentLength(outBytes.length);
                outMeta.setContentType(metadata.getContentType() != null ? metadata.getContentType() : ("image/" + formatName));
                try (InputStream putIs = new ByteArrayInputStream(outBytes)) {
                    s3.putObject(new PutObjectRequest(bucket, outKey, putIs, outMeta));
                }

                // --- SAAF INSPECTOR INTEGRATION START (SUCCESS) ---
                inspector.inspectAllDeltas();

                // Finalize inspector, calculating total runtime and returning the full metrics map
                Map<String, Object> out = inspector.finish();
                out.put("outputKey", outKey);
                return out;
                // --- SAAF INSPECTOR INTEGRATION END (SUCCESS) ---

            } finally {
                if (imageInputStream != null) {
                    try { imageInputStream.close(); } catch (IOException ignored) {}
                }
                if (tempFile != null && tempFile.exists()) {
                    try { tempFile.delete(); } catch (Exception ignored) {}
                }
            }

        } catch (Exception e) {
            // --- SAAF INSPECTOR INTEGRATION START (ERROR) ---
            return withMetricsError(inspector, "Exception: " + e.getMessage());
            // --- SAAF INSPECTOR INTEGRATION END (ERROR) ---
        }
    }

    /**
     * Utility: return metrics map containing error message, now integrated with Inspector.
     * @param inspector The initialized SAAF Inspector object.
     * @param message The error message to be included.
     * @return The final map containing all SAAF metrics and the error message.
     */
    private Map<String, Object> withMetricsError(Inspector inspector, String message) {
        // Run final deltas and finalize the Inspector
        inspector.inspectAllDeltas();
        Map<String, Object> out = inspector.finish();
        out.put("error", message);
        return out;
    }

    // All other helper methods remain the same as they do not directly interact with SAAF metrics

    private boolean shouldUseTmp(long contentLength) {
        if (contentLength <= 0) return false;
        long freeHeap = Runtime.getRuntime().freeMemory();
        return contentLength > (freeHeap / 2);
    }

    private void copyStream(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[8192];
        int r;
        while ((r = is.read(buf)) != -1) {
            os.write(buf, 0, r);
        }
    }

    private String detectFormatName(InputStream is) throws IOException {
        if (!is.markSupported()) {
            is = new BufferedInputStream(is);
        }
        is.mark(32 * 1024);
        try (ImageInputStream iis = ImageIO.createImageInputStream(is)) {
            if (iis == null) return null;
            Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
            if (!readers.hasNext()) {
                is.reset();
                return null;
            }
            ImageReader reader = readers.next();
            String fmt = reader.getFormatName();
            reader.dispose();
            is.reset();
            return fmt == null ? null : fmt.toLowerCase();
        } catch (IOException ex) {
            try { is.reset(); } catch (Exception ignored) {}
            return null;
        }
    }
}