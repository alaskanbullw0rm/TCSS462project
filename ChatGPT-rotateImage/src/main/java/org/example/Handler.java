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
// SAAMetrics is removed/replaced by Inspector

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;
import java.awt.Graphics2D;
import java.awt.geom.AffineTransform;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.nio.file.Files;

/**
 * Lambda handler for rotateImage.
 * - Reads an image from S3 (bucket/key).
 * - Rotates it 90 degrees clockwise.
 * - Preserves original image format.
 * - Writes result back to same bucket under key: "rotated-{key}".
 *
 * Fault tolerance:
 * - Checks missing bucket/key
 * - Catches invalid images
 * - Uses /tmp fallback if content size indicates insufficient heap
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

            // Validate input
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

            // Request object metadata to check size
            ObjectMetadata metadata = s3.getObjectMetadata(bucket, key);
            if (metadata != null) {
                metadata.getContentLength();
            }
            long contentLength = -1L;

            // Decide whether to stream via /tmp (if object is large relative to free heap)
            InputStream imageInputStream = null;
            File tempFile = null;
            try {
                if (shouldUseTmp(contentLength)) {
                    // Save to /tmp
                    tempFile = Files.createTempFile("s3img-", "-" + new File(key).getName()).toFile();
                    try (InputStream s3is = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                         OutputStream fos = new FileOutputStream(tempFile)) {
                        copyStream(s3is, fos);
                    }
                    imageInputStream = new FileInputStream(tempFile);
                } else {
                    // Stream directly
                    imageInputStream = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                }

                // Determine format
                String formatName = detectFormatName(imageInputStream);
                if (formatName == null) {
                    // Passed inspector instead of startTime
                    return withMetricsError(inspector, "Unsupported or unknown image format for key: " + key);
                }

                // Reset stream (if possible). If detection consumed stream by wrapping, reopen it
                if (imageInputStream.markSupported()) {
                    imageInputStream.reset();
                } else {
                    // If we used /tmp, we can just re-open
                    if (tempFile != null) {
                        imageInputStream.close();
                        imageInputStream = new FileInputStream(tempFile);
                    } else {
                        // Re-open from S3
                        imageInputStream.close();
                        imageInputStream = s3.getObject(new GetObjectRequest(bucket, key)).getObjectContent();
                    }
                }

                // Read image into BufferedImage
                BufferedImage src = ImageIO.read(imageInputStream);
                if (src == null) {
                    // Passed inspector instead of startTime
                    return withMetricsError(inspector, "Failed to decode image, invalid image data for key: " + key);
                }

                // Rotate 90 degrees clockwise: create dest with swapped width/height
                int w = src.getWidth();
                int h = src.getHeight();
                BufferedImage dest = new BufferedImage(h, w, src.getType() == 0 ? BufferedImage.TYPE_INT_ARGB : src.getType());
                Graphics2D g2d = dest.createGraphics();
                // 90 degrees clockwise: translate then rotate
                AffineTransform at = new AffineTransform();
                at.translate(h, 0);
                at.rotate(Math.toRadians(90));
                g2d.setTransform(at);
                g2d.drawImage(src, 0, 0, null);
                g2d.dispose();

                // Write result to S3
                String outKey = "rotated-" + key;
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                boolean written = ImageIO.write(dest, formatName, baos);
                if (!written) {
                    // fallback to PNG if format unsupported for writing
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
                    // try to clean up tmp file
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

    // Decide whether to write to /tmp based on content length and available heap
    private boolean shouldUseTmp(long contentLength) {
        if (contentLength <= 0) return false; // unknown -> stream
        long freeHeap = Runtime.getRuntime().freeMemory();
        // If object larger than 50% of free heap, prefer /tmp
        return contentLength > (freeHeap / 2);
    }

    // Copy stream helper
    private void copyStream(InputStream is, OutputStream os) throws IOException {
        byte[] buf = new byte[8192];
        int r;
        while ((r = is.read(buf)) != -1) {
            os.write(buf, 0, r);
        }
    }

    // Detect image format using ImageIO readers; returns lower-case format name (e.g., "png", "jpeg")
    private String detectFormatName(InputStream is) throws IOException {
        // Wrap to mark/reset if possible
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