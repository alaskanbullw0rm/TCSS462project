// Generated with Gemini 3.5 Pro

package lambda;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import javax.imageio.ImageIO;
import java.awt.Graphics2D;
import java.awt.RenderingHints;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Handler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.create();
    private static final int TARGET_WIDTH = 128;
    private static final int TARGET_HEIGHT = 128;

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        long startTime = System.currentTimeMillis();
        
        String bucket = (String) input.get("bucket");
        String key = (String) input.get("key");
        Map<String, Object> response = new HashMap<>();

        try {
            if (bucket == null || key == null) {
                throw new IllegalArgumentException("Missing bucket or key");
            }

            // 1. Read
            ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            BufferedImage originalImage = ImageIO.read(s3Stream);
            if (originalImage == null) throw new IOException("Invalid image format");

            // 2. Resize (Bilinear)
            BufferedImage resizedImage = new BufferedImage(TARGET_WIDTH, TARGET_HEIGHT, originalImage.getType());
            Graphics2D g2d = resizedImage.createGraphics();
            
            // Apply Bilinear Interpolation rule
            g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_BILINEAR);
            g2d.drawImage(originalImage, 0, 0, TARGET_WIDTH, TARGET_HEIGHT, null);
            g2d.dispose();

            // 3. Write
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            String format = key.contains(".") ? key.substring(key.lastIndexOf(".") + 1) : "png";
            ImageIO.write(resizedImage, format, os);
            
            String newKey = "resized-" + key;
            s3Client.putObject(PutObjectRequest.builder().bucket(bucket).key(newKey).build(), 
                    RequestBody.fromBytes(os.toByteArray()));

            response.put("outputKey", newKey);

        } catch (Exception e) {
            e.printStackTrace();
            response.put("error", e.getMessage());
        }

        long endTime = System.currentTimeMillis();

        SAAMetrics metrics = new SAAMetrics();
        metrics.setRuntime(endTime - startTime);
        
        Map<String, Object> metricsMap = metrics.toMap();
        response.putAll(metricsMap);
        
        return response;
    }
}
