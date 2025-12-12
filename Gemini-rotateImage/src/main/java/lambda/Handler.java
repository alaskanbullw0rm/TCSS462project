// Generated with Google Gemini 3.5

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
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import saaf.Inspector;

public class Handler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.create();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        long startTime = System.currentTimeMillis();
        
        //Added manually
        Inspector inspector = new Inspector();
        inspector.inspectAll();
        
        
        String bucket = (String) input.get("bucket");
        String key = (String) input.get("key");
        Map<String, Object> response = new HashMap<>();

        try {
            if (bucket == null || key == null) {
                throw new IllegalArgumentException("Missing bucket or key in input JSON");
            }

            // 1. Read Image from S3
            GetObjectRequest getRequest = GetObjectRequest.builder().bucket(bucket).key(key).build();
            ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(getRequest);
            
            BufferedImage originalImage = ImageIO.read(s3Stream);
            if (originalImage == null) {
                throw new IOException("Failed to read image. Format might be unsupported.");
            }

            // 2. Rotate Logic (90 degrees clockwise)
            int width = originalImage.getWidth();
            int height = originalImage.getHeight();
            BufferedImage rotatedImage = new BufferedImage(height, width, originalImage.getType());
            
            Graphics2D g2d = rotatedImage.createGraphics();
            // Move origin to top right for 90 degree rotation
            g2d.translate((height - width) / 2, (height - width) / 2);
            g2d.rotate(Math.toRadians(90), height / 2.0, width / 2.0);
            g2d.drawRenderedImage(originalImage, null);
            g2d.dispose();

            // 3. Write back to S3
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            // Determine format (default to png if unknown, or extract extension)
            String format = key.contains(".") ? key.substring(key.lastIndexOf(".") + 1) : "png";
            ImageIO.write(rotatedImage, format, os);
            
            byte[] imageBytes = os.toByteArray();
            String newKey = "rotated-" + key;
            
            PutObjectRequest putRequest = PutObjectRequest.builder().bucket(bucket).key(newKey).build();
            s3Client.putObject(putRequest, RequestBody.fromBytes(imageBytes));

            response.put("outputKey", newKey);

        } catch (Exception e) {
            e.printStackTrace();
            response.put("error", e.getMessage());
        }

        long endTime = System.currentTimeMillis();

        SAAMetrics metrics = new SAAMetrics();
        metrics.setRuntime(endTime - startTime);
        
        // Merge metrics into response
        Map<String, Object> metricsMap = metrics.toMap();
        response.putAll(metricsMap);
        
        //return response;
        
        // added manually
        inspector.inspectAllDeltas();
        return inspector.finish();
    }
}
