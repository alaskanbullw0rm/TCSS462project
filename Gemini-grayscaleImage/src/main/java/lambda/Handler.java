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
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Handler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final S3Client s3Client = S3Client.create();

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> input, Context context) {
        long startTime = System.currentTimeMillis();
        
        String bucket = (String) input.get("bucket");
        String key = (String) input.get("key");
        Map<String, Object> response = new HashMap<>();

        try {
            if (bucket == null || key == null) throw new IllegalArgumentException("Missing bucket or key");

            // 1. Read
            ResponseInputStream<GetObjectResponse> s3Stream = s3Client.getObject(GetObjectRequest.builder()
                    .bucket(bucket).key(key).build());
            BufferedImage originalImage = ImageIO.read(s3Stream);
            if (originalImage == null) throw new IOException("Invalid image format");

            // 2. Grayscale (Pixel by Pixel with specific weights)
            int width = originalImage.getWidth();
            int height = originalImage.getHeight();
            
            // We keep the original image type to preserve alpha channels if present (e.g., PNG)
            BufferedImage grayImage = new BufferedImage(width, height, originalImage.getType());

            for (int y = 0; y < height; y++) {
                for (int x = 0; x < width; x++) {
                    int rgb = originalImage.getRGB(x, y);
                    Color color = new Color(rgb, true);

                    int r = color.getRed();
                    int g = color.getGreen();
                    int b = color.getBlue();
                    int a = color.getAlpha();

                    // Formula: 0.21 * R + 0.72 * G + 0.07 * B
                    int grayVal = (int) ((0.21 * r) + (0.72 * g) + (0.07 * b));
                    
                    // Clamp value 0-255 just in case
                    if (grayVal > 255) grayVal = 255;
                    if (grayVal < 0) grayVal = 0;

                    Color newColor = new Color(grayVal, grayVal, grayVal, a);
                    grayImage.setRGB(x, y, newColor.getRGB());
                }
            }

            // 3. Write
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            String format = key.contains(".") ? key.substring(key.lastIndexOf(".") + 1) : "png";
            ImageIO.write(grayImage, format, os);
            
            String newKey = "grayscale-" + key;
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
