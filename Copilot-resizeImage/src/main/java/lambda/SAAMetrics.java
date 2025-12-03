
// File: resize-image-lambda/src/main/java/lambda/SAAMetrics.java
package lambda;

import java.util.LinkedHashMap;
import java.util.Map;

public class SAAMetrics {
    private long runtimeMs = 0;

    public void setRuntime(long runtimeMs) {
        this.runtimeMs = runtimeMs;
    }

    public Map<String, Object> toMap() {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        m.put("runtimeMs", runtimeMs);
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        m.put("jvmTotalMemory", rt.totalMemory());
        m.put("jvmFreeMemory", rt.freeMemory());
        m.put("jvmUsedMemory", used);
        return m;
    }
}
