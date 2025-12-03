
// File: rotate-image-lambda/src/main/java/lambda/SAAMetrics.java
package org.example;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Minimal SAAF metrics implementation compatible with the required snippet.
 * You can replace this with your canonical SAAF library later.
 */
public class SAAMetrics {
    private long runtimeMs = 0;

    public void setRuntime(long runtimeMs) {
        this.runtimeMs = runtimeMs;
    }

    public Map<String, Object> toMap() {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        m.put("runtimeMs", runtimeMs);
        // Add a few lightweight environment metrics
        Runtime rt = Runtime.getRuntime();
        long used = rt.totalMemory() - rt.freeMemory();
        m.put("jvmTotalMemory", rt.totalMemory());
        m.put("jvmFreeMemory", rt.freeMemory());
        m.put("jvmUsedMemory", used);
        return m;
    }
}
