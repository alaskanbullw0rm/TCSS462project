// Generated with Gemini 3.5 Pro

package lambda;

import java.util.HashMap;
import java.util.Map;

public class SAAMetrics {
    private long runtime;

    public void setRuntime(long runtime) {
        this.runtime = runtime;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("runtime", runtime);
        // Add other SAAF specific fields here if required by the framework
        // For this implementation, we return the runtime as the base metric
        return map;
    }
}
