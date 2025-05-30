package org.distributed.shardingjh.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;

import java.util.*;

@Slf4j
public class SignatureUtil {

    public static String toCanonicalJson(Object obj, ObjectMapper objectMapper) throws JsonProcessingException {
        // Convert to a Map, then recursively sort keys
        Map<String, Object> map = objectMapper.convertValue(obj, new TypeReference<>() {});
        Object canonical = recursivelySort(map);
        log.info("toCanonicalJson called with object: {}", canonical);
        return objectMapper.writeValueAsString(canonical);
    }

    private static Object recursivelySort(Object value) {
        if (value instanceof Map<?, ?>) {
            TreeMap<String, Object> sorted = new TreeMap<>();
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) value).entrySet()) {
                String key = entry.getKey().toString();
                sorted.put(key, recursivelySort(entry.getValue()));
            }
            return sorted;
        } else if (value instanceof Iterable<?>) {
            List<Object> list = new ArrayList<>();
            for (Object item : (Iterable<?>) value) {
                list.add(recursivelySort(item));
            }
            return list;
        } else if (value instanceof Object[]) {
            List<Object> list = new ArrayList<>();
            for (Object item : (Object[]) value) {
                list.add(recursivelySort(item));
            }
            return list;
        } else {
            return value;
        }
    }
}
