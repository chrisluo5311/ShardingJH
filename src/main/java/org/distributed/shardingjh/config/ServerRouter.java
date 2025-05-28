package org.distributed.shardingjh.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.distributed.shardingjh.common.constant.ShardConst;
import org.distributed.shardingjh.p2p.FingerTable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.*;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.Map;

/**
 * {
 *  64:  "http://3.147.58.62:8081",
 *  128: "http://3.15.149.110:8082",
 *  192: "http://52.15.151.104:8083"
 * }
 * */
@Slf4j
@Component
public class ServerRouter {

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    FingerTable fingerTable;

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Route by Member id: id.hashCode() % 256
     * @param id the id of the member
     * @return the URL of the server to forward the request to
     */
    public String getMemberResponsibleServerUrl(String id) {
        int target = Math.abs(id.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        log.info("[P2P] Routing member id: {} (hash: {})", id, target);
        // Return the first node ≥ target or wrap around to the first node
        Map.Entry<Integer, String> entry = fingerTable.finger.ceilingEntry(target);
        return entry != null ? entry.getValue() : fingerTable.finger.firstEntry().getValue();
    }

    /**
     * Route by Order ID: id.hashCode() % 256
     * @param orderId the id of the order
     * @return the URL of the server to forward the request to
     * */
    public String getOrderResponsibleServerUrl(String orderId) {
        int target = Math.abs(orderId.hashCode()) % ShardConst.FINGER_MAX_RANGE;
        log.info("[P2P] Routing order id: {} (hash: {})", orderId, target);
        // Return the first node ≥ target or wrap around to the first node
        Map.Entry<Integer, String> entry = fingerTable.finger.ceilingEntry(target);
        return entry != null ? entry.getValue() : fingerTable.finger.firstEntry().getValue();
    }

    /**
     * Forward a POST request with a body to the correct server
     */
    public <T> T forwardPost(String serverUrl, String endpointPath, String fronEndSignature, Object requestBody, Class<T> responseType) throws JsonProcessingException {
        String finalUrl = serverUrl + endpointPath;
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Signature", fronEndSignature);
        String jsonBody = objectMapper.writeValueAsString(requestBody);

        HttpEntity<String> entity = new HttpEntity<>(jsonBody, headers);
        return restTemplate.postForObject(URI.create(finalUrl), entity, responseType);
    }

    /**
     * Forward a GET request with query params to the correct server
     */
    public <T> T forwardGet(String url, String endpointWithQuery, String fronEndSignature, Class<T> responseType) {
        String finalUrl = url + endpointWithQuery;
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Signature", fronEndSignature);
        HttpEntity<Void> entity = new HttpEntity<>(headers);

        ResponseEntity<T> response = restTemplate.exchange(
                URI.create(finalUrl),
                HttpMethod.GET,
                entity,
                responseType
        );
        return response.getBody();
    }


    /**
     * Forward a DELETE request to the correct server
     */
    public void forwardDelete(String url, String endpointPath, String fronEndSignature) {
        String finalUrl = url + endpointPath;
        HttpHeaders headers = new HttpHeaders();
        headers.set("X-Signature", fronEndSignature);

        HttpEntity<Void> entity = new HttpEntity<>(headers);

        restTemplate.exchange(
                URI.create(finalUrl),
                HttpMethod.DELETE,
                entity,
                Void.class
        );
    }

    /**
     * Forward a GET request to the correct server and return raw response
     */
    public <T> T forwardGetRaw(String url, String endpointPath, String frontEndSignature, ParameterizedTypeReference<T> responseType) {
        String finalUrl = url + endpointPath;
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("X-Signature", frontEndSignature);
            HttpEntity<Void> entity = new HttpEntity<>(headers);
            ResponseEntity<T> response = restTemplate.exchange(
                    URI.create(finalUrl),
                    HttpMethod.GET,
                    entity,
                    responseType
            );
            return response.getBody();
        } catch (Exception e) {
            log.warn("[forwardGetRaw] Failed to forward GET to {}: {}", finalUrl, e.getMessage());
            return null;
        }
    }
}
