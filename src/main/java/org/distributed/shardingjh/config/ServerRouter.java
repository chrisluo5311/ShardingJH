package org.distributed.shardingjh.config;

import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.net.URI;
import java.util.List;

@Slf4j
@Component
public class ServerRouter {

    private final List<String> serverUrls = List.of(
            "http://3.147.58.62:8081", // server 1
            "http://3.15.149.110:8082", // server 2
            "http://52.15.151.104:8083"  // server 3
//            "http://localhost:8081", // local server
//            "http://localhost:8082", // local server
//            "http://localhost:8083" // local server
    );

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Route by Member id: id.hashCode() % server count
     */
    public int getMemberServerIndex(String id) {
        return Math.abs(id.hashCode()) % serverUrls.size();
    }

    /**
     * Route by Order month range:
     * server 0 (index:0) -> Jan, Feb, Mar, Apr
     * server 1 (index:1) -> May, Jun, Jul, Aug
     * server 2 (index:2) -> Sep, Oct, Nov, Dec
     * */
    public int getOrderServerIndex(int month) {
        if (month >= 1 && month <= 4) {
            return 0; // server 0
        } else if (month >= 5 && month <= 8) {
            return 1; // server 1
        } else if (month >= 9 && month <= 12) {
            return 2; // server 2
        }
        throw new IllegalArgumentException("Invalid month: " + month);
    }

    public String getServerUrl(String id) {
        return serverUrls.get(getMemberServerIndex(id));
    }

    public String getServerUrlFromMonth(int month) {
        return serverUrls.get(getOrderServerIndex(month));
    }

    /**
     * Forward a POST request with a body to the correct server
     */
    public <T> T forwardPost(String id, String endpointPath, Object requestBody, Class<T> responseType) {
        String url = getServerUrl(id) + endpointPath;
        return restTemplate.postForObject(URI.create(url), requestBody, responseType);
    }

    public <T> T forwardOrderPost(int month, String endpointPath, Object requestBody, Class<T> responseType) {
        String url = getServerUrlFromMonth(month) + endpointPath;
        return restTemplate.postForObject(URI.create(url), requestBody, responseType);
    }

    /**
     * Forward a GET request with query params to the correct server
     */
    public <T> T forwardGet(String id, String endpointWithQuery, Class<T> responseType) {
        String url = getServerUrl(id) + endpointWithQuery;
        return restTemplate.getForObject(URI.create(url), responseType);
    }

    public <T> T forwardOrderGet(int month, String endpointWithQuery, Class<T> responseType) {
        String url = getServerUrlFromMonth(month) + endpointWithQuery;
        return restTemplate.getForObject(URI.create(url), responseType);
    }

    /**
     * Forward a DELETE request to the correct server
     */
    public void forwardDelete(String id, String endpointPath) {
        String url = getServerUrl(id) + endpointPath;
        restTemplate.delete(URI.create(url));
    }

    public void forwardOrderDelete(int month, String endpointPath, Object requestBody) {
        String url = getServerUrlFromMonth(month) + endpointPath;
        restTemplate.delete(URI.create(url).toString(), requestBody);
    }

    /**
     * Forward a GET request to the correct server and return raw response
     */
    public <T> T forwardGetRaw(int serverIndex, String endpointPath, ParameterizedTypeReference<T> responseType) {
        String url = serverUrls.get(serverIndex) + endpointPath;
        ResponseEntity<T> response = restTemplate.exchange(
                URI.create(url),
                HttpMethod.GET,
                null,
                responseType
        );
        return response.getBody();
    }
}
