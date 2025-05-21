package org.distributed.shardingjh.config;

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
    );

    private final RestTemplate restTemplate = new RestTemplate();

    /**
     * Route by id: id.hashCode() % server count
     */
    public int getServerIndex(String id) {
        return Math.abs(id.hashCode()) % serverUrls.size();
    }

    public String getServerUrl(String id) {
        return serverUrls.get(getServerIndex(id));
    }

    /**
     * Forward a POST request with a body to the correct server
     */
    public <T> T forwardPost(String id, String endpointPath, Object requestBody, Class<T> responseType) {
        String url = getServerUrl(id) + endpointPath;
        return restTemplate.postForObject(URI.create(url), requestBody, responseType);
    }

    /**
     * Forward a GET request with query params to the correct server
     */
    public <T> T forwardGet(String id, String endpointWithQuery, Class<T> responseType) {
        String url = getServerUrl(id) + endpointWithQuery;
        return restTemplate.getForObject(URI.create(url), responseType);
    }

    /**
     * Forward a DELETE request to the correct server
     */
    public void forwardDelete(String id, String endpointPath) {
        String url = getServerUrl(id) + endpointPath;
        restTemplate.delete(URI.create(url));
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
