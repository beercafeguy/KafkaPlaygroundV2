package es.common;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Properties;

public class ESClientFactory {

    private ESClientFactory() {
    }

    public static RestHighLevelClient createClient(RequestType requestType) throws IOException {
        Properties properties;
        RestHighLevelClient client;
        if (requestType.equals(RequestType.LOCAL)) {
            properties = PropertyFactory.getAppProperties("application-local.properties");
            client = getLocalClient(properties);
        } else {
            properties = PropertyFactory.getAppProperties();
            client = getRemoteClient(properties);
        }
        return client;
    }

    private static RestHighLevelClient getLocalClient(Properties properties) {

        String hostname = properties.getProperty("es.hostname");
        String port = properties.getProperty("es.port");
        String scheme=properties.getProperty("es.scheme");
        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, Integer.parseInt(port),scheme)
        );
        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }

    private static RestHighLevelClient getRemoteClient(Properties properties) {

        String hostname = properties.getProperty("es.hostname");
        String port = properties.getProperty("es.port");
        String scheme = properties.getProperty("es.scheme");
        String username = properties.getProperty("es.username");
        String password = properties.getProperty("es.password");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder restClientBuilder = RestClient.builder(
                new HttpHost(hostname, Integer.parseInt(port), scheme)
        ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        RestHighLevelClient client = new RestHighLevelClient(restClientBuilder);
        return client;
    }
}
