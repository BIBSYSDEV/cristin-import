import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import nva.commons.core.JsonUtils;
import org.junit.jupiter.api.Test;

public class HttpTest {



    @Test
    public void getHttp() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://api.dev.nva.aws.unit.no/s3/"))
                                  .header("X-API-Key","Q7jXSnrQ2K46kEe6axpVQ3sQa2haeHsU8LhfzgiL")
                                  .GET()
                                  .build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(response.statusCode(),is(equalTo(HttpURLConnection.HTTP_OK)));
    }


    @Test
    public void putHttp() throws IOException, InterruptedException {
        HttpClient httpClient = HttpClient.newHttpClient();
        ObjectNode body = JsonUtils.objectMapper.createObjectNode();
        String randomString = UUID.randomUUID().toString();
        body.put("key","value");
        HttpRequest request = HttpRequest.newBuilder(URI.create("https://api.dev.nva.aws.unit.no/s3/orestis-test/"+randomString))
                                  .header("X-API-Key","Q7jXSnrQ2K46kEe6axpVQ3sQa2haeHsU8LhfzgiL")
                                  .header("Content-Type","application/json")
                                  .PUT(BodyPublishers.ofString(body.toPrettyString()))
                                  .build();
        HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString(StandardCharsets.UTF_8));
        assertThat(response.statusCode(),is(equalTo(HttpURLConnection.HTTP_OK)));
    }

}
