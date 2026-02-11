package helloworld;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

/**
 * PackageName : helloworld
 * FileName    : GetUserByIdHandler
 * Author      : Baekgwa
 * Date        : 26. 2. 11.
 * Description : scan 기반. Full scan 과 동일하여 모든 데이터를 다 조사.
 * =====================================================================================================================
 * DATE          AUTHOR               NOTE
 * ---------------------------------------------------------------------------------------------------------------------
 * 26. 2. 11.     Baekgwa               Initial creation
 */
public class GetUserByIdHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

	private static final ObjectMapper objectMapper = new ObjectMapper();

	private static final DynamoDbClient client = createClient();

	// DynamoDbClient 생성 함수.
	// 프로파일에 따라 연결 대상이 달라짐
	private static DynamoDbClient createClient() {
		String profile = System.getenv("PROFILE");

		if ("dev".equals(profile)) {
			return DynamoDbClient.builder()
				.endpointOverride(URI.create("http://host.docker.internal:8000"))
				.region(Region.AP_NORTHEAST_2)
				.credentialsProvider(StaticCredentialsProvider.create(
					AwsBasicCredentials.create("local", "local")))
				.build();
		}

		return DynamoDbClient.builder()
			.httpClient(UrlConnectionHttpClient.builder().build())
			.credentialsProvider(EnvironmentVariableCredentialsProvider.create())
			.region(Region.AP_NORTHEAST_2)
			.build();
	}

	private static final String TABLE_NAME = System.getenv("TABLE_NAME");

	@Override
	public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent input, Context context) {
		String userId = input.getPathParameters() != null ? input.getPathParameters().get("userId") : null;

		if (userId == null) {
			return new APIGatewayProxyResponseEvent().withStatusCode(400).withBody("{\"error\": \"Missing userId\"}");
		}

		try {
			Map<String, AttributeValue> key = new HashMap<>();
			key.put("userId", AttributeValue.builder().s(userId).build());

			GetItemResponse response = client.getItem(GetItemRequest.builder()
				.tableName(TABLE_NAME)
				.key(key)
				.build());

			if (response.hasItem()) {
				Map<String, String> result = new HashMap<>();
				result.put("userId", response.item().get("userId").s());
				result.put("name", response.item().get("name").s());
				result.put("date", response.item().get("date").s());

				return new APIGatewayProxyResponseEvent().withStatusCode(200)
					.withBody(objectMapper.writeValueAsString(result));
			} else {
				return new APIGatewayProxyResponseEvent().withStatusCode(404).withBody("{\"error\": \"User not found\"}");
			}
		} catch (Exception e) {
			return new APIGatewayProxyResponseEvent().withStatusCode(500).withBody("{\"error\": \"" + e.getMessage() + "\"}");
		}
	}
}
