package helloworld;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * PackageName : helloworld
 * FileName    : GetUserByNameScanHandler
 * Author      : Baekgwa
 * Date        : 26. 2. 11.
 * Description : 
 * =====================================================================================================================
 * DATE          AUTHOR               NOTE
 * ---------------------------------------------------------------------------------------------------------------------
 * 26. 2. 11.     Baekgwa               Initial creation
 */
public class GetUserByNameScanHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

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
		String searchName = null;
		if (input.getQueryStringParameters() != null) {
			searchName = input.getQueryStringParameters().get("name");
		}

		if (searchName == null || searchName.trim().isEmpty()) {
			return new APIGatewayProxyResponseEvent()
				.withStatusCode(400)
				.withBody("{\"error\": \"Missing required query parameter: 'name'\"}");
		}

		try {
			ScanRequest scanRequest = ScanRequest.builder()
				.tableName(TABLE_NAME)
				.filterExpression("#name = :nameVal")
				.expressionAttributeNames(
					Map.of("#name", "name"))
				.expressionAttributeValues(
					Map.of(":nameVal", AttributeValue.builder().s(searchName).build()))
				.build();

			ScanResponse response = client.scan(scanRequest);

			List<Map<String, String>> users = new ArrayList<>();
			for (Map<String, AttributeValue> item : response.items()) {
				Map<String, String> user = new HashMap<>();
				user.put("userId", item.get("userId").s());
				user.put("name", item.get("name").s());
				user.put("date", item.get("date").s());
				users.add(user);
			}

			return new APIGatewayProxyResponseEvent()
				.withStatusCode(200)
				.withBody(objectMapper.writeValueAsString(users));

		} catch (Exception e) {
			context.getLogger().log("Error scanning by name: " + e.getMessage());
			return new APIGatewayProxyResponseEvent()
				.withStatusCode(500)
				.withBody("{\"error\": \"" + e.getMessage() + "\"}");
		}
	}
}
