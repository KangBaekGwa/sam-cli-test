package helloworld;

import java.net.URI;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

/**
 * PackageName : helloworld
 * FileName    : CreateUserHandler
 * Author      : Baekgwa
 * Date        : 26. 2. 10.
 * Description : 
 * =====================================================================================================================
 * DATE          AUTHOR               NOTE
 * ---------------------------------------------------------------------------------------------------------------------
 * 26. 2. 10.     Baekgwa               Initial creation
 */
public class CreateUserHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

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
		context.getLogger().log("Handler started. Input Body: " + input.getBody());

		try {
			UserRequestDto userRequest = objectMapper.readValue(input.getBody(), UserRequestDto.class);

			String generatedUserId = UUID.randomUUID().toString();
			String generatedDate = LocalDateTime.now().toString();

			Map<String, AttributeValue> mainItem = new HashMap<>();
			mainItem.put("userId", AttributeValue.builder().s(generatedUserId).build());
			mainItem.put("name", AttributeValue.builder().s(userRequest.getName()).build());
			mainItem.put("date", AttributeValue.builder().s(generatedDate).build());

			Map<String, AttributeValue> markerItem = new HashMap<>();
			markerItem.put("userId", AttributeValue.builder().s("NAME#" + userRequest.getName()).build());

			TransactWriteItem putMain = TransactWriteItem.builder()
				.put(Put.builder()
					.tableName(TABLE_NAME)
					.item(mainItem)
					.build())
				.build();

			TransactWriteItem putMarker = TransactWriteItem.builder()
				.put(Put.builder()
					.tableName(TABLE_NAME)
					.item(markerItem)
					.conditionExpression("attribute_not_exists(userId)")
					.build())
				.build();

			TransactWriteItemsRequest transactRequest = TransactWriteItemsRequest.builder()
				.transactItems(putMain, putMarker)
				.build();

			client.transactWriteItems(transactRequest);

			Map<String, String> responseBody = new HashMap<>();
			responseBody.put("message", "User saved successfully!");
			responseBody.put("userId", generatedUserId);
			responseBody.put("savedName", userRequest.getName());
			responseBody.put("createDate", generatedDate);

			return new APIGatewayProxyResponseEvent()
				.withStatusCode(200)
				.withBody(objectMapper.writeValueAsString(responseBody));

		} catch (TransactionCanceledException e) {
			context.getLogger().log("Transaction canceled (Duplicated Name): " + e.getMessage());
			return new APIGatewayProxyResponseEvent()
				.withStatusCode(409)
				.withBody("{ \"error\": \"Name already exists. Please choose another name.\" }");

		} catch (Exception e) {
			context.getLogger().log("Error: " + e.getMessage());
			return new APIGatewayProxyResponseEvent()
				.withStatusCode(500)
				.withBody("{ \"error\": \"Failed to create user: " + e.getMessage() + "\" }");
		}
	}

	public static class UserRequestDto {
		private String name;
		private String date;

		public UserRequestDto() {
		}

		public String getName() {
			return name;
		}

		public String getDate() {
			return date;
		}
	}
}