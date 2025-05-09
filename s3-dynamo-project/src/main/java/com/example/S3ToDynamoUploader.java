package com.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class S3ToDynamoUploader {
    private final S3Client s3 = S3Client.create();
    private final DynamoDbClient dynamoDb = DynamoDbClient.create();

    public void uploadJsonToDynamo(String bucket, String key, String tableName) throws Exception {
        InputStream s3Object = s3.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build());
        ObjectMapper mapper = new ObjectMapper();
        JsonNode array = mapper.readTree(s3Object);

        for (JsonNode node : array) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("id", AttributeValue.builder().s(node.get("id").asText()).build());
            item.put("name", AttributeValue.builder().s(node.get("name").asText()).build());
            item.put("age", AttributeValue.builder().n(node.get("age").asText()).build());

            dynamoDb.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build());
        }
    }
}
