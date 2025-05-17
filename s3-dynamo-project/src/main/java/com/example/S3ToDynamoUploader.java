package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class S3ToDynamoUploader implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(S3ToDynamoUploader.class);
    private final S3Client s3Client;
    private final DynamoDbClient dynamoDbClient;
    
    public S3ToDynamoUploader() {
        this.s3Client = S3Client.builder().build();
        this.dynamoDbClient = DynamoDbClient.builder().build();
        logger.debug("AWS客户端初始化完成");
    }
    
    public void createTableIfNotExists(String tableName) {
        try {
            DescribeTableRequest describeRequest = DescribeTableRequest.builder()
                .tableName(tableName)
                .build();
            
            try {
                dynamoDbClient.describeTable(describeRequest);
                logger.info("DynamoDB表已存在: {}", tableName);
            } catch (ResourceNotFoundException e) {
                CreateTableRequest createRequest = CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                    .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                    .billingMode(BillingMode.PAY_PER_REQUEST)
                    .build();
                
                dynamoDbClient.createTable(createRequest);
                logger.info("DynamoDB表创建请求已提交: {}", tableName);
                
                dynamoDbClient.waiter().waitUntilTableExists(describeRequest);
                logger.info("DynamoDB表已激活: {}", tableName);
            }
        } catch (Exception e) {
            logger.error("DynamoDB表操作失败", e);
            throw new RuntimeException("DynamoDB表操作失败: " + e.getMessage(), e);
        }
    }
    
    public int uploadJsonToDynamo(String bucketName, String key, String tableName) {
        int processedCount = 0;
        try (InputStream s3Object = s3Client.getObject(
            GetObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build())) {
            
            ObjectMapper mapper = new ObjectMapper();
            JsonNode jsonArray = mapper.readTree(s3Object);
            
            for (JsonNode node : jsonArray) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("id", AttributeValue.builder().s(node.get("id").asText()).build());
                item.put("name", AttributeValue.builder().s(node.get("name").asText()).build());
                item.put("age", AttributeValue.builder().n(String.valueOf(node.get("age").asInt())).build());
                
                dynamoDbClient.putItem(PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build());
                
                processedCount++;
            }
            logger.info("成功导入 {} 条记录到DynamoDB", processedCount);
            return processedCount;
        } catch (Exception e) {
            logger.error("DynamoDB数据导入失败", e);
            throw new RuntimeException("DynamoDB数据导入失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void close() {
        try {
            if (s3Client != null) s3Client.close();
            if (dynamoDbClient != null) dynamoDbClient.close();
            logger.debug("AWS客户端已关闭");
        } catch (Exception e) {
            logger.warn("关闭AWS客户端时出错", e);
        }
    }
}