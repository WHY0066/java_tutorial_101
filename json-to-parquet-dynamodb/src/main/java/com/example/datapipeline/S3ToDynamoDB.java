package com.example.datapipeline;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class S3ToDynamoDB {

    private static final Logger logger = LogManager.getLogger(S3ToDynamoDB.class);

    
    public static void run(String region, String bucketName, String jsonKey, String tableName) {
        
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .build();

            S3Object s3Object = s3Client.getObject(bucketName, jsonKey);
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()));

            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            String jsonString = sb.toString();

           

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonString);

            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(region)
                    .build();
            DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
            Table table = dynamoDB.getTable(tableName);

            

            int successCount = 0;
            int failCount = 0;

            for (JsonNode node : rootNode) {
                try {
                    String id = node.get("id").asText();
                    String name = node.get("name").asText();
                    int age = node.get("age").asInt();

                    Item item = new Item()
                            .withPrimaryKey("id", id)
                            .withString("name", name)
                            .withNumber("age", age);

                    table.putItem(item);
                    successCount++;
                    logger.info("id={}", id);
                } catch (Exception e) {
                    failCount++;
                    logger.error("fail：record={}，error={}", node.toString(), e.getMessage());
                }
            }

            

        } catch (Exception e) {
            logger.error("error in S3ToDynamoDB：", e);
        }
    }

    
}
