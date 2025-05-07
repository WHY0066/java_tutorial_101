package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.*;

public class DynamoDBService implements AutoCloseable {
    private static final Logger logger = LogManager.getLogger(DynamoDBService.class);
    private final DynamoDbClient ddb;
    private final String tableName = "student";
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void close() {
        ddb.close();
    }

    public DynamoDBService() {
        ddb = DynamoDbClient.builder()
                .region(Region.US_WEST_2)
                .build();
    }

    public void addStudent(String id, String name, String school) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("student_id", AttributeValue.builder().s(id).build());
        item.put("student_name", AttributeValue.builder().s(name).build());
        item.put("school_id", AttributeValue.builder().s(school).build());

        PutItemRequest request = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .build();

        try {
            ddb.putItem(request);
            logger.info("Student added: ID={} Name={} School={}", id, name, school);
        } catch (DynamoDbException e) {
            logger.error("Failed to add student", e);
        }
    }
    public String getStudent(String id) {
        Map<String, AttributeValue> key = Map.of(
            "student_id", AttributeValue.builder().s(id).build()
        );
    
        try {
            GetItemResponse response = ddb.getItem(GetItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .build());
    
            if (response.hasItem()) {
                Map<String, AttributeValue> item = response.item();
                ObjectNode json = objectMapper.createObjectNode();
                json.put("student_id", item.get("student_id").s());
                json.put("student_name", item.get("student_name").s());
                json.put("school_id", item.get("school_id").s());
    
                String resultJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                logger.info( resultJson);
                return resultJson;
            } else {
                String notFoundMsg = "Student " + id + " not found";
                logger.warn( notFoundMsg); 
                return "{\"error\": \"" + notFoundMsg + "\"}";
            }
        } catch (Exception e) {
            logger.error( e.getMessage(), e); 
            return "{\"error\": \"" + e.getMessage().replace("\"", "'") + "\"}";
        }
    }
    
}

