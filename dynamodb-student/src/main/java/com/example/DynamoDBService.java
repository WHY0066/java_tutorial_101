package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.*;
import software.amazon.awssdk.services.dynamodb.model.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;

public class DynamoDBService {
    private static final Logger logger = LogManager.getLogger(DynamoDBService.class);
    private final DynamoDbClient ddb;
    private final String tableName = "student";

    // 构造方法中初始化 DynamoDbClient
    public DynamoDBService() {
        ddb = DynamoDbClient.builder()
                .region(Region.US_WEST_2) 
                .build();
    }

    // 插入学生数据
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
            System.out.println("Inserting student: " + id);  // 输出到命令行
            logger.info("Student added: ID={} Name={} School={}", id, name, school);  // 打印日志
        } catch (DynamoDbException e) {
            logger.error("Failed to add student", e);
        }
    }

    // 查询学生数据
    public void getStudent(String id) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("student_id", AttributeValue.builder().s(id).build());
    
        GetItemRequest request = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();
    
        try {
            Map<String, AttributeValue> item = ddb.getItem(request).item();
            if (item != null) {
                // 打印日志，直接输出学生信息
                String studentId = item.get("student_id").s();
                String studentName = item.get("student_name").s();
                String schoolId = item.get("school_id").s();
                
                logger.info("Student found: ID={} Name={} School={}", studentId, studentName, schoolId);  // 打印日志
            } else {
                logger.warn("Student not found: " + id);
            }
        } catch (DynamoDbException e) {
            logger.error("Error getting student", e);
        }
    }
}
