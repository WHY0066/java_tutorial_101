package com.example;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.util.List;

public class App {
    public static void main(String[] args) {
        String tableName = "Users";

        // 设置区域（与你的 AWS CLI 配置一致）
        Region region = Region.US_WEST_2; // 替换成你的区域
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();

        // 检查表是否已存在
        ListTablesResponse existingTables = ddb.listTables();
        if (existingTables.tableNames().contains(tableName)) {
            System.out.println("Table '" + tableName + "' already exists.");
            return;
        }

        // 创建表请求
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName(tableName)
                .keySchema(KeySchemaElement.builder()
                        .attributeName("UserId")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("UserId")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST) // 按需计费（不需要手动设置吞吐量）
                .build();

        // 创建表
        try {
            CreateTableResponse response = ddb.createTable(request);
            System.out.println("Created table: " + response.tableDescription().tableName());
        } catch (DynamoDbException e) {
            System.err.println("Failed to create table: " + e.getMessage());
        }
    }
}


/**
 * Hello world!
 *
 
public class App 
{
   
   public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
    }
    

}*/
