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

    // ✅ 添加这个方法！
    public static void run(String region, String bucketName, String jsonKey, String tableName) {
        logger.info("程序启动，Region={}, Bucket={}, JSON文件={}, DynamoDB表={}",
                region, bucketName, jsonKey, tableName);

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

            logger.info("成功读取 S3 JSON，长度={} 字符", jsonString.length());

            ObjectMapper mapper = new ObjectMapper();
            JsonNode rootNode = mapper.readTree(jsonString);

            AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
                    .withRegion(region)
                    .build();
            DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);
            Table table = dynamoDB.getTable(tableName);

            if (!rootNode.isArray()) {
                logger.error("JSON 文件格式错误：应为数组");
                return;
            }

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
                    logger.info("写入成功：id={}", id);
                } catch (Exception e) {
                    failCount++;
                    logger.error("写入失败：记录={}，错误={}", node.toString(), e.getMessage());
                }
            }

            logger.info("上传完成，成功={} 条，失败={} 条", successCount, failCount);

        } catch (Exception e) {
            logger.error("程序运行异常：", e);
        }
    }

    // 可选保留 main 方法
    public static void main(String[] args) {
        run("eu-north-1", "s3exercisebucketwhy", "data/data.json", "tests3todynamodb");
    }
}
