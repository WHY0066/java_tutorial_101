package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {
    private static final Logger logger = LogManager.getLogger(App.class);
    
    public static void main(String[] args) {
        String jsonPath = "src/main/resources/data.json";
        String parquetPath = "output/output.parquet";
        String bucketName = "s3exercisebucketwhy";
        String parquetS3Key = "parquet/output.parquet";
        String jsonS3Key = "data/data.json";
        String tableName = "s3_exercise";
        
        try (S3Uploader s3Uploader = new S3Uploader();
             S3ToDynamoUploader dynamoUploader = new S3ToDynamoUploader()) {
            
            // 1. 创建DynamoDB表
            dynamoUploader.createTableIfNotExists(tableName);
            logger.info("[DynamoDB] 表创建/验证完成: {}", tableName);
            
            // 2. JSON转Parquet
            JsonToParquet.convert(jsonPath, parquetPath);
            logger.info("[Converter] JSON转Parquet完成");
            
            // 3. 上传Parquet到S3
            s3Uploader.uploadToS3(bucketName, parquetS3Key, parquetPath);
            logger.info("[S3] Parquet文件上传完成");
            
            // 4. 上传JSON到S3
            s3Uploader.uploadToS3(bucketName, jsonS3Key, jsonPath);
            logger.info("[S3] JSON文件上传完成");
            
            // 5. 从S3导入数据到DynamoDB
            int importedCount = dynamoUploader.uploadJsonToDynamo(bucketName, jsonS3Key, tableName);
            logger.info("[DynamoDB] 数据导入完成 ({}条记录)", importedCount);
            
        } catch (Exception e) {
            logger.error("[System] 程序执行失败: {}", e.getMessage());
            System.exit(1);
        }
    }
}