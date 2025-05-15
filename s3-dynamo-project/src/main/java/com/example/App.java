package com.example;

public class App {
    public static void main(String[] args) throws Exception {
        String jsonPath = "src/main/resources/data.json";
        String parquetPath = "output/output.parquet";

        String bucketName = "your-s3-bucket-name";
        String parquetS3Key = "parquet/output.parquet";
        String jsonS3Key = "data/data.json";

        String tableName = "your-dynamodb-table-name";

        // 1. JSON 转 Parquet
        JsonToParquet.convert(jsonPath, parquetPath);

        // 2. 上传 Parquet 到 S3
        S3Uploader.uploadToS3(bucketName, parquetS3Key, parquetPath);

        // 3. 上传 JSON 到 S3
        S3Uploader.uploadToS3(bucketName, jsonS3Key, jsonPath);

        // 4. 从 S3 上传 JSON 到 DynamoDB
        S3ToDynamoUploader.uploadJsonFromS3(bucketName, jsonS3Key, tableName);
    }
}
