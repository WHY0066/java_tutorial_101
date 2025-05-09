package com.example;

public class App {
    public static void main(String[] args) throws Exception {
        String jsonPath = "data.json";
        String parquetPath = "data.parquet";
        String bucketName = "your-s3-bucket";
        String parquetKey = "parquet/data.parquet";
        String jsonKey = "json/data.json";
        String dynamoTable = "your-dynamo-table";

        JsonToParquetConverter converter = new JsonToParquetConverter();
        converter.convertJsonToParquet(jsonPath, parquetPath, converter.getSchema());

        S3Uploader uploader = new S3Uploader();
        uploader.uploadToS3(bucketName, parquetKey, parquetPath);

        S3ToDynamoUploader jsonUploader = new S3ToDynamoUploader();
        jsonUploader.uploadJsonToDynamo(bucketName, jsonKey, dynamoTable);
    }
}
