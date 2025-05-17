package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.nio.file.Paths;

public class S3Uploader implements AutoCloseable {
    private final S3Client s3Client;
    
    public S3Uploader() {
        this.s3Client = S3Client.create();
    }
    
    public void uploadToS3(String bucketName, String key, String filePath) {
        try {
            s3Client.putObject(
                PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build(),
                Paths.get(filePath)
            );
        } catch (Exception e) {
            throw new RuntimeException("S3上传失败: " + e.getMessage(), e);
        }
    }
    
    @Override
    public void close() {
        if (s3Client != null) s3Client.close();
    }
}