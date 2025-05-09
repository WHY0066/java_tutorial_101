package com.example;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.nio.file.Paths;

public class S3Uploader {
    private final S3Client s3 = S3Client.create();

    public void uploadToS3(String bucket, String key, String localFilePath) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();

        s3.putObject(request, Paths.get(localFilePath));
    }
}
