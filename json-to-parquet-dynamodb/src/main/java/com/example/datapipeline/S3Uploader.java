package com.example.datapipeline;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.file.Paths;

public class S3Uploader {
    private static final Logger logger = LogManager.getLogger(S3Uploader.class);

    private final S3Client s3Client;

    public S3Uploader() {
        this.s3Client = S3Client.builder()
                .region(Region.EU_NORTH_1)  // 根据你的bucket区域修改
                .credentialsProvider(ProfileCredentialsProvider.create()) // 默认凭证配置
                .build();
    }

    public void uploadFile(String bucketName, String key, String filePath) {
        try {
            PutObjectRequest request = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3Client.putObject(request, RequestBody.fromFile(Paths.get(filePath)));
            logger.info("Parquet 文件上传成功: s3://{}/{}", bucketName, key);
        } catch (Exception e) {
            logger.error("上传 Parquet 文件失败", e);
            throw e;
        }
    }

    public void close() {
        s3Client.close();
    }
}
