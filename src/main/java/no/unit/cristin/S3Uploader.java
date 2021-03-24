package no.unit.cristin;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.parallel.ParallelExecutionException;
import nva.commons.core.parallel.ParallelMapper;

public class S3Uploader {

    public static final int BATCH_SIZE = 100;
    public static final String S3_BUCKET = "BUCKET";
    private final S3Driver s3Driver;

    @JacocoGenerated
    public S3Uploader() {
        this(defaultS3Driver());
    }

    public S3Uploader(S3Driver s3Driver) {
        this.s3Driver = s3Driver;
    }

    public void uploadFile(String entryId, String content) {
        s3Driver.insertFile(Path.of(entryId), content);
    }

    public List<String> uploadFiles(List<KeyValue> entries) throws InterruptedException {
        ParallelMapper<KeyValue, String> mapper =
            new ParallelMapper<>(entries, this::uploadEntry, BATCH_SIZE).run();
        return collectFailedInsertionIds(mapper);
    }

    @JacocoGenerated
    private static S3Driver defaultS3Driver() {
        Environment environment = new Environment();
        String bucketName = environment.readEnv(S3_BUCKET);
        return S3Driver.fromPermanentCredentialsInEnvironment(bucketName);
    }

    private List<String> collectFailedInsertionIds(ParallelMapper<KeyValue, String> mapper) {
        return mapper.getExceptions().stream()
                   .map(ParallelExecutionException::getInput)
                   .map(inputObject -> (KeyValue) inputObject)
                   .map(KeyValue::getKey)
                   .collect(Collectors.toList());
    }

    private String uploadEntry(KeyValue keyValue) {
        s3Driver.insertFile(Path.of(keyValue.getKey()), keyValue.getValue());
        return keyValue.getKey();
    }
}
