package no.unit.cristin;

import static nva.commons.core.attempt.Try.attempt;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.parallel.ParallelExecutionException;
import nva.commons.core.parallel.ParallelMapper;

/**
 * Upload files to an S3 bucket. The name of the bucket is set thought the Environment variable: "BUCKET". The AWS
 * credentials are given as the Environment variables "AWS_ACCESS_KEY_ID" and "AWS_SECRET_ACCESS_KEY".
 */
public class S3Uploader {

    public static final String S3_BUCKET = "BUCKET";
    public static final int DEFAULT_BATCH_SIZE = 100;
    private final S3Driver s3Driver;
    private final Path folderPath;

    @JacocoGenerated
    public S3Uploader(Path folderPath) {
        this(defaultS3Driver(),folderPath);
    }

    public S3Uploader(S3Driver s3Driver, Path pathInBucket) {
        this.s3Driver = s3Driver;
        this.folderPath = pathInBucket;
    }

    public List<KeyValue> uploadFiles(List<KeyValue> entries) {
        if (!entries.isEmpty()) {
            return attempt(() -> insertAllValues(entries)).orElseThrow();
        }
        return Collections.emptyList();
    }

    @JacocoGenerated
    private static S3Driver defaultS3Driver() {
        Environment environment = new Environment();
        String bucketName = environment.readEnv(S3_BUCKET);
        return S3Driver.fromPermanentCredentialsInEnvironment(bucketName);
    }

    private List<KeyValue> insertAllValues(List<KeyValue> values) throws InterruptedException {
        Collection<List<KeyValue>> entryGroups = splitEntriesIntoGroups(values);
        ParallelMapper<List<KeyValue>, Void> processingResults = processGroups(entryGroups);
        return failedEntries(processingResults);
    }

    private ParallelMapper<List<KeyValue>, Void> processGroups(Collection<List<KeyValue>> groups)
        throws InterruptedException {
        return new ParallelMapper<>(groups, this::insertGroup).map();
    }

    private List<KeyValue> failedEntries(ParallelMapper<List<KeyValue>, Void> mapper) {
        return mapper.getExceptions()
                   .stream()
                   .map(ParallelExecutionException::getInput)
                   .map(failedGroup -> (List<?>) failedGroup)
                   .flatMap(Collection::stream)
                   .map(entry -> (KeyValue) entry)
                   .collect(Collectors.toList());
    }

    private Void insertGroup(List<KeyValue> group) {
        try {
            List<String> values = group.stream().map(KeyValue::getValue).collect(Collectors.toList());
            s3Driver.insertAndCompressFiles(folderPath, values);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private <T> Collection<List<T>> splitEntriesIntoGroups(List<T> values) {
        Collection<List<T>> groups = new ArrayList<>();
        for (int index = 0; index < values.size(); index += DEFAULT_BATCH_SIZE) {
            int endIndex = endOfBatchOrEndOfList(values, index);
            groups.add(values.subList(index, endIndex));
        }
        return groups;
    }

    private <T> int endOfBatchOrEndOfList(List<T> values, int index) {
        return Math.min(values.size(), index + DEFAULT_BATCH_SIZE);
    }
}
