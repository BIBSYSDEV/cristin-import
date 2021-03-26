package no.unit.cristin;

import static nva.commons.core.attempt.Try.attempt;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.Environment;
import nva.commons.core.JacocoGenerated;
import nva.commons.core.ioutils.IoUtils;
import nva.commons.core.parallel.ParallelExecutionException;
import nva.commons.core.parallel.ParallelMapper;

public class S3Uploader {

    public static final String BATCH_SIZE_PROPERTY = "batchSize";
    public static final String S3_BUCKET = "BUCKET";
    public static final String DEFAULT_PROPERTIES = "default.properties";
    public static final String OVERRIDING_PROPERTIES_FILE_PATH = "PROPERTIES_PATH";

    private final S3Driver s3Driver;
    private final Environment environment;
    private final int batchSize;
    private Properties applicationProperties;

    @JacocoGenerated
    public S3Uploader() {
        this(defaultS3Driver(), new Environment());
    }

    public S3Uploader(S3Driver s3Driver, Environment environment) {
        this.environment = environment;
        this.s3Driver = s3Driver;
        attempt(this::initProperties).orElseThrow();
        batchSize = Integer.parseInt(applicationProperties.getProperty(BATCH_SIZE_PROPERTY));
    }

    public void uploadFile(String entryId, String content) {
        s3Driver.insertFile(Path.of(entryId), content);
    }

    public List<KeyValue> uploadFiles(List<KeyValue> entries) throws InterruptedException {
        ParallelMapper<KeyValue, String> mapper =
            new ParallelMapper<>(entries, this::uploadEntry, batchSize).run();
        return collectFailedInsertionObjects(mapper);
    }

    @JacocoGenerated
    private static S3Driver defaultS3Driver() {
        Environment environment = new Environment();
        String bucketName = environment.readEnv(S3_BUCKET);
        return S3Driver.fromPermanentCredentialsInEnvironment(bucketName);
    }

    private Void initProperties() throws IOException {
        Properties defaultProperties = new Properties();
        defaultProperties.load(IoUtils.inputStreamFromResources(DEFAULT_PROPERTIES));

        Properties overridingProperties = readOverridingPropertiesFile();

        applicationProperties = new Properties(defaultProperties);
        applicationProperties.putAll(overridingProperties);
        applicationProperties.putAll(System.getProperties());
        return null;
    }

    private Properties readOverridingPropertiesFile() throws IOException {
        InputStream overridingPropertiesContent = environment.readEnvOpt(OVERRIDING_PROPERTIES_FILE_PATH)
                                                      .map(Path::of)
                                                      .map(IoUtils::stringFromFile)
                                                      .map(IoUtils::stringToStream)
                                                      .orElse(InputStream.nullInputStream());

        Properties overridingProperties = new Properties();
        overridingProperties.load(overridingPropertiesContent);
        return overridingProperties;
    }

    private List<KeyValue> collectFailedInsertionObjects(ParallelMapper<KeyValue, String> mapper) {
        return mapper.getExceptions().stream()
                   .map(ParallelExecutionException::getInput)
                   .map(inputObject -> (KeyValue) inputObject)
                   .collect(Collectors.toList());
    }

    private String uploadEntry(KeyValue keyValue) {
        s3Driver.insertFile(Path.of(keyValue.getKey()), keyValue.getValue());
        return keyValue.getKey();
    }
}
