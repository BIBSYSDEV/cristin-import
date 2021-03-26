package no.unit.cristin;

import static no.unit.cristin.S3Uploader.BATCH_SIZE_PROPERTY;
import static no.unit.cristin.S3Uploader.DEFAULT_PROPERTIES;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.javafaker.Faker;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.Environment;
import nva.commons.core.JsonUtils;
import nva.commons.core.ioutils.IoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class S3UploaderTest {

    public static final Faker FAKER = Faker.instance();
    public static final String CONTENT_THROWING_EXCEPTION = "throwException";
    public static final String ANY_CONTENT = ".*";
    public static final String OVERRIDING_PROPERTIES_FILE = "temp.properties";
    public static final String JSON_EXTENSION = ".json";
    private static final Integer DEFAULT_BATCH_SIZE = loadDefaultBatchSize();
    public static final Integer FILE_SPECIFIED_BATCH_SIZE = DEFAULT_BATCH_SIZE * 2;
    private static final Integer SYSTEM_DEFINED_BATCH_SIZE = FILE_SPECIFIED_BATCH_SIZE * 2;
    private static final int MAX_ITERATIONS = 1;
    private S3Driver s3Driver;
    private S3Uploader uploader;

    @BeforeEach
    public void init() {
        s3Driver = mock(S3Driver.class);
        doThrow(RuntimeException.class).when(s3Driver)
            .insertFile(any(Path.class), matches(ANY_CONTENT + CONTENT_THROWING_EXCEPTION + ANY_CONTENT));
        uploader = new S3Uploader(s3Driver, new Environment());
        System.clearProperty(BATCH_SIZE_PROPERTY);
    }

    @Test
    @Tag("RemoteTest")
    public void s3DriverWritesEntriesInS3Bucket() throws InterruptedException {
        System.setProperty("batchSize", "10000");
        List<KeyValue> entries = createSampleEntries(10);
        S3Uploader s3Uploader = new S3Uploader();
        List<KeyValue> failedItems = s3Uploader.uploadFiles(entries);
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            failedItems = s3Uploader.uploadFiles(failedItems);
        }

        assertThat(failedItems, is(empty()));
    }

    @Test
    public void insertEntryInsertsSingleEntryInS3() {
        String contentForUpload = IoUtils.stringFromResources(Path.of("load.json"));
        String entryId = UUID.randomUUID().toString();
        uploader.uploadFile(entryId, contentForUpload);
        verify(s3Driver, times(1)).insertFile(any(Path.class), any(String.class));
    }

    @Test
    public void insertEntriesInsertsMultipleEntriesInS3() throws InterruptedException {
        List<KeyValue> entries = sampleEntries(this::randomContent);

        uploader.uploadFiles(entries);
        verify(s3Driver, times(entries.size())).insertFile(any(Path.class), any(String.class));
    }

    @Test
    public void insertEntriesReturnsListWithIdsOfFailedInsertions() throws InterruptedException {
        List<KeyValue> entries = sampleEntries(this::failingContent);

        List<String> failedIds = uploader.uploadFiles(entries)
                                     .stream()
                                     .map(KeyValue::getKey)
                                     .collect(Collectors.toList());

        String[] expectedFailedIds = entries.stream()
                                         .map(KeyValue::getKey)
                                         .collect(Collectors.toList())
                                         .toArray(String[]::new);
        assertThat(failedIds, containsInAnyOrder(expectedFailedIds));
    }

    @Test
    public void batchSizeIsSetToDefaultBatchSizeWhenNoOtherSettingExists()
        throws NoSuchFieldException, IllegalAccessException {
        uploader = new S3Uploader(s3Driver, new Environment());
        Integer batchSize = reachBatchSizeFieldValue();
        assertThat(batchSize, is(equalTo(DEFAULT_BATCH_SIZE)));
    }

    @Test
    public void batchSizeIsSetToValueInOverridingFile() throws NoSuchFieldException, IllegalAccessException,
                                                               IOException {
        Environment mockEnvironment = environmentSpecifyingAlternativeConfigFile();
        uploader = new S3Uploader(s3Driver, mockEnvironment);
        Integer batchSize = reachBatchSizeFieldValue();
        assertThat(batchSize, is(equalTo(FILE_SPECIFIED_BATCH_SIZE)));
    }

    @Test
    public void batchSizeIsSetToValueDefinedInSystemPropertiesOverridingPropertiesInSuppliedFile()
        throws NoSuchFieldException,
               IllegalAccessException,
               IOException {
        System.setProperty(BATCH_SIZE_PROPERTY, SYSTEM_DEFINED_BATCH_SIZE.toString());
        Environment mockEnvironment = environmentSpecifyingAlternativeConfigFile();
        uploader = new S3Uploader(s3Driver, mockEnvironment);
        Integer batchSize = reachBatchSizeFieldValue();
        assertThat(batchSize, is(equalTo(SYSTEM_DEFINED_BATCH_SIZE)));
    }

    private static int loadDefaultBatchSize() {
        try {
            Properties properties = new Properties();
            properties.load(IoUtils.inputStreamFromResources(DEFAULT_PROPERTIES));
            return Optional.of(properties.get(BATCH_SIZE_PROPERTY))
                       .map(String::valueOf)
                       .map(Integer::parseInt)
                       .orElseThrow();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<KeyValue> createSampleEntries(int size) {
        return IntStream.range(0, size).boxed()
                   .map(i -> new KeyValue(randomFilename(), randomContent()))
                   .collect(Collectors.toList());
    }

    private String randomFilename() {
        return SortableIdentifier.next() + JSON_EXTENSION;
    }

    private Integer reachBatchSizeFieldValue() throws NoSuchFieldException, IllegalAccessException {
        Field field = uploader.getClass().getDeclaredField("batchSize");
        field.setAccessible(true);
        return (Integer) field.get(uploader);
    }

    private Environment environmentSpecifyingAlternativeConfigFile() throws IOException {
        Environment mockEnvironment = mock(Environment.class);
        when(mockEnvironment.readEnvOpt(S3Uploader.OVERRIDING_PROPERTIES_FILE_PATH))
            .thenReturn(Optional.of(propertiesFilePath()));
        return mockEnvironment;
    }

    private String propertiesFilePath() throws IOException {
        File file = createOverridingPropertiesFilePath();
        writePropertiesFile(file);
        return file.toPath().toString();
    }

    private void writePropertiesFile(File file) throws IOException {
        String newBatchSize = "batchSize = " + FILE_SPECIFIED_BATCH_SIZE;
        try (FileWriter writer = new FileWriter(file)) {
            writer.append(newBatchSize);
            writer.flush();
        }
    }

    private File createOverridingPropertiesFilePath() {
        File file = new File(OVERRIDING_PROPERTIES_FILE);
        if (file.exists()) {
            file.delete();
        }
        file.deleteOnExit();
        return file;
    }

    private String failingContent() {
        return randomContent() + CONTENT_THROWING_EXCEPTION;
    }

    private List<KeyValue> sampleEntries(Supplier<String> contentSupplier) {
        return IntStream.range(0, DEFAULT_BATCH_SIZE * 2).boxed()
                   .map(i -> new KeyValue(UUID.randomUUID().toString(), contentSupplier.get()))
                   .collect(Collectors.toList());
    }

    private String randomContent() {
        ObjectNode root = JsonUtils.objectMapper.createObjectNode();
        String fieldValue = FAKER.lorem().sentence(10);
        root.put("someField", fieldValue);
        return attempt(() -> JsonUtils.objectMapper.writeValueAsString(root)).orElseThrow();
    }
}