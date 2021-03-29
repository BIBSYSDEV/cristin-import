package no.unit.cristin;

import static no.unit.cristin.S3Uploader.DEFAULT_BATCH_SIZE;
import static nva.commons.core.attempt.Try.attempt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.javafaker.Faker;
import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import no.unit.nva.identifiers.SortableIdentifier;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.JsonUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class S3UploaderTest {

    public static final Faker FAKER = Faker.instance();
    public static final String JSON_EXTENSION = ".json";
    private static final int MAX_ITERATIONS = 1;
    private S3Driver s3Driver;
    private S3Uploader uploader;

    @BeforeEach
    public void init() {
        s3Driver = mock(S3Driver.class);
        uploader = new S3Uploader(s3Driver);
    }

    @Test
    @Tag("RemoteTest")
    public void s3DriverWritesEntriesInS3Bucket() {
        List<KeyValue> entries = createSampleEntries(10);
        S3Uploader s3Uploader = new S3Uploader();

        List<KeyValue> failedItems = s3Uploader.uploadFiles(entries);
        for (int i = 0; i < MAX_ITERATIONS; i++) {
            failedItems = s3Uploader.uploadFiles(failedItems);
            System.out.println(failedItems.size());
        }

        assertThat(failedItems, is(empty()));
    }

    @Test
    public void insertEntriesInsertsMultipleEntriesInS3() throws IOException {
        List<KeyValue> entries = sampleEntries(this::randomContent);

        uploader.uploadFiles(entries);
        int numberOfExpectedCalls = entries.size() / DEFAULT_BATCH_SIZE;
        verify(s3Driver, times(numberOfExpectedCalls)).insertAndCompressFiles(any(List.class));
    }

    @Test
    public void insertEntriesReturnsListWithIdsOfFailedInsertions() throws IOException {
        List<KeyValue> entries = sampleEntries(this::randomContent);
        s3DriverThrowsExceptions();
        uploader = new S3Uploader(s3Driver);

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


    private void s3DriverThrowsExceptions() throws IOException {
        s3Driver = mock(S3Driver.class);
        doThrow(IOException.class).when(s3Driver).insertAndCompressFiles(any(List.class));
    }

    private List<KeyValue> createSampleEntries(int size) {
        return IntStream.range(0, size).boxed()
                   .map(i -> new KeyValue(randomFilename(), randomContent()))
                   .collect(Collectors.toList());
    }

    private String randomFilename() {
        return SortableIdentifier.next() + JSON_EXTENSION;
    }


    private List<KeyValue> sampleEntries(Supplier<String> contentSupplier) {
        return IntStream.range(0, DEFAULT_BATCH_SIZE * 2).boxed()
                   .map(i -> new KeyValue(UUID.randomUUID().toString(), contentSupplier.get()))
                   .collect(Collectors.toList());
    }

    private String randomContent() {
        ObjectNode root = JsonUtils.objectMapper.createObjectNode();
        String fieldValue = FAKER.lorem().paragraph(100);
        root.put("someField", fieldValue);
        return attempt(() -> JsonUtils.objectMapper.writeValueAsString(root)).orElseThrow();
    }
}