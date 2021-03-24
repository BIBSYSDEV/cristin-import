package no.unit.cristin;

import static no.unit.cristin.S3Uploader.BATCH_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import com.github.javafaker.Faker;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import no.unit.nva.s3.S3Driver;
import nva.commons.core.ioutils.IoUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class S3UploaderTest {

    public static final Faker FAKER = Faker.instance();
    public static final String CONTENT_THROWING_EXCEPTION = "throwException";
    public static final String ANY_CONTENT = ".*";
    private S3Driver s3Driver;

    @BeforeEach
    public void init() {
        s3Driver = mock(S3Driver.class);
        doThrow(RuntimeException.class).when(s3Driver)
            .insertFile(any(Path.class), matches(ANY_CONTENT + CONTENT_THROWING_EXCEPTION + ANY_CONTENT));
    }

    @Test
    public void insertEntryInsertsSingleEntryInS3() {
        String contentForUpload = IoUtils.stringFromResources(Path.of("load.json"));
        String entryId = UUID.randomUUID().toString();
        S3Uploader uploader = new S3Uploader(s3Driver);
        uploader.uploadFile(entryId, contentForUpload);
        verify(s3Driver, times(1)).insertFile(any(Path.class),any(String.class));
    }

    @Test
    public void insertEntriesInsertsMultipleEntriesInS3() throws InterruptedException {
        List<KeyValue> entries = sampleEntries(this::randomContent);
        S3Uploader uploader = new S3Uploader(s3Driver);
        uploader.uploadFiles(entries);
        verify(s3Driver, times(entries.size())).insertFile(any(Path.class),any(String.class));
    }

    @Test
    public void insertEntriesReturnsListWithIdsOfFailedInsertions() throws InterruptedException {
        List<KeyValue> entries = sampleEntries(this::failingContent);
        S3Uploader uploader = new S3Uploader(s3Driver);
        List<String> failedIds = uploader.uploadFiles(entries);

        String[] expectedFailedIds = entries.stream()
                                         .map(KeyValue::getKey)
                                         .collect(Collectors.toList())
                                         .toArray(String[]::new);
        assertThat(failedIds, containsInAnyOrder(expectedFailedIds));
    }

    private String failingContent() {
        return randomContent() + CONTENT_THROWING_EXCEPTION;
    }

    private List<KeyValue> sampleEntries(Supplier<String> contentSupplier) {
        return IntStream.range(0, BATCH_SIZE * 2).boxed()
                   .map(i -> new KeyValue(UUID.randomUUID().toString(), contentSupplier.get()))
                   .collect(Collectors.toList());
    }

    private String randomContent() {
        return FAKER.lorem().sentence(10);
    }
}