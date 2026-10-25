package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.RecordType;
import group.gnometrading.schemas.StreamRecord;
import group.gnometrading.schemas.book.Bbo1mDecoder;
import group.gnometrading.schemas.book.Bbo1sDecoder;
import group.gnometrading.schemas.book.MboDecoder;
import group.gnometrading.schemas.book.Mbp10Decoder;
import group.gnometrading.schemas.book.Mbp1Decoder;
import group.gnometrading.schemas.book.Ohlcv1hDecoder;
import group.gnometrading.schemas.book.Ohlcv1mDecoder;
import group.gnometrading.schemas.book.Ohlcv1sDecoder;
import group.gnometrading.schemas.book.TradesDecoder;
import java.util.stream.Stream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class StreamRecordEventAdapterTest {

    static Stream<Arguments> templateIdAndRecordType() {
        return Stream.of(
                Arguments.of(MboDecoder.TEMPLATE_ID, RecordType.MBO),
                Arguments.of(Mbp10Decoder.TEMPLATE_ID, RecordType.MBP_10),
                Arguments.of(Mbp1Decoder.TEMPLATE_ID, RecordType.MBP_1),
                Arguments.of(Bbo1sDecoder.TEMPLATE_ID, RecordType.BBO_1S),
                Arguments.of(Bbo1mDecoder.TEMPLATE_ID, RecordType.BBO_1M),
                Arguments.of(TradesDecoder.TEMPLATE_ID, RecordType.TRADES),
                Arguments.of(Ohlcv1sDecoder.TEMPLATE_ID, RecordType.OHLCV_1S),
                Arguments.of(Ohlcv1mDecoder.TEMPLATE_ID, RecordType.OHLCV_1M),
                Arguments.of(Ohlcv1hDecoder.TEMPLATE_ID, RecordType.OHLCV_1H));
    }

    @ParameterizedTest
    @MethodSource("templateIdAndRecordType")
    void dispatchesToCorrectRecordType(int templateId, RecordType expectedRecordType) throws Exception {
        StreamRecord[] receivedSchema = new StreamRecord[1];
        long[] receivedSeq = new long[1];
        boolean[] receivedEndOfBatch = {true};

        StreamRecordEventAdapter adapter = new StreamRecordEventAdapter((schema, sequence, endOfBatch) -> {
            receivedSchema[0] = schema;
            receivedSeq[0] = sequence;
            receivedEndOfBatch[0] = endOfBatch;
        });

        StreamRecord source = expectedRecordType.newInstance();
        adapter.onSequencedEvent(42L, templateId, source.buffer, source.totalMessageSize());

        assertEquals(expectedRecordType, receivedSchema[0].recordType);
        assertEquals(42L, receivedSeq[0]);
        assertFalse(receivedEndOfBatch[0]);
    }

    @Test
    void wrapsSchemaAroundEventBuffer() throws Exception {
        Mbp10Schema source = new Mbp10Schema();
        source.encoder.streamId(42);

        int[] capturedStreamId = new int[1];
        StreamRecordEventAdapter adapter = new StreamRecordEventAdapter((schema, sequence, endOfBatch) -> {
            capturedStreamId[0] = schema.streamId();
        });

        adapter.onSequencedEvent(1L, Mbp10Decoder.TEMPLATE_ID, source.buffer, source.totalMessageSize());

        assertEquals(42, capturedStreamId[0]);
    }

    @Test
    void throwsIllegalArgumentForUnregisteredTemplateId() {
        StreamRecordEventAdapter adapter = new StreamRecordEventAdapter((schema, sequence, endOfBatch) -> {});
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 0, buffer, 8));
        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 10, buffer, 8));
        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 14, buffer, 8));
    }

    @Test
    void throwsIllegalArgumentForOutOfRangeTemplateId() {
        StreamRecordEventAdapter adapter = new StreamRecordEventAdapter((schema, sequence, endOfBatch) -> {});
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, -1, buffer, 8));
        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 10_000, buffer, 8));
    }
}
