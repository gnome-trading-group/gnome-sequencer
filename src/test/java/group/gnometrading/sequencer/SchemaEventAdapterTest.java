package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import group.gnometrading.schemas.Bbo1mDecoder;
import group.gnometrading.schemas.Bbo1sDecoder;
import group.gnometrading.schemas.MboDecoder;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Decoder;
import group.gnometrading.schemas.Ohlcv1hDecoder;
import group.gnometrading.schemas.Ohlcv1mDecoder;
import group.gnometrading.schemas.Ohlcv1sDecoder;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.SchemaType;
import group.gnometrading.schemas.TradesDecoder;
import java.util.stream.Stream;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SchemaEventAdapterTest {

    static Stream<Arguments> templateIdAndSchemaType() {
        return Stream.of(
                Arguments.of(MboDecoder.TEMPLATE_ID, SchemaType.MBO),
                Arguments.of(Mbp10Decoder.TEMPLATE_ID, SchemaType.MBP_10),
                Arguments.of(Mbp1Decoder.TEMPLATE_ID, SchemaType.MBP_1),
                Arguments.of(Bbo1sDecoder.TEMPLATE_ID, SchemaType.BBO_1S),
                Arguments.of(Bbo1mDecoder.TEMPLATE_ID, SchemaType.BBO_1M),
                Arguments.of(TradesDecoder.TEMPLATE_ID, SchemaType.TRADES),
                Arguments.of(Ohlcv1sDecoder.TEMPLATE_ID, SchemaType.OHLCV_1S),
                Arguments.of(Ohlcv1mDecoder.TEMPLATE_ID, SchemaType.OHLCV_1M),
                Arguments.of(Ohlcv1hDecoder.TEMPLATE_ID, SchemaType.OHLCV_1H));
    }

    @ParameterizedTest
    @MethodSource("templateIdAndSchemaType")
    void dispatchesToCorrectSchemaType(int templateId, SchemaType expectedSchemaType) throws Exception {
        Schema[] receivedSchema = new Schema[1];
        long[] receivedSeq = new long[1];
        boolean[] receivedEndOfBatch = {true};

        SchemaEventAdapter adapter = new SchemaEventAdapter((schema, sequence, endOfBatch) -> {
            receivedSchema[0] = schema;
            receivedSeq[0] = sequence;
            receivedEndOfBatch[0] = endOfBatch;
        });

        Schema source = expectedSchemaType.newInstance();
        adapter.onSequencedEvent(42L, templateId, source.buffer, source.totalMessageSize());

        assertEquals(expectedSchemaType, receivedSchema[0].schemaType);
        assertEquals(42L, receivedSeq[0]);
        assertFalse(receivedEndOfBatch[0]);
    }

    @Test
    void wrapsSchemaAroundEventBuffer() throws Exception {
        Mbp10Schema source = new Mbp10Schema();
        source.encoder.exchangeId((short) 42);

        int[] capturedExchangeId = new int[1];
        SchemaEventAdapter adapter = new SchemaEventAdapter((schema, sequence, endOfBatch) -> {
            capturedExchangeId[0] = ((Mbp10Schema) schema).decoder.exchangeId();
        });

        adapter.onSequencedEvent(1L, Mbp10Decoder.TEMPLATE_ID, source.buffer, source.totalMessageSize());

        assertEquals(42, capturedExchangeId[0]);
    }

    @Test
    void throwsIllegalArgumentForUnregisteredTemplateId() {
        SchemaEventAdapter adapter = new SchemaEventAdapter((schema, sequence, endOfBatch) -> {});
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 0, buffer, 8));
        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 10, buffer, 8));
        assertThrows(IllegalArgumentException.class, () -> adapter.onSequencedEvent(1L, 14, buffer, 8));
    }

    @Test
    void throwsArrayIndexOutOfBoundsForOutOfRangeTemplateId() {
        SchemaEventAdapter adapter = new SchemaEventAdapter((schema, sequence, endOfBatch) -> {});
        UnsafeBuffer buffer = new UnsafeBuffer(new byte[8]);

        assertThrows(ArrayIndexOutOfBoundsException.class, () -> adapter.onSequencedEvent(1L, -1, buffer, 8));
        assertThrows(ArrayIndexOutOfBoundsException.class, () -> adapter.onSequencedEvent(1L, 15, buffer, 8));
    }
}
