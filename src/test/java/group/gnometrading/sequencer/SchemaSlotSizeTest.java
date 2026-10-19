package group.gnometrading.sequencer;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import group.gnometrading.schemas.MessageHeaderEncoder;
import group.gnometrading.schemas.MetricColumnDecoder;
import group.gnometrading.schemas.MetricRowDecoder;
import group.gnometrading.schemas.MetricStreamDecoder;
import group.gnometrading.sequencer.metrics.MetricLimits;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Token;
import uk.co.real_logic.sbe.xml.IrGenerator;
import uk.co.real_logic.sbe.xml.ParserOptions;
import uk.co.real_logic.sbe.xml.XmlSchemaParser;

/**
 * Every ring slot and journal record is sized by {@link SequencedEvent#MAX_MESSAGE_SIZE}. Read from the schema the
 * codecs are generated from, so a message added or grown later is checked without anyone listing it here.
 */
class SchemaSlotSizeTest {

    private static final Set<Integer> METRIC_TEMPLATES =
            Set.of(MetricRowDecoder.TEMPLATE_ID, MetricStreamDecoder.TEMPLATE_ID, MetricColumnDecoder.TEMPLATE_ID);

    // Enough for any stream declared today (13 columns at most) with room to grow.
    private static final int MIN_METRIC_COLUMNS = 32;
    private static final int MIN_NAME_LENGTH = 64;

    private static final Map<String, Integer> MESSAGE_SIZES = new TreeMap<>();
    private static final Map<String, Integer> METRIC_SIZES = new TreeMap<>();

    @BeforeAll
    static void readSchema() throws Exception {
        try (InputStream xml = SchemaSlotSizeTest.class.getClassLoader().getResourceAsStream("schema.xml")) {
            Ir ir = new IrGenerator()
                    .generate(XmlSchemaParser.parse(
                            xml,
                            ParserOptions.builder()
                                    .suppressOutput(true)
                                    .stopOnError(true)
                                    .build()));
            for (List<Token> message : ir.messages()) {
                Token begin = message.get(0);
                int size = MessageHeaderEncoder.ENCODED_LENGTH + begin.encodedLength();
                MESSAGE_SIZES.put(begin.name(), size);
                if (METRIC_TEMPLATES.contains((int) begin.id())) {
                    METRIC_SIZES.put(begin.name(), size);
                }
            }
        }
        assertTrue(METRIC_SIZES.size() == METRIC_TEMPLATES.size(), "Every metric message is in the schema");
    }

    @Test
    void everyMessageFitsARingSlot() {
        MESSAGE_SIZES.forEach((name, size) -> assertTrue(
                size <= SequencedEvent.MAX_MESSAGE_SIZE,
                name + " takes " + size + " bytes but a ring slot holds " + SequencedEvent.MAX_MESSAGE_SIZE
                        + "; grow SequencedEvent.MAX_MESSAGE_SIZE"));
    }

    @Test
    void noMetricMessageSetsTheSlotSize() {
        int largestOther = MESSAGE_SIZES.entrySet().stream()
                .filter(entry -> !METRIC_SIZES.containsKey(entry.getKey()))
                .mapToInt(Map.Entry::getValue)
                .max()
                .orElseThrow();
        METRIC_SIZES.forEach((name, size) -> assertTrue(
                size < largestOther,
                name + "'s fixed part (" + size + " bytes) must stay smaller than the largest other message ("
                        + largestOther + "); metrics fit the slot, they don't size it"));
        assertFalse(
                MESSAGE_SIZES.entrySet().stream()
                        .filter(entry -> entry.getValue() == SequencedEvent.MAX_MESSAGE_SIZE)
                        .allMatch(entry -> METRIC_SIZES.containsKey(entry.getKey())),
                "The slot is sized by a message that isn't a metric");
    }

    @Test
    void metricRowsAndNamesKeepEnoughRoom() {
        assertTrue(
                MetricLimits.MAX_ROW_BYTES >= MIN_METRIC_COLUMNS * Long.BYTES,
                "A metric row now holds only " + MetricLimits.MAX_ROW_BYTES + " bytes; streams with "
                        + MIN_METRIC_COLUMNS + " long or double columns would be refused");
        assertTrue(
                MetricLimits.MAX_NAME_BYTES >= MIN_NAME_LENGTH,
                "Metric names are now capped at " + MetricLimits.MAX_NAME_BYTES + " characters");
    }
}
