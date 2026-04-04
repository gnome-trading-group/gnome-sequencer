package group.gnometrading.sequencer;

import com.lmax.disruptor.EventHandler;
import group.gnometrading.schemas.Bbo1mDecoder;
import group.gnometrading.schemas.Bbo1mSchema;
import group.gnometrading.schemas.Bbo1sDecoder;
import group.gnometrading.schemas.Bbo1sSchema;
import group.gnometrading.schemas.IntentDecoder;
import group.gnometrading.schemas.MboDecoder;
import group.gnometrading.schemas.MboSchema;
import group.gnometrading.schemas.Mbp10Decoder;
import group.gnometrading.schemas.Mbp10Schema;
import group.gnometrading.schemas.Mbp1Decoder;
import group.gnometrading.schemas.Mbp1Schema;
import group.gnometrading.schemas.Ohlcv1hDecoder;
import group.gnometrading.schemas.Ohlcv1hSchema;
import group.gnometrading.schemas.Ohlcv1mDecoder;
import group.gnometrading.schemas.Ohlcv1mSchema;
import group.gnometrading.schemas.Ohlcv1sDecoder;
import group.gnometrading.schemas.Ohlcv1sSchema;
import group.gnometrading.schemas.Schema;
import group.gnometrading.schemas.TradesDecoder;
import group.gnometrading.schemas.TradesSchema;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * Adapts an {@link EventHandler}{@code <Schema>} for use as a {@link SequencedEventHandler}.
 *
 * <p>On each event, looks up the pre-allocated schema by templateId, wraps it around
 * the SequencedEvent's buffer memory (zero-copy), and delegates to the underlying handler.
 *
 * <p>Zero-copy: the schema's buffer is wrapped around the SequencedEvent's memory using
 * {@link UnsafeBuffer#wrap}, then encoder/decoder are re-wrapped via {@link Schema#wrap}.
 * No bytes are copied. The schema reference is safe to use only within the handler callback.
 *
 * <p>Handles market data template IDs (1-9). Template IDs outside this range will throw.
 */
public final class SchemaEventAdapter implements SequencedEventHandler {

    // IntentDecoder.TEMPLATE_ID is the highest template ID in the current schema set.
    private static final int SCHEMA_ARRAY_SIZE = IntentDecoder.TEMPLATE_ID + 1;

    private final Schema[] schemasByTemplateId;
    private final EventHandler<Schema> delegate;

    /**
     * Constructs a new adapter wrapping the given delegate handler.
     *
     * @param delegate the handler to receive unwrapped Schema events
     */
    public SchemaEventAdapter(EventHandler<Schema> delegate) {
        this.delegate = delegate;
        this.schemasByTemplateId = new Schema[SCHEMA_ARRAY_SIZE];
        this.schemasByTemplateId[MboDecoder.TEMPLATE_ID] = new MboSchema();
        this.schemasByTemplateId[Mbp10Decoder.TEMPLATE_ID] = new Mbp10Schema();
        this.schemasByTemplateId[Mbp1Decoder.TEMPLATE_ID] = new Mbp1Schema();
        this.schemasByTemplateId[Bbo1sDecoder.TEMPLATE_ID] = new Bbo1sSchema();
        this.schemasByTemplateId[Bbo1mDecoder.TEMPLATE_ID] = new Bbo1mSchema();
        this.schemasByTemplateId[TradesDecoder.TEMPLATE_ID] = new TradesSchema();
        this.schemasByTemplateId[Ohlcv1sDecoder.TEMPLATE_ID] = new Ohlcv1sSchema();
        this.schemasByTemplateId[Ohlcv1mDecoder.TEMPLATE_ID] = new Ohlcv1mSchema();
        this.schemasByTemplateId[Ohlcv1hDecoder.TEMPLATE_ID] = new Ohlcv1hSchema();
    }

    @Override
    public void onSequencedEvent(long globalSequence, int templateId, UnsafeBuffer buffer, int length)
            throws Exception {
        Schema schema = schemasByTemplateId[templateId];
        if (schema == null) {
            throw new IllegalArgumentException("No schema registered for templateId: " + templateId);
        }
        // Zero-copy: wrap the schema's buffer around the SequencedEvent buffer memory,
        // then re-wrap encoder/decoder so field reads return the correct values.
        schema.buffer.wrap(buffer, 0, length);
        schema.wrap(schema.buffer);
        delegate.onEvent(schema, globalSequence, false);
    }
}
