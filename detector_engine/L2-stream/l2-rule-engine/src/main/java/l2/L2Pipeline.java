package l2;

import l2.cep.CepDetector;
import l2.config.AppConfig;
import l2.model.AlignedWindow;
import l2.model.AnomalyEvent;
import l2.model.DetectionCandidate;
import l2.model.FusionInput;
import l2.model.WindowEvaluation;
import l2.rules.FinalDecisionFunction;
import l2.rules.L1WindowReplayDeduplicator;
import l2.rules.WindowEvaluationFunction;
import l2.sink.KafkaSinkFactory;
import l2.sink.PostgresFinalEventLinkSink;
import l2.sink.PostgresWindowAuditSink;
import l2.source.KafkaSourceFactory;
import l2.source.ParseProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/** Builds the L2 dataflow by connecting its source, detection, fusion, and sink stages. */
public final class L2Pipeline {
    private L2Pipeline() {}

    public static void build(StreamExecutionEnvironment env, AppConfig config) {
        env.enableCheckpointing(10_000, CheckpointingMode.EXACTLY_ONCE);

        DataStream<String> json = env.fromSource(KafkaSourceFactory.create(config),
                WatermarkStrategy.noWatermarks(), "L1 Kafka Source");
        SingleOutputStreamOperator<AlignedWindow> parsed = json
                .process(new ParseProcessFunction()).name("Parse and validate L1 JSON");
        parsed.getSideOutput(ParseProcessFunction.INVALID_RECORDS).printToErr("INVALID-L1");

        DataStream<AlignedWindow> timed = parsed.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AlignedWindow>forBoundedOutOfOrderness(
                                Duration.ofSeconds(config.watermarkSeconds()))
                        .withTimestampAssigner((window, ignored) -> window.getWindowEndEpochMillis())
                        .withIdleness(Duration.ofSeconds(config.idleSeconds())))
                .name("L1 Event Time and Watermarks");
        DataStream<AlignedWindow> uniqueWindows = timed.keyBy(AlignedWindow::getEntityId)
                .process(new L1WindowReplayDeduplicator(config.dedupSeconds()))
                .name("L1 window replay deduplication");

        SingleOutputStreamOperator<WindowEvaluation> evaluated = uniqueWindows
                .process(new WindowEvaluationFunction(config))
                .name("Drools window marks and severe or joint candidates");
        evaluated.addSink(new PostgresWindowAuditSink()).name("PostgreSQL L2 window audit");
        DataStream<DetectionCandidate> droolsCandidates = evaluated
                .getSideOutput(WindowEvaluationFunction.CANDIDATES);
        KeyedStream<WindowEvaluation, String> windowsByEntity = evaluated
                .keyBy(item -> item.getWindow().getEntityId());

        DataStream<FusionInput> droolsInputs = droolsCandidates
                .map(FusionInput::candidate).name("Drools candidate fusion input");
        DataStream<FusionInput> cepInputs = CepDetector.detect(windowsByEntity, config);
        DataStream<AnomalyEvent> finalEvents = droolsInputs.union(cepInputs)
                .keyBy(L2Pipeline::entityId)
                .process(new FinalDecisionFunction(config.dedupSeconds(),
                        config.associationWindowSeconds(), config.watermarkSeconds(),
                        config.cepMaxSequenceSeconds()))
                .name("Drools-CEP association, deduplication and final decision");

        if (config.printResults()) finalEvents.print("L2-ANOMALY");
        finalEvents.sinkTo(KafkaSinkFactory.create(config)).name("L2 Kafka Sink");
        finalEvents.addSink(new PostgresFinalEventLinkSink()).name("PostgreSQL L2 final event window links");
    }

    private static String entityId(FusionInput input) {
        DetectionCandidate candidate = input.getCandidate();
        if (candidate != null) return candidate.getEntityId();
        return input.getClosedSequence().getEntityId();
    }
}
