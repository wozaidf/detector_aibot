// 解析算子：成功解析的窗口向下游输出，失败记录输出到无效记录侧输出流。
package l2.source;

import l2.model.AlignedWindow;
import l2.model.InvalidRecord;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class ParseProcessFunction extends ProcessFunction<String, AlignedWindow> {
    public static final OutputTag<InvalidRecord> INVALID_RECORDS = new OutputTag<>("invalid-l1-records") {};
    private transient AlignedWindowParser parser;
    @Override public void open(org.apache.flink.api.common.functions.OpenContext context) { parser = new AlignedWindowParser(); }
    @Override public void processElement(String value, Context ctx, Collector<AlignedWindow> out) {
        try { out.collect(parser.parse(value)); }
        catch (Exception e) { ctx.output(INVALID_RECORDS, new InvalidRecord(e.getMessage(), value)); }
    }
}
