package Utils;

import com.google.gson.JsonObject;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class windowFuncDiff implements WindowFunction<Tuple4<Float, Float, Float, String>, String, Tuple, TimeWindow> {
    String idx;
    String typ;
    public windowFuncDiff(String index, String type){
        idx = index;
        typ = type;
    }
    public void apply(Tuple key, TimeWindow window, Iterable<Tuple4<Float, Float, Float, String>> input,
            Collector<String> out) {
        float total = 0;
        float count = 0;
        float max = Float.MIN_VALUE;
        float min = Float.MAX_VALUE;
        boolean nothing = true;
        for (Tuple4<Float, Float, Float, String> in : input) {
            nothing = false;
            float tmp = in.getField(Integer.valueOf(idx));
            total = total + tmp;
            count ++;
            if (tmp > max) {
                max = tmp;
            }
            if (tmp < min) {
                min = tmp;
            }
        }
        if (nothing) {
            max = min;
        }
        
        out.collect("{'start_time': " + window.getStart() + ", 'device_id': '" + key.getField(0) +"', 'end_time': " + window.getEnd() + ", 'diff': '" + (max-min) + "', 'type': '"+ typ + "', 'avg': '"+ (total/count) + "' }");
        nothing = true;
    }
  }