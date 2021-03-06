package meetupStreaming.operations;

import meetupStreaming.jsonparser.MeetupRSVGevent;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Vector;

public class ContarVentanaGroup implements WindowFunction<MeetupRSVGevent, Tuple2<String, Integer>, Tuple, TimeWindow> {

    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<MeetupRSVGevent> iterable,
                      Collector<Tuple2<String, Integer>> collector){
        int cont = 0;
        String name = "";
        MeetupRSVGevent event = null;

        Iterator<MeetupRSVGevent> itr = iterable.iterator();

        while(itr.hasNext()){
            event = itr.next();
            cont++;

        }

        collector.collect(new Tuple2<String, Integer>(event.getGroup().getGroup_name(),cont));

    }


}
