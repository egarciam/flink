package meetupStreaming;

import meetupStreaming.jsonparser.*;
import meetupStreaming.meetupWebSocketSource.MeetupStreamingSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import meetupStreaming.operations.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;


/**
 *  Entry point.
 */
public class Main {

    public static void main(String[] args) throws Exception {


        // set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //add the event source
        String url = "wss://stream.meetup.com/2/rsvps";
        DataStream<MeetupRSVGevent> events = env.addSource(new MeetupStreamingSource(url));

        /********
         *
         * implement the business logic
         *
          */
        events.filter(new FilterNulls())
                .keyBy("event.event_name")
                .timeWindow(Time.seconds(2))
                .apply(new ContarVentanaGroup());


        /* add the sink */
        events.print();


        env.execute("Meetup Flink DataStream");
    }
}
