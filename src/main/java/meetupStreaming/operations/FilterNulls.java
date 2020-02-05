package meetupStreaming.operations;

import meetupStreaming.jsonparser.MeetupRSVGevent;
import org.apache.flink.api.common.functions.FilterFunction;

public class FilterNulls implements FilterFunction<MeetupRSVGevent> {

    public boolean filter(MeetupRSVGevent event) throws Exception{
        return event.getGroup() != null &&
                event.getResponse() != null;
    }
}


