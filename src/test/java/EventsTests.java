import fediazgon.events.AccidentEvent;
import fediazgon.events.AvgSpeedEvent;
import fediazgon.events.PositionEvent;
import org.apache.flink.types.NullFieldException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventsTests {

    @Test(expected = NullFieldException.class)
    public void shouldCreateEmptyPositionEvent() {
        PositionEvent empty = new PositionEvent();
        empty.getFieldNotNull(0);
    }

    @Test(expected = NullFieldException.class)
    public void shouldCreateEmptyAverageSpeedEvent() {
        AvgSpeedEvent empty = new AvgSpeedEvent();
        empty.getFieldNotNull(0);
    }

    @Test(expected = NullFieldException.class)
    public void shouldCreateEmptyAccidentEvent() {
        AccidentEvent empty = new AccidentEvent();
        empty.getFieldNotNull(0);
    }

}
