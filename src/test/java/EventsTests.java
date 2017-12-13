import es.upm.cc.events.AverageSpeedEvent;
import es.upm.cc.events.PositionEvent;
import org.apache.flink.types.NullFieldException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class EventsTests {

    @Test(expected = NullFieldException.class)
    public void shouldCreateEmptyPositionEvent() {
        PositionEvent empty = new PositionEvent();
        empty.getFieldNotNull(0);
    }

    @Test
    public void shouldCreatePositionEvent() {
        PositionEvent pe = new PositionEvent(30, "1", 91, 0,
                3, 1, 52, 100000);

        assertEquals(30, pe.getTimestamp());
        assertEquals("1", pe.getVid());
        assertEquals(91, pe.getSpeed());
        assertEquals(0, pe.getXway());
        assertEquals(1, pe.getDir());
        assertEquals(52, pe.getSegment());
    }

    @Test(expected = NullFieldException.class)
    public void shouldCreateEmptyAverageSpeedEvent() {
        AverageSpeedEvent empty = new AverageSpeedEvent();
        empty.getFieldNotNull(0);
    }

    @Test
    public void shouldCreateAverageSpeedEvent() {
        AverageSpeedEvent ae = new AverageSpeedEvent(720, 870,
                "1", 0, 0, 45);

        assertEquals(720, ae.getT1());
        assertEquals(870, ae.getT2());
    }

}
