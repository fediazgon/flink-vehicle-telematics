import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({EventsTests.class,
        SpeedRadarTests.class,
        AverageSpeedControlTests.class,
        MainTests.class})
public class MySuite {
}