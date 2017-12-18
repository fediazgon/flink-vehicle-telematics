import es.upm.cc.AccidentReporter;
import es.upm.cc.AverageSpeedControl;
import es.upm.cc.SpeedRadar;
import es.upm.cc.Telematics;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static es.upm.cc.Telematics.AVG_SPEED_FILE;
import static es.upm.cc.Telematics.SPEED_RADAR_FILE;
import static org.junit.Assert.assertTrue;

public class MainTests extends StreamingMultipleProgramsTestBase {

    private static final String INPUT_FILE = "test_input.csv";
    private static final String OUTPUT_FOLDER = "output";

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();


    @Test(expected = Exception.class)
    public void shouldPrintHelpMessageAndExit() throws Exception {
        Telematics.main(new String[0]);
    }

    @Test(expected = JobExecutionException.class)
    public void inputFileDoesNotExists() throws Exception {
        String output = temporaryFolder.newFolder(OUTPUT_FOLDER).getPath();
        Telematics.main(new String[]{"whatever", output});
    }

    @Test
    public void shouldRunMainAndCreateFiles() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        String input = new File(classLoader.getResource(INPUT_FILE).getFile()).getPath();
        String output = temporaryFolder.newFolder(OUTPUT_FOLDER).getPath();
        String[] args = new String[]{input, output};

        Telematics.main(args);

        assertTrue(new File(String.format("%s/%s", output, SPEED_RADAR_FILE)).exists());
        assertTrue(new File(String.format("%s/%s", output, AVG_SPEED_FILE)).exists());
    }

    //TODO: delete and make private constructors when jacoco 0.8 is released
    @Test
    public void hackConstructorCoverage() {
        new Telematics();
        new SpeedRadar();
        new AverageSpeedControl();
        new AccidentReporter();
    }

}
