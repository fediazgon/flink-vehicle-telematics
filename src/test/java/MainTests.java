import es.upm.cc.Telematics;
import org.apache.flink.runtime.client.JobExecutionException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

import static es.upm.cc.Telematics.AVG_SPEED_FILE;
import static es.upm.cc.Telematics.SPEED_RADAR_FILE;
import static org.junit.Assert.assertTrue;

public class MainTests {

    private static final String INPUT_FILE = "test_input.csv";
    private static final String OUTPUT_FOLDER = "output";

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Test
    public void shouldPrintHelpMessageAndExit() {
        exit.expectSystemExit();
        Telematics.main(new String[0]);
    }

    @Test
    public void inputFileDoesNotExists() {
        exit.expectSystemExit();
        Telematics.main(new String[]{"whatever", "whatever"});
    }

    @Test
    public void shouldRunMainAndCreateFiles() throws IOException {
        ClassLoader classLoader = getClass().getClassLoader();
        String input = new File(classLoader.getResource(INPUT_FILE).getFile()).getPath();
        String output = temporaryFolder.newFolder(OUTPUT_FOLDER).getPath();
        String[] args = new String[]{input, output};

        Telematics.main(args);

        assertTrue(new File(String.format("%s/%s", output, SPEED_RADAR_FILE)).exists());
        assertTrue(new File(String.format("%s/%s", output, AVG_SPEED_FILE)).exists());
    }

}
