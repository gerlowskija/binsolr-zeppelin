package org.apache.solr.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.util.SolrCLI;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

public class ZeppelinTool extends SolrCLI.ToolBase {
    private static final String ZEPP_VERSION = "0.9.0";
    private static final String ZEPP_INSTALL_TYPE = "netinst";
    private static final String ZEPP_UNPACK_DIR_NAME = "zeppelin-" + ZEPP_VERSION + "-bin-" + ZEPP_INSTALL_TYPE;
    private static final String MIRROR_BASE = "http://apache.cs.utah.edu/zeppelin/zeppelin-" + ZEPP_VERSION + "/";
    private static final String ZEPP_ARCHIVE = ZEPP_UNPACK_DIR_NAME + ".tgz";
    private static final String ZEPP_ARCHIVE_URL = MIRROR_BASE + ZEPP_ARCHIVE;
    private static final int PROCESS_WAIT_TIME_SECONDS = 90;

    private static final String SOLR_INSTALL_DIR = System.getProperty("solr.install.dir");
    private static final String ZEPP_INTERPRETER_TEMPLATE_ABS_PATH = Paths.get(SOLR_INSTALL_DIR, "server", "resources", "zeppelin-solr-interpreter.json.template").toAbsolutePath().toString();
    private static final String ZEPP_INTERPRETER_ABS_PATH = Paths.get(SOLR_INSTALL_DIR, "server", "resources", "zeppelin-solr-interpreter.json").toAbsolutePath().toString();
    private static final String ZEPP_BASE_ABS_PATH = Paths.get(SOLR_INSTALL_DIR, "zeppelin").toAbsolutePath().toString();
    private static final String ZEPP_UNPACK_LOG_ABS_PATH = Paths.get(ZEPP_BASE_ABS_PATH, "logs.txt").toAbsolutePath().toString();
    private static final String ZEPP_ARCHIVE_ABS_PATH = Paths.get(ZEPP_BASE_ABS_PATH, ZEPP_ARCHIVE).toAbsolutePath().toString();
    private static final String ZEPP_UNPACKED_ABS_PATH = Paths.get(ZEPP_BASE_ABS_PATH, ZEPP_UNPACK_DIR_NAME).toAbsolutePath().toString();
    private static final String ZEPP_DAEMON_EXE_NAME = (SystemUtils.IS_OS_WINDOWS) ? "zeppelin-daemon.cmd" : "zeppelin-daemon.sh";
    private static final String ZEPP_INTERP_EXE_NAME = (SystemUtils.IS_OS_WINDOWS) ? "install-interpreter.cmd" : "install-interpreter.sh";
    private static final String ZEPP_DAEMON_ABS_PATH = Paths.get(ZEPP_UNPACKED_ABS_PATH, "bin", ZEPP_DAEMON_EXE_NAME).toAbsolutePath().toString();
    private static final String ZEPP_INTERP_ABS_PATH = Paths.get(ZEPP_UNPACKED_ABS_PATH, "bin", ZEPP_INTERP_EXE_NAME).toAbsolutePath().toString();

    @Override
    public String getName() { return "zeppelin"; }

    @Override
    public Option[] getOptions() {
        return new Option[] {
                Option.builder("a")
                        .desc("The action to perform on the Zeppelin install.  Options are: bootstrap, clean, start, stop, update-interpreter")
                        .longOpt("action")
                        .hasArg(true)
                        .argName("action")
                        .required()
                        .build(),
                Option.builder("z")
                        .desc("Only valid for action=update-interpreter.  The Zeppelin URL to update.  Defaults to http://localhost:8080")
                        .longOpt("zeppelinUrl")
                        .hasArg()
                        .argName("url")
                        .build(),
                Option.builder("s")
                        .desc("The URL of a valid Solr instance.  Used to configure the Zeppelin interpreter.  Defaults to values read from solr.in.sh")
                        .longOpt("solrUrl")
                        .hasArg()
                        .argName("url")
                        .required()
                        .build()
        };
    }

    @Override
    protected void runImpl(CommandLine cli) throws Exception {
        final String zeppelinUrl = cli.getOptionValue("zeppelinUrl", "http://localhost:8080");
        final String solrUrl = cli.getOptionValue("solrUrl");
        final String action = cli.getOptionValue("action").toLowerCase(Locale.ROOT);

        switch (action) {
            case "bootstrap":
                bootstrapZeppelinInstallation(zeppelinUrl, solrUrl);
                break;
            case "clean":
                cleanZeppelinInstallation(cli);
                break;
            case "start":
                startZeppelinInstall();
                break;
            case "stop":
                stopZeppelin(cli);
                break;
            case "update-interpreter":
                updateSolrInterpreter(zeppelinUrl, solrUrl);
                break;
            default:
                echo("Invalid action value [" + action + "]; unable to proceed");
                exit(1);
        }
    }

    private void bootstrapZeppelinInstallation(String zeppelinUrl, String solrUrl) throws Exception {
        final File zeppelinBaseDirFile = new File(ZEPP_BASE_ABS_PATH);
        if (! zeppelinBaseDirFile.exists()) {
            final boolean result = zeppelinBaseDirFile.mkdir();
            if (! result) {
                echo("Unable to create base directory [" + ZEPP_BASE_ABS_PATH + "]for Zeppelin install; exiting.");
                exit(1);
            }
        }
        echo("Zeppelin base dir created successfully at " + ZEPP_BASE_ABS_PATH);

        final File zeppelinArchiveFile = new File(ZEPP_ARCHIVE_ABS_PATH);
        if (! zeppelinArchiveFile.exists()) {
            echo("Downloading zeppelin; this may take a few minutes");
            org.apache.commons.io.FileUtils.copyURLToFile(new URL(ZEPP_ARCHIVE_URL), zeppelinArchiveFile);
        }

        final File unpackedZeppelinArchiveFile = new File(ZEPP_UNPACKED_ABS_PATH);
        if (! unpackedZeppelinArchiveFile.exists()) {
            // TODO consider replacing with commons-compress?
            final File unpackLogs = new File(ZEPP_UNPACK_LOG_ABS_PATH);
            Process ps = new ProcessBuilder().command("tar", "-xvf", ZEPP_ARCHIVE_ABS_PATH)
                    .redirectError(unpackLogs)
                    .redirectOutput(unpackLogs)
                    .directory(zeppelinBaseDirFile)
                    .start();
            final boolean completed = ps.waitFor(PROCESS_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
            if (! completed) {
                echo("Zeppelin could not be unpacked within " + PROCESS_WAIT_TIME_SECONDS + "s; unable to install Zeppelin");
                exit(1);
            }

            if (0 != ps.exitValue()) {
                echo("Attempt to unpack Zeppelin archive failed with code " + ps.exitValue() + "; exiting");
                exit(1);
            } else {
                echo("Zeppelin successfully downloaded and unpacked to " + zeppelinBaseDirFile.getAbsolutePath());
            }

            if (unpackLogs.exists()) {
                unpackLogs.delete();
            }
        }

        echo("Finished initializing Zeppelin sandbox, attempting to start zeppelin...");
        startZeppelinInstall();
        echo("Finished starting zeppelin, attempting to update zeppelin-solr plugin");
        runCommand(ZEPP_INTERP_ABS_PATH, "--name", "solr", "--artifact", "com.lucidworks.zeppelin:zeppelin-solr:0.1.6");
        echo("Finished installing zeppelin-solr plugin, attempting to restart zepplin");
        runCommand(ZEPP_DAEMON_ABS_PATH, "restart");
        echo("Finished restarting zeppelin, attempting to update 'solr' interpreter (sleeping for a few first)");

        //TODO do a spin-wait until Zeppelin starts responding to requests instead of sleeping.
        //Thread.sleep(60 * 1000);
        //updateSolrInterpreter(zeppelinUrl, solrUrl);
    }

    private void cleanZeppelinInstallation(CommandLine cli) throws Exception {
        if (new File(ZEPP_DAEMON_ABS_PATH).exists() && isZeppelinRunning()) {
            stopZeppelin(cli);
        }

        org.apache.commons.io.FileUtils.deleteDirectory(new File(ZEPP_BASE_ABS_PATH));
    }

    private void startZeppelinInstall() throws Exception {
        runCommand(ZEPP_DAEMON_ABS_PATH, "start");
    }

    private void stopZeppelin(CommandLine cli) throws Exception {
        if (! new File(ZEPP_DAEMON_ABS_PATH).exists()) {
            echoIfVerbose("No Zeppelin sandbox exists to stop; done.", cli);
            exit(0);
        }

        if (! isZeppelinRunning()) {
            echoIfVerbose("Zeppelin was already stopped", cli);
            exit(0);
        }

        echoIfVerbose("Stopping Zeppelin using executable: " + ZEPP_DAEMON_ABS_PATH, cli);
        runCommand(ZEPP_DAEMON_ABS_PATH, "stop");
    }

    private void updateSolrInterpreter(String zeppelinUrl, String solrUrl) throws Exception {
        final File interpreterEntityFile = createInterpreterFromTemplate(solrUrl);
        // TODO Check whether we need to create or update instead of always creating
        try (final CloseableHttpClient httpClient = HttpClientUtil.createClient(new ModifiableSolrParams())) {
            final HttpPost request = new HttpPost();
            final URI requestURI = new URI(zeppelinUrl + "/api/interpreter/setting");
            echo ("POSTing solr interpreter update to " + requestURI.toString());
            request.setURI(requestURI);

            echo ("POST entity will be file: " + interpreterEntityFile.getAbsolutePath());
            request.setEntity(new FileEntity(interpreterEntityFile, ContentType.APPLICATION_JSON));
            final HttpResponse response = httpClient.execute(request);
            if (response.getStatusLine().getStatusCode() > 299) {
                echo("Received [" + response.getStatusLine().getStatusCode() + "] status when creating Solr interpreter; aborting.");
                response.getEntity().writeTo(System.err);
                exit(1);
            }
        } catch (Exception e) {
            echo("Error encountered creating/updating Solr interpreter " + e.getMessage());
            e.printStackTrace(System.err);
        }

        interpreterEntityFile.delete();
        echo("Successfully created Solr interpreter");
    }

    private File createInterpreterFromTemplate(String solrUrl) throws IOException {
        final File interpreterTemplateFile = new File(ZEPP_INTERPRETER_TEMPLATE_ABS_PATH);
        final String interpreterTemplate = org.apache.commons.io.FileUtils.readFileToString(interpreterTemplateFile, StandardCharsets.UTF_8);
        final String interpreter = interpreterTemplate.replaceAll("@@SOLR_URL@@", solrUrl);

        final File interpreterFile = new File(ZEPP_INTERPRETER_ABS_PATH);
        FileUtils.writeStringToFile(interpreterFile, interpreter, StandardCharsets.UTF_8, false);
        return interpreterFile;
    }

    private boolean isZeppelinRunning() {
        return false; // TODO implement Zeppelin status - API call?  executable?  port scan on localhost?
    }

    private void ensureZeppelinInstallationBootstrapped(String action) {
        final File zeppelinScriptFile = new File(ZEPP_DAEMON_ABS_PATH);
        if (! zeppelinScriptFile.exists()) {
            echo("Unable to " + action + " Zeppelin installation; no bootstrapped install exists.");
        }
    }

    private void runCommand(String executable, String... args) throws Exception {
        final List<String> allArgs = new ArrayList<>();
        allArgs.add(executable);
        for (String arg : args) allArgs.add(arg);
        final String debugString = String.join(" ", allArgs);

        final Process ps = new ProcessBuilder().command(allArgs).start();
        final boolean completed = ps.waitFor(PROCESS_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        if (! completed) {
            echo("Command [" + debugString + "] did not finish within " + PROCESS_WAIT_TIME_SECONDS + " seconds; aborting");
            exit(1);
        }

        if (0 != ps.exitValue()) {
            echo("Command [" + debugString + "] failed with code: " + ps.exitValue() + ".  Aborting.");
            exit(1);
        }
    }

    private static void exit(int exitStatus) {
        try {
            System.exit(exitStatus);
        } catch (java.lang.SecurityException secExc) {
            if (exitStatus != 0)
                throw new RuntimeException("SolrCLI failed to exit with status "+exitStatus);
        }
    }
}
