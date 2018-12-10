package ch.psi.jstriptool;

import ch.psi.jstriptool.SwingUtils.OptionResult;
import ch.psi.jstriptool.SwingUtils.OptionType;
import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowListener;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import javax.swing.UIManager;
import static javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE;
import static javax.swing.WindowConstants.HIDE_ON_CLOSE;
import org.epics.ca.Context;

/**
 *
 */
public class App {

    private static String[] arguments;
    private static Properties caProperties;
    private static List<String> path = new ArrayList<>();
    static Config defaultConfig;
    static boolean simulated;

    //Arguments
    static public String[] getArguments() {
        return arguments;
    }

    static public Properties getCaProperties() {
        return caProperties;
    }

    /**
     * Returns the command line argument value for a given key, if present. If
     * not defined returns null. If defined multiple times then returns the
     * latest.
     */
    static public String getArgumentValue(String name) {
        List<String> values = getArgumentValues(name);
        int entries = values.size();
        if (entries <= 0) {
            return null;
        }
        return values.get(entries - 1);
    }

    /**
     * Returns true if argument value is set and not empty
     */
    static public boolean isArgumentDefined(String name) {
        return ((getArgumentValue(name) != null) && (getArgumentValue(name).length() > 0));
    }

    /**
     * Returns the command line argument values for a given key. If key is no
     * present then returns an empty list.
     */
    static public List<String> getArgumentValues(String name) {
        ArrayList<String> argumentValues = new ArrayList<>();
        for (String arg : arguments) {
            if (arg.startsWith("-")) {
                arg = arg.substring(1);
            }
            String[] tokens = arg.split("=");
            if ((tokens.length == 2) && (tokens[0].equals(name))) {
                String ret = tokens[1].trim();
                if (ret.length() >= 1) {
                    argumentValues.add(ret);
                }
            }
        }
        return argumentValues;
    }

    static public boolean hasArgument(String name) {
        if (arguments != null) {
            for (String arg : arguments) {
                if (arg.startsWith("-")) {
                    arg = arg.substring(1);
                }
                if (arg.equals(name)) {
                    return true;
                }
                String[] tokens = arg.split("=");
                if ((tokens.length == 2) && (tokens[0].equals(name))) {
                    return true;
                }
            }
        }
        return false;
    }

    static ConfigFrame configFrame;
    static PlotFrame plotFrame;

    public Config getConfig() {
        return configFrame.config;
    }

    public Config getDefaultConfig() {
        return defaultConfig;
    }

    public static File getJarFile() {
        return new java.io.File(App.class.getProtectionDomain()
                .getCodeSource()
                .getLocation()
                .getPath());
    }

    public static String getApplicationVersion() {
        try {
            JarInputStream jarStream = new JarInputStream(new FileInputStream(getJarFile()));
            Manifest manifest = jarStream.getManifest();
            Attributes attr = manifest.getMainAttributes();
            String version = attr.getValue("Implementation-Version");
            String buildTime = attr.getValue("Build-Time");
            return version + " (build " + buildTime + ")";
        } catch (Exception ex) {
            return null;
        }
    }

    static String expandUserHome(String folder) {
        return folder.replaceFirst("^~", System.getProperty("user.home"));
    }

    public static boolean isBlocking() {
        return App.hasArgument("sync");
    }

    public static File resolveFile(String name) throws FileNotFoundException {
        name = name.trim();
        File file = new File(expandUserHome(name));
        if (file.exists()) {
            return file;
        }
        for (String folder : path) {
            file = Paths.get(folder.trim(), name).toFile();
            if (file.exists()) {
                return file;
            }
        }

        throw new FileNotFoundException(name);
    }

    public static String getHome() {
        String userHome = String.valueOf(System.getProperty("user.home"));
        String home = getArgumentValue("home");
        if (home != null) {
            try {
                return new File(expandUserHome(home.trim())).getCanonicalPath();
            } catch (IOException ex) {
                Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return userHome;
    }

    static final String DARK_LOOK_AND_FEEL = "com.bulenkov.darcula.DarculaLaf";

    public static boolean isDark() {
        return UIManager.getLookAndFeel().getClass().getName().equals(DARK_LOOK_AND_FEEL);
    }

    public static boolean isSimulated() {
        return simulated;
    }

    public static boolean isAntiAliasing() {
        return !String.valueOf(getArgumentValue("aa")).equalsIgnoreCase("false");
    }

    static void printStartupMessage() {
        System.out.println("JStripChart");
        String version = getApplicationVersion();
        if (version != null) {
            System.out.println("Version " + version);
        }
        System.out.println("\n");
    }

    static void printHelpMessage() {
        System.out.println("Usage: jstriptool [-arg[=value]] filename");
        System.out.println("Arguments: ");
        System.out.println("\t-?\t\tPrint this help message");
        System.out.println("\t-config\t\tShow config dialog even if filename is provided");
        System.out.println("\t-home=<dir>\tSet home folder");
        System.out.println("\t-default=<dir>\tSet default configuration file");
        System.out.println("\t-laf=<name>\tSupported values: nimbus, metal, dark or system");
        System.out.println("\t-aa=false\tDisable anti-aliasing");
        System.out.println("\t-xrm='<name>:<val>'\tSet graphical resources");
        System.out.println("EPICS CA arguments: ");
        for (Context.Configuration cfg : Context.Configuration.values()) {
            System.out.println("\t-" + cfg.toString() + "=<value>");
        }
        System.out.println("Environmennt variables: ");
        System.out.println("\tSTRIP_SITE_DEFAULTS");
        System.out.println("\tSTRIP_FILE_SEARCH_PATH");
        System.out.println("\n");
    }

    public static void main(String args[]) throws Exception {
        arguments = args;

        printStartupMessage();
        if (hasArgument("h")) {
            printHelpMessage();
            System.exit(0);
        };

        caProperties = new Properties();
        for (Context.Configuration cfg : Context.Configuration.values()) {
            String val = getArgumentValue(cfg.toString());
            if (val != null) {
                caProperties.setProperty(cfg.toString(), val);
            }
        }
        String lastArg = args.length > 0 ? args[args.length - 1].trim() : null;
        String startupFile = (lastArg != null) && !lastArg.isEmpty() && !lastArg.startsWith("-") && !lastArg.startsWith("#")
                ? lastArg : null;

        String laf = getArgumentValue("laf");
        if (laf != null) {
            switch (laf) {
                case "nimbus":
                    for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                        if ("Nimbus".equals(info.getName())) {
                            javax.swing.UIManager.setLookAndFeel(info.getClassName());
                            break;
                        }
                    }
                    break;
                case "dark":
                    UIManager.setLookAndFeel(DARK_LOOK_AND_FEEL);
                    break;
                case "metal":
                    UIManager.setLookAndFeel(UIManager.getCrossPlatformLookAndFeelClassName());
                    break;
                case "system":
                    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
                    break;
                default:
                    try {
                        UIManager.setLookAndFeel(laf);
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                    break;
            }
        }

        if (isBlocking()) {
            System.out.println("Blocking mode");
        }
        if (!hasArgument("noraise")) {
            PlotPanel.setBringSelectedToTheTop(true);
        }
        simulated = hasArgument("simulated");

        System.out.println("Home folder: " + getHome());
        System.out.println("Startup file: " + startupFile);

        //Builds the search path
        path.add(".");
        path.add(expandUserHome("~"));
        for (String folder : getArgumentValues("home")) {
            path.add(expandUserHome(folder));
        }
        String searchPath = System.getenv("STRIP_FILE_SEARCH_PATH");
        if (searchPath != null) {
            for (String folder : searchPath.split(":")) {
                folder = folder.trim();
                if (!folder.isEmpty()) {
                    path.add(folder);
                }
            }
        }

        //Loads default configuration
        String[] defaults = new String[]{getArgumentValue("default"), System.getenv("STRIP_SITE_DEFAULTS")};
        for (String filename : defaults) {

            if (filename != null) {
                File defaultConfigFile = new File(expandUserHome(filename));
                if (defaultConfigFile.exists()) {
                    System.out.println("Default file: " + filename);
                    defaultConfig = new Config();
                    defaultConfig.open(defaultConfigFile);
                } else {
                    System.out.println("Default file not found: " + filename);
                }
            }
        }

        //Checks resources
        int frameWidth = 0;
        int frameHeight = 0;
        for (String resource : getArgumentValues("xrm")) {
            try {
                if (resource.contains(":")) {
                    String name = resource.substring(0, resource.indexOf(":"));
                    String value = resource.substring(resource.indexOf(":") + 1);
                    switch (name) {
                        case "StripTool.StripGraph.width":
                            frameWidth = Integer.valueOf(value);
                            break;
                        case "StripTool.StripGraph.height":
                            frameHeight = Integer.valueOf(value);
                            break;
                    }
                    System.out.println("Resource: " + name + " = " + value);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        Dimension frameSize = new Dimension(frameWidth, frameHeight);

        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                plotFrame = new PlotFrame();
                configFrame = new ConfigFrame();

                WindowListener windowListener = new WindowAdapter() {
                    @Override
                    public void windowClosing(java.awt.event.WindowEvent e) {
                        JFrame other = ((JFrame) e.getWindow()) == plotFrame ? configFrame : plotFrame;
                        if (!other.isShowing()) {
                            if (SwingUtils.showOption(e.getWindow(), "Close",
                                    "Do you want to close the application?", OptionType.YesNo) == OptionResult.Yes) {
                                System.exit(0);
                            } else {
                                ((JFrame) e.getWindow()).setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
                            }
                        } else {
                            ((JFrame) e.getWindow()).setDefaultCloseOperation(HIDE_ON_CLOSE);
                        }
                    }
                };
                plotFrame.addWindowListener(windowListener);
                configFrame.addWindowListener(windowListener);

                if (frameSize.width > 0) {
                    plotFrame.setSize(new Dimension(frameSize.width, plotFrame.getHeight()));
                }
                if (frameSize.height > 0) {
                    plotFrame.setSize(new Dimension(plotFrame.getWidth(), frameSize.height));
                }

                boolean forceConfig = hasArgument("config");
                if (startupFile != null) {
                    plotFrame.setVisible(true);
                    try {
                        configFrame.open(resolveFile(startupFile));
                    } catch (Exception ex) {
                        Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
                        forceConfig = true;
                    }
                }

                if ((forceConfig) || (startupFile == null)) {
                    configFrame.setVisible(true);
                    configFrame.setLocationRelativeTo(null);
                }
            }
        });
    }
}
