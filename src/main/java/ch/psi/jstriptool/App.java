package ch.psi.jstriptool;

import ch.psi.jstriptool.SwingUtils.OptionResult;
import ch.psi.jstriptool.SwingUtils.OptionType;
import java.awt.Dimension;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JFrame;
import javax.swing.UIManager;
import static javax.swing.WindowConstants.DO_NOTHING_ON_CLOSE;
import static javax.swing.WindowConstants.HIDE_ON_CLOSE;
import org.epics.ca.impl.ProtocolConfiguration;

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

    public static boolean isDebug(){
        return App.hasArgument("debug");
    }
          
    public static boolean getHideInvalid(){
        return App.hasArgument("hide_invalid");
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

    static final String DARK_LOOK_AND_FEEL = "com.formdev.flatlaf.FlatDarculaLaf";

    public static boolean isDark() {
        return UIManager.getLookAndFeel().getClass().getName().equals(DARK_LOOK_AND_FEEL);
    }

    public static boolean isSimulated() {
        return simulated;
    }

    public static boolean isAntiAliasing() {
        return hasArgument("aa");
    }
    
    
    public static boolean isMac(){
        return System.getProperty("os.name").toLowerCase().contains("mac");
    }

    static void printStartupMessage() {
        System.out.println("JStripTool");
        String version = getApplicationVersion();
        if (version != null) {
            System.out.println("Version " + version);
        }
        System.out.println("\n");
    }

    static void printHelpMessage() {
        System.out.println("Usage: jstriptool [-arg[=value]] filename");
        System.out.println("Arguments: ");
        System.out.println("\t-l\t\tProvide channel list instead of fileneme: \"[CHANNEL 1] [CHANNEL 2] ...\"");
        System.out.println("\t-?\t\tPrint this help message");
        System.out.println("\t-config\t\tShow config dialog even if filename is provided");
        System.out.println("\t-home=<dir>\tSet home folder");
        System.out.println("\t-default=<dir>\tSet default configuration file");
        System.out.println("\t-laf=<name>\tSupported values: nimbus, metal, dark or system");
        System.out.println("\t-clog=<level>\tSet the console logging level");
        System.out.println("\t-aa\t\tEnable anti-aliasing");
        System.out.println("\t-debug\t\tShow debug information");
        System.out.println("\t-cli  \t\tStarts the CLI");
        System.out.println("\t-hide_invalid\tDo not display invalid values");
        System.out.println("\t-xrm=\"...\"\tSet graphical resources in the format: <name>:<value>");
        System.out.println("\t-cmd=\"...\"\tList of commands for changing the plot configuration (';' separated):");
        System.out.println("\t\t\t  add <channel>                    (add a plot)");
        System.out.println("\t\t\t  del <channel or index>           (remove a plot)");
        System.out.println("\t\t\t  min <channel or index> <value>   (set plot minimum value)");
        System.out.println("\t\t\t  max <channel or index> <value>   (set plot maximum value)");
        System.out.println("\t\t\t  range <channel or index> <range> (set plot range)");
        System.out.println("\t\t\t  log <channel or index> <value>   (set plot logarithmic/linear)");
        System.out.println("\t\t\t  color <channel or index> <color> (set plot color)");
        System.out.println("\t\t\t  span <time in seconds>           (set plot time span)");
        System.out.println("\t\t\t  poll <time in seconds>           (set channel update interval - 0 for monitored)");
        System.out.println("\t\t\t  redraw <time in seconds>         (set plot redraw interval)");
        System.out.println("\t\t\tCommands can be entered in the CLI using the same format");
        System.out.println("\nEPICS CA arguments: ");
        for (ProtocolConfiguration.PropertyNames cfg : ProtocolConfiguration.PropertyNames.values()) {
            System.out.println("\t-" + cfg.toString() + "=<value>");
        }
        System.out.println("\nEnvironmennt variables: ");
        System.out.println("\tSTRIP_SITE_DEFAULTS");
        System.out.println("\tSTRIP_FILE_SEARCH_PATH");
        System.out.println("\n");
    }
    
    public static void main(String args[]) throws Exception {
        arguments = args;
        final Object startLock = new Object();
        printStartupMessage();
        if (hasArgument("h")) {
            printHelpMessage();
            System.exit(0);
        };
        
        // Otherwise org.epics:ca on Java 21 uses IPv6
        if (!hasArgument("ipv6")) {
            System.setProperty("java.net.preferIPv4Stack", "true");        
        }
        
        //Console log level is warning, unless otherwise specified
        Level consoleLogLevel = Level.WARNING;        
        try{
            consoleLogLevel = Level.parse(getArgumentValue("clog"));
        } catch (Exception ex){            
        }
        for (Handler handler : Logger.getLogger("").getHandlers()) {
            if (handler instanceof ConsoleHandler) {
                handler.setLevel(consoleLogLevel);
            }
        }
        if (!System.getProperties().contains("CA_LIBRARY_LOG_LEVEL")){
            System.getProperties().put("CA_LIBRARY_LOG_LEVEL", consoleLogLevel.toString());
        }
        if (!System.getProperties().contains("CA_REPEATER_LOG_LEVEL")){
            System.getProperties().put("CA_REPEATER_LOG_LEVEL", consoleLogLevel.toString());
        }

        caProperties = new Properties();
        for (ProtocolConfiguration.PropertyNames cfg : ProtocolConfiguration.PropertyNames.values()) {
            String val = getArgumentValue(cfg.toString());
            if (val != null) {
                caProperties.setProperty(cfg.toString(), val);
            }
        }
        
        if (isDebug()) {
            caProperties.setProperty("CA_DEBUG", "1");
        }     
        
        //Split ignoting quotes
        String lastArg = null;
        if (args.length > 0){
            String[] tokens =  String.join(" ", args).split(" (?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            lastArg = tokens[tokens.length-1].trim();
            if ((lastArg.length()>1) && (lastArg.startsWith("\""))&& (lastArg.endsWith("\""))){
                //On macos the command line splits entries in quotes containing spaces
                lastArg = lastArg.substring(1, lastArg.length()-1);
            } else {
                //On linux not, and the quotes are removed
                lastArg = args[args.length - 1].trim();
            }
        }
        
        boolean isChannelList =  hasArgument("l");
        String startup = (lastArg != null) && 
                         (!lastArg.isEmpty()||isChannelList) && 
                          !lastArg.startsWith("-") && 
                            !lastArg.startsWith("#")
                ? lastArg : null;
        
        
        String startupFile = isChannelList ? null : startup;
        String[] startupChannels = (isChannelList  && (startup!=null)) ? startup.split(" ") : null;

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

        if (isDebug()){
            System.out.println("Home folder: " + getHome());
            System.out.println("Startup: " + startup);
        }
    
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
                if (startup != null) {
                    plotFrame.setVisible(true);
                    try {
                        if (startupFile != null){
                            configFrame.open(resolveFile(startupFile));
                        } else if (startupChannels != null) {
                            configFrame.load(startupChannels);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                        Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
                        if (startupFile != null){
                            forceConfig = true;
                        }
                    }
                } else if (startupChannels != null) {
                    
                }

                if ((forceConfig) || (startup == null)) {
                    configFrame.setVisible(true);
                    configFrame.setLocationRelativeTo(null);
                }
                
                for (String cmd : App.getArgumentValues("cmd")){
                    for (String statement : cmd.split(";")){
                        statement = statement.trim();
                        if (!statement.isBlank()){
                            try {
                                System.out.println("Executing command: " + statement);
                                executeStatement(statement);
                            } catch (Exception ex) {
                                Logger.getLogger(App.class.getName()).log(Level.SEVERE, null, ex);
                            }
                        }
                    }
                }
                synchronized(startLock){
                    startLock.notifyAll();
                }
            }
        });
                
        synchronized(startLock){
            startLock.wait();
        }
        
        if (hasArgument("cli")){
            //Run console        
            String cursor = "> ";
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                while (true) {
                    System.out.print(cursor);
                    String statement = null;
                    try {
                        statement = reader.readLine();
                    } catch (IOException ex) {
                    }
                    if (statement == null) {
                        break;
                    }

                    try {
                        Object ret = executeStatement(statement);
                        if (ret != null){
                            System.out.println(ret);
                        }
                    } catch (Exception ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }        
    }
    
    static Object executeStatement(String statement) throws Exception{
        statement = statement.trim();
        if (!statement.isBlank()){
            String[] tokens=statement.split(" ");
            String op = tokens[0];
            switch (op){
                case "add":                            
                    configFrame.addChannel(tokens[1]);
                    return "Ok";    
                case "del":   
                    configFrame.removeChannel(configFrame.getIndex(tokens[1]));
                    return "Ok";    
                case "range":   
                    configFrame.setChannelMin(configFrame.getIndex(tokens[1]), Double.valueOf(tokens[2]));
                    configFrame.setChannelMax(configFrame.getIndex(tokens[1]), Double.valueOf(tokens[3]));
                    return "Ok";    
                case "min":   
                    configFrame.setChannelMin(configFrame.getIndex(tokens[1]), Double.valueOf(tokens[2]));
                    return "Ok";    
                case "max":   
                    configFrame.setChannelMax(configFrame.getIndex(tokens[1]), Double.valueOf(tokens[2]));
                    return "Ok";    
                case "log":   
                    configFrame.setChannelLog(configFrame.getIndex(tokens[1]), Boolean.valueOf(tokens[2]));
                    return "Ok";    
                case "color":   
                    configFrame.setChannelColor(configFrame.getIndex(tokens[1]), tokens[2]);
                    return "Ok";    
                case "channels":   
                    return String.join("\n", configFrame.config.getCurvesNames().toArray(new String[0]));
                case "indexes":  
                    List<Integer> indexes =  configFrame.config.getCurvesIndexes();
                    String[] names = new String[indexes.size()];
                    for (int i=0; i< names.length; i++){
                        names[i] = String.valueOf(indexes.get(i));
                    }
                    return String.join("\n", names);     
                case "span":
                    configFrame.setTimespan(Integer.valueOf(tokens[1]));
                    return "Ok";    
                case "poll":
                    configFrame.setSampleInterval(Double.valueOf(tokens[1]));
                    return "Ok";
                case "redraw":
                    configFrame.setRedrawInterval(Double.valueOf(tokens[1]));
                    return "Ok";             
                default:
                    return "Invalid command";
            }                    
        }
        return null;
    }
}
