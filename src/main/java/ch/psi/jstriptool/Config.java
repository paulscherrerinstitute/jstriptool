package ch.psi.jstriptool;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.epics.ca.Channel;
import org.epics.ca.Context;

/**
 *
 */
public class Config {

    public static final String EXTENSION = "stp";
    public static final String VERSION = "1.2";
    public static final int MAX_NUMBER_PLOTS = 10;

    public static class DeepColor {

        public final int r;
        public final int g;
        public final int b;
        final static int SYSTEM_DEFAULT = -1;

        public DeepColor(int r, int g, int b) {
            if (isDefault(r, g, b)) {
                this.r = r;
                this.g = g;
                this.b = b;
            } else {
                this.r = Math.min(Math.max(r, 0), 65535);
                this.g = Math.min(Math.max(g, 0), 65535);
                this.b = Math.min(Math.max(b, 0), 65535);
            }
        }

        public DeepColor(Color c) {
            this(c.getRed() * 256, c.getGreen() * 256, c.getBlue() * 256);
        }

        //Use default system color
        public DeepColor() {
            this(SYSTEM_DEFAULT, SYSTEM_DEFAULT, SYSTEM_DEFAULT);
        }

        boolean isDefault(int r, int g, int b) {
            return ((r == SYSTEM_DEFAULT) || (g == SYSTEM_DEFAULT) || (b == SYSTEM_DEFAULT));
        }

        public boolean isDefault() {
            return isDefault(r, g, b);
        }

        public Color toColor() {
            if (isDefault()) {
                return null;
            }
            return new Color(r / 256, g / 256, b / 256);
        }
    }

    public static class Curve {

        String name;
        String units;
        String comment;
        int precision;
        double min;
        double max;
        Scale scale;
        Boolean plotStatus;

        @Override
        public Curve clone() {
            Curve ret = new Curve();
            ret.name = name;
            ret.units = units;
            ret.comment = comment;
            ret.precision = precision;
            ret.min = min;
            ret.max = max;
            ret.scale = scale;
            ret.plotStatus = plotStatus;
            return ret;
        }
        
        public boolean isEnabled(){
            return  (name!=null) && ((plotStatus==null) || (plotStatus));
        }
    }

    public enum GridVisibility {
        none,
        some,
        all
    }

    public enum LabelColorAxisY {
        foreground,
        selectedCurve,
    }

    public enum Scale {
        linear,
        logarithmic
    }

    String version = VERSION;

    //Time
    int timespan = 300; //s
    int numSamples = 7200;
    double sampleInterval = 1.0; //s
    double refreshInterval = 1.0; //s

    //Color 
    DeepColor foreground = new DeepColor(0, 0, 0);
    DeepColor background = new DeepColor(65535, 65535, 65535);
    DeepColor grid = new DeepColor(49087, 49087, 49087);
    DeepColor[] colors = new DeepColor[]{
        new DeepColor(0, 0, 65535),
        new DeepColor(27499, 36494, 8995),
        new DeepColor(42405, 10794, 10794),
        new DeepColor(24415, 40606, 41120),
        new DeepColor(65535, 42405, 0),
        new DeepColor(41120, 8224, 61680),
        new DeepColor(65535, 0, 0),
        new DeepColor(65535, 55255, 0),
        new DeepColor(48316, 36751, 36751),
        new DeepColor(39578, 52685, 12850),};

    //Option
    GridVisibility gridXon = GridVisibility.some;
    GridVisibility gridYon = GridVisibility.some;
    LabelColorAxisY axisYcolorStat = LabelColorAxisY.selectedCurve;
    int graphLineWidth = 2;
    Curve[] curves = new Curve[MAX_NUMBER_PLOTS];

    List<Integer> getCurvesIndexes() {
        List<Integer> ret = new ArrayList<>();
        for (int i = 0; i < MAX_NUMBER_PLOTS; i++) {
            if (curves[i] != null) {
                ret.add(i);
            }
        }
        return ret;
    }

    int getNumberCurves() {
        for (int i = 0; i < MAX_NUMBER_PLOTS; i++) {
            if (curves[i] == null) {
                return i;
            }
        }
        return MAX_NUMBER_PLOTS;
    }

    Curve removeCurve(int index) {
        if ((index >= 0) && (index < MAX_NUMBER_PLOTS)) {
            Curve ret = curves[index];
            System.arraycopy(curves, index + 1, curves, index, MAX_NUMBER_PLOTS - index - 1);
            curves[MAX_NUMBER_PLOTS - 1] = null;
            return ret;
        }
        return null;
    }

    Curve insertCurve(int index) {
        if ((index >= 0) && (index < MAX_NUMBER_PLOTS)) {
            System.arraycopy(curves, index, curves, index + 1, MAX_NUMBER_PLOTS - index - 1);
            curves[index] = new Curve();
            return curves[index];
        }
        return null;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////
    // Persistence
    ////////////////////////////////////////////////////////////////////////////////////////////////    
    int parseInt(String[] pars, int index) {
        try {
            return Integer.valueOf(pars[index]);
        } catch (Exception ex) {
            return 0;
        }
    }

    double parseDouble(String[] pars, int index) {
        try {
            return Double.valueOf(pars[index]);
        } catch (Exception ex) {
            return 0.0;
        }
    }

    void parse(String line) {
        line = line.trim();
        int endCode = line.indexOf(" ");
        if (endCode < 0) {
            endCode = line.length();
        }
        String[] code = line.substring(0, endCode).trim().split("\\.");
        String arg = line.substring(endCode).trim();
        arg = arg.replaceAll(" +", " ");
        String[] pars = arg.split(" ");
        int index;
        switch (code[0]) {
            case "StripConfig":
                version = arg;
                break;
            case "Strip":
                switch (code[1]) {
                    case "Time":
                        switch (code[2]) {
                            case "Timespan":
                                timespan = parseInt(pars, 0);
                                break;
                            case "NumSamples":
                                numSamples = parseInt(pars, 0);
                                break;
                            case "SampleInterval":
                                sampleInterval = parseDouble(pars, 0);
                                break;
                            case "RefreshInterval":
                                refreshInterval = parseDouble(pars, 0);
                                break;
                        }
                        break;
                    case "Color":
                        DeepColor c = new DeepColor(parseInt(pars, 0),
                                parseInt(pars, 1),
                                parseInt(pars, 2));
                        switch (code[2]) {
                            case "Background":
                                background = c;
                                break;
                            case "Foreground":
                                foreground = c;
                                break;
                            case "Grid":
                                grid = c;
                                break;
                            default:
                                index = Integer.valueOf(code[2].substring(5));
                                colors[index - 1] = c;
                                break;
                        }
                        break;
                    case "Option":
                        switch (code[2]) {
                            case "GridXon":
                                switch (parseInt(pars, 0)) {
                                    case 0:
                                        gridXon = GridVisibility.none;
                                        break;
                                    case 1:
                                        gridXon = GridVisibility.some;
                                        break;
                                    case 2:
                                        gridXon = GridVisibility.all;
                                        break;
                                }
                                break;
                            case "GridYon":
                                switch (parseInt(pars, 0)) {
                                    case 0:
                                        gridYon = GridVisibility.none;
                                        break;
                                    case 1:
                                        gridYon = GridVisibility.some;
                                        break;
                                    case 2:
                                        gridYon = GridVisibility.all;
                                        break;
                                }
                                break;
                            case "AxisYcolorStat":
                                axisYcolorStat = (parseInt(pars, 0) == 0)
                                        ? LabelColorAxisY.foreground
                                        : LabelColorAxisY.selectedCurve;
                                break;
                            case "GraphLineWidth":
                                graphLineWidth = Math.min(parseInt(pars, 0), 10);
                                break;
                        }
                        break;
                    case "Curve":
                        index = Integer.valueOf(code[2]);
                        if (curves[index] == null) {
                            curves[index] = new Curve();
                        }
                        switch (code[3]) {
                            case "Name":
                                curves[index].name = arg;
                                break;
                            case "Units":
                                curves[index].units = arg;
                                break;
                            case "Comment":
                                curves[index].comment = arg;
                                break;
                            case "Precision":
                                curves[index].precision = parseInt(pars, 0);
                                break;
                            case "Min":
                                curves[index].min = parseDouble(pars, 0);
                                break;
                            case "Max":
                                curves[index].max = parseDouble(pars, 0);
                                break;
                            case "Scale":
                                curves[index].scale = (parseInt(pars, 0) == 1)
                                        ? Scale.logarithmic
                                        : Scale.linear;
                                break;
                            case "PlotStatus":
                                curves[index].plotStatus = (parseInt(pars, 0) == 0) ? false : true;
                                break;
                        }
                        break;
                }
                break;
        }

    }

    public void open(File file) throws IOException {
        try (Stream<String> lines = Files.lines(file.toPath())) {
            lines.forEachOrdered(line -> parse(line));
        }
    }

    void write(PrintWriter out, String code, DeepColor color) {
        write(out, code, new Object[]{color.r, color.g, color.b});
    }

    void write(PrintWriter out, String code, Object par) {
        write(out, code, new Object[]{par});
    }

    void write(PrintWriter out, String code, Object[] pars) {
        StringBuilder sb = new StringBuilder();
        sb.append(StringUtils.rightPad(code, 30));
        for (Object par : pars) {
            String str = String.valueOf(par);
            sb.append((pars.length > 1) ? StringUtils.rightPad(str, 10) : str);
        }
        out.println(sb.toString());
    }

    public void save(File file) throws IOException {
        try (PrintWriter out = new PrintWriter(file)) {
            write(out, "StripConfig", version);
            write(out, "Strip.Time.Timespan", timespan);
            write(out, "Strip.Time.NumSamples", numSamples);
            write(out, "Strip.Time.SampleInterval", sampleInterval);
            write(out, "Strip.Time.RefreshInterval", refreshInterval);
            write(out, "Strip.Color.Background", background);
            write(out, "Strip.Color.Foreground", foreground);
            write(out, "Strip.Color.Grid", grid);
            for (int i = 0; i < MAX_NUMBER_PLOTS; i++) {
                write(out, "Strip.Color.Color" + (i + 1), colors[i]);
            }
            write(out, "Strip.Option.GridXon", gridXon.ordinal());
            write(out, "Strip.Option.GridYon", gridYon.ordinal());
            write(out, "Strip.Option.AxisYcolorStat", axisYcolorStat.ordinal());
            write(out, "Strip.Option.GraphLineWidth ", graphLineWidth);

            List<Integer> curvesIndexes = getCurvesIndexes();
            for (Integer i : curvesIndexes) {
                if (curves[i].name!=null) write(out, "Strip.Curve." + i + ".Name", curves[i].name);
            }
            for (Integer i : curvesIndexes) {
                if (curves[i].units!=null) write(out, "Strip.Curve." + i + ".Units", curves[i].units);
            }
            for (Integer i : curvesIndexes) {
                if (curves[i].comment!=null) write(out, "Strip.Curve." + i + ".Comment", curves[i].comment);
            }
            for (Integer i : curvesIndexes) {
                write(out, "Strip.Curve." + i + ".Precision", curves[i].precision);

            }
            for (Integer i : curvesIndexes) {
                write(out, "Strip.Curve." + i + ".Min", curves[i].min);
            }
            for (Integer i : curvesIndexes) {
                write(out, "Strip.Curve." + i + ".Max", curves[i].max);
            }
            for (Integer i : curvesIndexes) {
                if (curves[i].scale!=null) write(out, "Strip.Curve." + i + ".Scale", curves[i].scale.ordinal());
            }
            for (Integer i : curvesIndexes) {
                if (curves[i].plotStatus!=null)  write(out, "Strip.Curve." + i + ".PlotStatus ", curves[i].plotStatus ? 1 : 0);
            }
        }
    }

    public static Color getColorFromString(String str) {
        if (str == null) {
            return null;
        }
        String[] tokens = str.split(",");
        if (tokens.length == 3) {
            return new Color(Integer.valueOf(tokens[0].trim()), Integer.valueOf(tokens[1].trim()), Integer.valueOf(tokens[2].trim()));
        }
        try {
            return (Color) Color.class.getField(str.trim()).get(null);
        } catch (Exception ex) {
            return null;
        }
    }

    public static String getStringFromColor(Color color) {
        return (color == null) ? "" : color.getRed() + "," + color.getGreen() + "," + color.getBlue();
    }

    @Override
    public Config clone() {
        Config config = new Config();
        config.timespan = timespan;
        config.numSamples = numSamples;
        config.sampleInterval = sampleInterval;
        config.refreshInterval = refreshInterval;
        config.foreground = foreground;
        config.background = background;
        config.grid = grid;
        for (int i = 0; i < config.colors.length; i++) {
            config.colors[i] = colors[i] == null ? null : colors[i];
        }
        config.gridXon = gridXon;
        config.gridYon = gridYon;
        config.axisYcolorStat = axisYcolorStat;
        config.graphLineWidth = graphLineWidth;
        for (int i = 0; i < config.curves.length; i++) {
            config.curves[i] = curves[i] == null ? null : curves[i].clone();
        }
        return config;
    }
    
    public static String getChannelDesc(Context context, String channelName){
        String desc = "";
        try {
            Channel<String> channelDesc = context.createChannel(channelName + ".DESC", String.class);
            channelDesc.connectAsync().get(2, TimeUnit.SECONDS);
            desc = channelDesc.get();
        } catch (Exception ex) {
        }
        return desc;
    }
                    
    public static void main(String[] args) throws IOException {
        Config config = new Config();
        config.open(new File("./test.stp"));
        config.save(new File("./out.stp"));
    }

}
