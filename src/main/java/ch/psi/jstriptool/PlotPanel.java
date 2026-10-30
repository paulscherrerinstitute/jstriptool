package ch.psi.jstriptool;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.event.ActionEvent;
import java.awt.geom.Line2D;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.swing.JCheckBoxMenuItem;
import javax.swing.JFileChooser;
import javax.swing.JMenu;
import javax.swing.JMenuItem;
import javax.swing.JPopupMenu;
import javax.swing.event.PopupMenuEvent;
import javax.swing.event.PopupMenuListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.CrosshairState;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.PlotRenderingInfo;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYItemRendererState;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.Range;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
import org.jfree.data.xy.XYDataItem;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.RectangleInsets;
import ch.psi.jstriptool.SwingUtils.ExtensionFileFilter;

public class PlotPanel extends javax.swing.JPanel {

    static final double AUTO_RANGE_MINIMUM_SIZE = 1e-12; //TODO: if smaller there are plot repainting problem in scans in JFreeChart > 1.0.18
    static final double AUTO_RANGE_LOG_MINIMUM_SIZE = 1e-32;
    static final int PREFERRED_WIDTH = 600;
    static final int PREFERRED_HEIGHT = 400;
    protected static final String LINE_SEPARATOR = System.lineSeparator();
    protected static final String FIELD_SEPARATOR = "\t";
    static final double AXIS_MARGIN = 0.005;
    final static SimpleDateFormat DATE_TIME_FORMAT = new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSS");
    final static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.S");
    final static DecimalFormat DECIMAL_FORMAT = new DecimalFormat("0.##########");
    
    static final Color DEFAULT_TEXT_COLOR = Color.DARK_GRAY;
    static final Color DEFAULT_DARK_TEXT_COLOR = new Color(187, 187, 187);
    static final Color DEFAULT_BACKGROUND_COLOR = null;
    static final Color DEFAULT_DARK_BACKGROUND_COLOR = new Color(43, 43, 43);
    static final Color DEFAULT_GRID_COLOR = Color.LIGHT_GRAY;
    static final Color DEFAULT_DARK_GRID_COLOR = new Color(187, 187, 187) ;    
    static final Color TRANSPARENT_COLOR = new Color(1, 0, 0, 0);    
    static final int LINE_EXTENSION_TRANSPARENCY = 64; 
    


    final JFreeChart chart;
    final ChartPanel chartPanel;
    boolean autoScale = false;
    boolean scrollMode = true;

    class SeriesInfo {

        SeriesInfo(int index, PlotSeries plotSeries, TimeSeries timeSeries, TimeSeriesCollection data) {
            this.index = index;
            this.plotSeries = plotSeries;
            this.timeSeries = timeSeries;
            this.data = data;
        }
        final int index;
        final PlotSeries plotSeries;
        final TimeSeries timeSeries;
        final TimeSeriesCollection data;

        XYLineAndShapeRenderer getRenderer() {
            return PlotPanel.this.getRenderer(index);
        }

        NumberAxis getAxis() {
            return (NumberAxis) chart.getXYPlot().getRangeAxis(index);
        }

        Range getRange() {
            return plotSeries.isFixedRange() ? new Range(plotSeries.getRangeMin(), plotSeries.getRangeMax()) : null;
        }
        
        void setLineExtension(boolean value){
            if (getRenderer() instanceof ExtendedLineAndShapeRenderer renderer){
                renderer.setLineExtension(value);
            }
        }   
        
    }
    final List<SeriesInfo> series;

    Font tickLabelFont = new Font(Font.SANS_SERIF, 0, 10);
    Font labelFont = new Font(Font.SANS_SERIF, 0, 11);

    static boolean bringSelectedToTheTop = false;

    public static boolean getBringSelectedToTheTop() {
        return bringSelectedToTheTop;
    }

    public static void setBringSelectedToTheTop(boolean value) {
        bringSelectedToTheTop = value;
    }

    boolean markersVisible = true;
    boolean legendVisible = true;

    private boolean started = false;
    private boolean closed = false;

    private final Paint systemTickMarkPaint;
    private final Paint systemAxisLinePaint;

    public PlotPanel() {
        initComponents();

        series = new ArrayList<>();
        chart = ChartFactory.createTimeSeriesChart(null,
                "Time",
                null,
                new TimeSeriesCollection(),
                true,
                true,
                false
        );

        // Customize legend
        chart.getLegend().setVisible(false);
        chart.getLegend().setBackgroundPaint(null);
        chart.getLegend().setBorder(0, 0, 0, 0);

        final XYPlot plot = chart.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        axis.setAutoRange(true);
        axis.setFixedAutoRange(60000.0);  // 60 seconds

        axis = plot.getRangeAxis();
        axis.setAutoRange(true);
        updateLafColors();
        plot.getDomainAxis().setLabelFont(labelFont);
        plot.getRangeAxis().setLabelFont(labelFont);
        plot.getDomainAxis().setTickLabelFont(tickLabelFont);
        plot.getRangeAxis().setTickLabelFont(tickLabelFont);
        BasicStroke dotted = new BasicStroke(0, BasicStroke.CAP_BUTT, BasicStroke.JOIN_BEVEL, 1f, new float[]{2f}, 0f);
        plot.setDomainMinorGridlineStroke(dotted);
        plot.setRangeMinorGridlineStroke(dotted);

        ((NumberAxis) plot.getRangeAxis()).setAutoRangeIncludesZero(false);
        ((NumberAxis) plot.getRangeAxis()).setAutoRangeMinimumSize(AUTO_RANGE_MINIMUM_SIZE);               

        setLayout(new java.awt.BorderLayout());
        boolean userBuffer = !App.isMac();
        chartPanel = new ChartPanel(chart, userBuffer) {
            @Override
            public void restoreAutoRangeBounds() {
                super.restoreAutoRangeBounds();
                for (int i = 0; i < getNumberOfSeries(); i++) {
                    ValueAxis axis = plot.getRangeAxis(i);
                    Range range = getSeriesInfo(i).getRange();
                    if ((range != null) && (range.getUpperBound() > range.getLowerBound())) {
                        axis.setRangeWithMargins(range);
                    } else {
                        axis.setAutoRange(true);
                    }
                }
                chart.getXYPlot().getDomainAxis().setAutoRange(scrollMode);
            }
        };

        add(chartPanel);
        chartPanel.setMaximumDrawHeight(2000); //Fonts will be distorted if greater than this value 
        chartPanel.setMaximumDrawWidth(2000);
        chartPanel.setPreferredSize(new java.awt.Dimension(PREFERRED_WIDTH, PREFERRED_HEIGHT));
        setBackground(getBackground());
        setAxisSize(axisSize);
        setupMenus();
        setMarkersVisible(App.isMarkersVisible());
        setLegendVisible(false);
        setTimeAxisLabel(null);

        systemTickMarkPaint = chart.getXYPlot().getDomainAxis().getTickMarkPaint();
        systemAxisLinePaint = chart.getXYPlot().getDomainAxis().getAxisLinePaint();
    }
    
    void updateLafColors(){
        if (chart.getTitle() != null) {
            chart.getTitle().setPaint(getAxisTextColor());
        }
        if (chart.getLegend() != null) {
            chart.getLegend().setItemPaint(getAxisTextColor());
        }

        XYPlot plot = chart.getXYPlot();        
        if (plot!=null){
            setPlotBackgroundColor(plotBackgroundColor);
            setPlotGridColor(plotGridColor);
            setAxisTextColor(axisTextColor);
        }
    }
    
    class ExtendedLineAndShapeRenderer extends XYLineAndShapeRenderer {

      public ExtendedLineAndShapeRenderer() {
          super(true, false); //lines=true, shapes=false
      }
      
      @Override
        public void drawItem(Graphics2D g2, 
                             XYItemRendererState state, 
                             Rectangle2D dataArea, 
                             PlotRenderingInfo info, 
                             XYPlot plot, 
                             ValueAxis domainAxis, 
                             ValueAxis rangeAxis, 
                             XYDataset dataset, 
                             int series, 
                             int item, 
                             CrosshairState crosshairState, 
                             int pass) {
            // Draw the usual item
            super.drawItem(g2, state, dataArea, info, plot, domainAxis, rangeAxis, dataset, series, item, crosshairState, pass);
            if (lineExtension){
                // If this is the last item in the series, extend the line to the plot's edge
                int itemCount = dataset.getItemCount(series);
                if ((item ==  itemCount- 1) && isLinePass(pass)) {
                    double x1 = dataset.getXValue(series, item);
                    double y1 = dataset.getYValue(series, item);
                    double now = System.currentTimeMillis();    //domainAxis.getUpperBound() 
                    if (now > x1){                    
                        // Transform the last data point to Java2D space
                        double xx1 = domainAxis.valueToJava2D(x1, dataArea, plot.getDomainAxisEdge());
                        double yy1 = rangeAxis.valueToJava2D(y1, dataArea, plot.getRangeAxisEdge());
                        double xx2 = domainAxis.valueToJava2D(now, dataArea, plot.getDomainAxisEdge());
                        Paint p = getSeriesPaint(series);
                        if (p instanceof Color c){
                            p = new Color(c.getRed(), c.getGreen(), c.getBlue(),  LINE_EXTENSION_TRANSPARENCY);
                        }
                        g2.setPaint(p);
                        g2.setStroke(getSeriesStroke(series)); // new BasicStroke(1f));;
                        g2.draw(new Line2D.Double(xx1, yy1, xx2, yy1));        
                    }
                }
            }
        }
        boolean lineExtension;

        public void setLineExtension(boolean value){
            if (value!=lineExtension){
                lineExtension = value;
                if (isShowing()) {
                    repaint();
                }
                
            }
        }    
    }        

    XYPlot getPlot() {
        return chart.getXYPlot();
    }

    void moverOverPlot(XYDataItem translationVector, int index) {
        double translatedDomainIntervalMin = chart.getXYPlot().getDomainAxis().getRange().getLowerBound() + translationVector.getX().doubleValue();
        double translatedDomainIntervalMax = chart.getXYPlot().getDomainAxis().getRange().getUpperBound() + translationVector.getX().doubleValue();
        double translatedRangeIntervalMin = chart.getXYPlot().getRangeAxis(index).getRange().getLowerBound() + translationVector.getY().doubleValue();
        double translatedRangeIntervalMax = chart.getXYPlot().getRangeAxis(index).getRange().getUpperBound() + translationVector.getY().doubleValue();

        Range domainAxisRange = new Range(translatedDomainIntervalMin, translatedDomainIntervalMax);
        Range rangeAxisRange = new Range(translatedRangeIntervalMin, translatedRangeIntervalMax);
        //We set notify to false in the first call..
        chart.getXYPlot().getDomainAxis().setRange(domainAxisRange, true, false);
        //...and true in the last
        chart.getXYPlot().getRangeAxis(index).setRange(rangeAxisRange, true, true);

    }

    void moveY(double offset, int index) {
        XYDataItem translationVector = new XYDataItem(0.0, offset);
        double translatedRangeIntervalMin = chart.getXYPlot().getRangeAxis(index).getRange().getLowerBound() + translationVector.getY().doubleValue();
        double translatedRangeIntervalMax = chart.getXYPlot().getRangeAxis(index).getRange().getUpperBound() + translationVector.getY().doubleValue();
        Range rangeAxisRange = new Range(translatedRangeIntervalMin, translatedRangeIntervalMax);
        chart.getXYPlot().getRangeAxis(index).setRange(rangeAxisRange, true, true);
    }

    void moveX(double offset) {
        XYDataItem translationVector = new XYDataItem(offset, 0.0);
        double translatedDomainIntervalMin = chart.getXYPlot().getDomainAxis().getRange().getLowerBound() + translationVector.getX().doubleValue();
        double translatedDomainIntervalMax = chart.getXYPlot().getDomainAxis().getRange().getUpperBound() + translationVector.getX().doubleValue();
        Range domainAxisRange = new Range(translatedDomainIntervalMin, translatedDomainIntervalMax);
        chart.getXYPlot().getDomainAxis().setRange(domainAxisRange, true, false);
    }

    void zoomOutX() {
        if (scrollMode) {
            moveX(-chart.getXYPlot().getDomainAxis().getRange().getLength() / 2);
        }
        getPlot().zoomDomainAxes(2.0, null, null);
    }

    void zoomInX() {
        getPlot().zoomDomainAxes(0.5, null, null);
        if (scrollMode) {
            moveX(chart.getXYPlot().getDomainAxis().getRange().getLength() / 2);
        }
    }

    void zoomOutY() {
        getPlot().zoomRangeAxes(2.0, null, null);
    }

    void zoomInY() {
        getPlot().zoomRangeAxes(0.5, null, null);
    }

    void panLeft() {
        try {
            //getPlot().panDomainAxes(-50.0, null, null);
            moveX(-((DateAxis) chart.getXYPlot().getDomainAxis()).getTickUnit().getSize());

        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }

    void panRight() {
        try {
            //getPlot().panDomainAxes(50.0, null, null);
            moveX(((DateAxis) chart.getXYPlot().getDomainAxis()).getTickUnit().getSize());
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }

    void panUp() {
        try {
            //plotPanel.getPlot().panRangeAxes(50.0, null, null);
            for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
                moveY(((NumberAxis) chart.getXYPlot().getRangeAxis(i)).getTickUnit().getSize(), i);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }

    void panDown() {
        try {
            //plotPanel.getPlot().panRangeAxes(-50.0, null, null);
            for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
                moveY(-((NumberAxis) chart.getXYPlot().getRangeAxis(i)).getTickUnit().getSize(), i);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }

    void setScrollMode(boolean value) {       
        if (scrollMode != value) {
            scrollMode = value;
            chart.getXYPlot().getDomainAxis().setAutoRange(value);
        }
        updateTooltips();
    }

    void resetZoom() {
        chartPanel.restoreAutoBounds();
    }

    void setAutoScale(boolean value) {
        autoScale = value;
        for (SeriesInfo s : series) {
            applyRange(s.index, s.getRange());
        }
    }

    public int getNumberOfSeries() {
        return series.size();
    }

    SeriesInfo getSeriesInfo(int index) {
        for (SeriesInfo info : series) {
            if (info.index == index) {
                return info;
            }
        }
        return null;
    }

    SeriesInfo getSeriesInfo(PlotSeries plotSeries) {
        for (SeriesInfo info : series) {
            if (info.plotSeries == plotSeries) {
                return info;
            }
        }
        return null;
    }

    TimeSeriesCollection getData(int index) {
        SeriesInfo info = getSeriesInfo(index);
        return (info == null) ? null : info.data;
    }

    TimeSeries getTimeSeries(int index) {
        SeriesInfo info = getSeriesInfo(index);
        return (info == null) ? null : info.timeSeries;
    }

    XYLineAndShapeRenderer getRenderer(int index) {

        if ((selectedRenderer != null) && (selectedIndex == index)) {
            return (XYLineAndShapeRenderer) selectedRenderer;
        }
        XYPlot plot = chart.getXYPlot();
        return (XYLineAndShapeRenderer) plot.getRenderer(index);
    }

    public void setAntiAliasing(boolean value) {
        if (chart != null) {
            chart.setAntiAlias(value);
        }
    }

    public boolean isAntiAlias() {
        if (chart != null) {
            return chart.getAntiAlias();
        }
        return false;
    }

    public void setLabelFont(Font f) {
        labelFont = f;
        XYPlot plot = chart.getXYPlot();
        plot.getDomainAxis().setLabelFont(f);
        plot.getRangeAxis().setLabelFont(f);
    }

    public void setTickLabelFont(Font f) {
        tickLabelFont = f;
        XYPlot plot = chart.getXYPlot();
        plot.getDomainAxis().setTickLabelFont(f);
        plot.getRangeAxis().setTickLabelFont(f);
    }

    public Font getLabelFont() {
        return labelFont;
    }

    public Font getTickLabelFont() {
        return tickLabelFont;
    }

    protected void setupMenus() {
        JCheckBoxMenuItem menuMarkers;
        JCheckBoxMenuItem menuLegend;
        JMenuItem menuPauseStart;
        JMenuItem menuReset;
        JMenuItem menuCopyData;
        JMenu menuSeries;

        menuMarkers = new JCheckBoxMenuItem("Show Markers");
        menuMarkers.addActionListener((ActionEvent e) -> {
            setMarkersVisible(menuMarkers.isSelected());
        });

        menuPauseStart = new JMenuItem("Pause");
        menuPauseStart.addActionListener((ActionEvent e) -> {
            if (isStarted()) {
                pause();
            } else {
                start();
            }
        });
        menuReset = new JMenuItem("Reset");
        menuReset.addActionListener((ActionEvent e) -> {
            App.getGraphPanel().reset();
        });

        menuSeries = new JMenu("Series Visibility");
        
        menuCopyData= new JMenuItem("Copy Data");
        menuCopyData.addActionListener((ActionEvent e) -> {
            copyData();
        });        

        getPopupMenu().addPopupMenuListener(new PopupMenuListener() {

            @Override
            public void popupMenuWillBecomeVisible(PopupMenuEvent e) {
                menuMarkers.setSelected(isMarkersVisible());
                menuSeries.removeAll();
                for (SeriesInfo s : series) {
                    JCheckBoxMenuItem item = new JCheckBoxMenuItem(s.plotSeries.getName(), !isSeriesHidden(s.plotSeries));
                    item.addActionListener((ActionEvent ae) -> {
                        setSeriesHidden(s.plotSeries, !item.isSelected());
                    });
                    menuSeries.add(item);
                }

                menuPauseStart.setText(isStarted() ? "Pause" : "Start");
            }

            @Override
            public void popupMenuWillBecomeInvisible(PopupMenuEvent e) {
            }

            @Override
            public void popupMenuCanceled(PopupMenuEvent e) {
            }
        });

        addPopupMenuItem(null);
        addPopupMenuItem(menuMarkers);
        addPopupMenuItem(menuPauseStart);
        addPopupMenuItem(menuReset);
        addPopupMenuItem(menuSeries);

        JMenuItem menuSaveTxt = new JMenuItem("TXT...");
        menuSaveTxt.addActionListener((ActionEvent e) -> {
            try{
                saveData();
            } catch (IOException ex) {
                    SwingUtils.showException(this, ex);
            }                
        });
        try {
            JMenu menuSave = (JMenu) getPopupMenu().getComponent(3);
            menuSave.add(menuSaveTxt);
        } catch (Exception ex) {
            Logger.getLogger(PlotPanel.class.getName()).log(Level.WARNING, null, ex);
        }
         getPopupMenu().add(menuCopyData, 3);
    }

    public void saveData()  throws IOException {
        JFileChooser chooser = new JFileChooser();
        FileNameExtensionFilter filter = new FileNameExtensionFilter("Text files", "txt");
        chooser.setFileFilter(filter);
        if (chooser.showSaveDialog(PlotPanel.this) == JFileChooser.APPROVE_OPTION) {
            String fileName = chooser.getSelectedFile().getAbsolutePath();
            if (chooser.getSelectedFile().getName().contains(".")) {
                fileName += ".txt";
            }
            saveData(fileName);
        }        
    }

    public void saveData(String filename) throws IOException {
        if (new File(filename).isFile()){
            if (SwingUtils.showOption(this, "Overwrite", "File " + filename + " already exists.\nDo you want to overwrite it?", 
                    SwingUtils.OptionType.YesNo) == SwingUtils.OptionResult.No) {
                return;
            }
        }
        try (FileWriter writer = new FileWriter(filename, false)) {
            writeData(writer, isSparse());
        } 
    }    
    
    protected String getDataAsString() {
        try (StringWriter writer = new StringWriter()) {
            writeData(writer, isSparse());           
            return writer.toString();     
        } catch (Exception ex){
            return null;
        }
    }    
        
    public void copyData() {
        try (StringWriter writer = new StringWriter()) {
            writeData(writer, isSparse());           
            SwingUtils.copyToClipboard(writer.toString());
        } catch (Exception ex) {
            Logger.getLogger(PlotPanel.class.getName()).log(Level.WARNING, null, ex);
        }
    }
    
    
    public BufferedImage copyImage(Dimension size) {
        return chart.createBufferedImage(size.width, size.height);
    }    
    
    public void saveImageAsPng(String filename, Dimension size) throws IOException {
        if (new File(filename).isFile()){
            if (SwingUtils.showOption(this, "Overwrite", "File " + filename + " already exists.\nDo you want to overwrite it?", 
                    SwingUtils.OptionType.YesNo) == SwingUtils.OptionResult.No) {
                return;
            }
        }        
        ChartUtilities.saveChartAsPNG(new File(filename), chart, size.width, size.height);
    }    

    public void save() throws IOException {
        JFileChooser chooser = new JFileChooser();
        var filterTxt = new ExtensionFileFilter("Text file (.txt)", new String[]{"txt"});
        var filterPng = new ExtensionFileFilter("Image file (.png)", new String[]{"png"});
        chooser.addChoosableFileFilter(filterTxt);
        chooser.addChoosableFileFilter(filterPng);
        chooser.setAcceptAllFileFilterUsed(false);
        chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);

        int rVal = chooser.showSaveDialog(this);
        if (rVal == JFileChooser.APPROVE_OPTION) {
            String fileName = chooser.getSelectedFile().getAbsolutePath();
            if (!chooser.getSelectedFile().getName().contains(".")) {
                fileName += "."+((ExtensionFileFilter)chooser.getFileFilter()).getExtensions()[0];
            }
            if (chooser.getFileFilter() == filterTxt){                           
                saveData(fileName);
            } else if (chooser.getFileFilter() == filterPng){
                saveImageAsPng(fileName, new Dimension(getWidth(),getHeight()));
            }            
        }
    }
    
    public static boolean isSparse() {
        return App.timeMode.isSparse();
    }


    protected void writeData(Writer writer, boolean sparse) throws IOException {
        if (isSparse()) {
            for (int i = 0; i < series.size(); i++) {
                List<ChannelValue> values = getSeriesData(i);
                writer.append("#Series: ").append(getSeriesName(i)).append(LINE_SEPARATOR);
                Double last = null;
                for (ChannelValue item : values) {
                    Double val = item.value();
                    if (val == null) {
                        val = Double.NaN;
                    }
                    if (!val.equals(last)) {
                        last = val;
                        writer.append(DATE_TIME_FORMAT.format(item.timestamp()));
                        writer.append(" ");
                        writer.append(String.valueOf(val));
                        writer.append(LINE_SEPARATOR);
                    }
                }
                writer.append(LINE_SEPARATOR);
            }
        } else {
            //Header
            int numSeries = series.size();
            writer.append("Time").append(FIELD_SEPARATOR);
            for (int i = 0; i < numSeries; i++) {
                writer.append(series.get(i).plotSeries.name)
                        .append(" [").append(series.get(i).plotSeries.units).append("]").
                        append(FIELD_SEPARATOR);
            }
            writer.append(LINE_SEPARATOR);

            List<List<ChannelValue>> values = new ArrayList<>();
            String[] format = new String[numSeries];

            for (int i = 0; i < numSeries; i++) {
                values.add(getSeriesData(i));
                format[i] = "";

            }
            int itemCount = values.size() > 0 ? values.get(0).size() : 0;

            //If series was appended , may contain less points;
            int offsets[] = new int[numSeries];
            for (int i = 0; i < numSeries; i++) {
                offsets[i] = Math.max(itemCount - values.get(i).size(), 0);
            }
            double[] last = new double[numSeries];

            for (int i = 0; i < itemCount; i++) {
                double[] record = new double[values.size()];
                for (int j = 0; j < values.size(); j++) {
                    List<ChannelValue> data = values.get(j);
                    int index = i - offsets[j];
                    record[j] = (data.size() > index) && (index > 0) ? data.get(index).displayValue() : Double.NaN;
                }
                if (!Arrays.equals(last, record)) {
                    last = record;
                    String datetime = DATE_TIME_FORMAT.format(values.get(0).get(i).timestamp());
                    writer.append(datetime);
                    writer.append(FIELD_SEPARATOR);
                    for (int j = 0; j < record.length; j++) {
                        writer.append(series.get(j).plotSeries.toString(record[j], false, false, false)).append(FIELD_SEPARATOR);
                    }
                    writer.append(LINE_SEPARATOR);
                }

            }
        }
    }

    int axisSize = -1;

    public void setAxisSize(int size) {
        axisSize = size;
        chart.getXYPlot().getRangeAxis().setFixedDimension(size);
    }

    public int getAxisSize() {
        return axisSize;
    }

    @Override
    public void setBackground(Color c) {
        super.setBackground(c);
        if (chartPanel != null) {
            chartPanel.setBackground(c);
            chartPanel.getChart().setBackgroundPaint(c);
        }
    }

    Color plotBackgroundColor = null;
    public  Color getPlotBackgroundColor() {
        if (plotBackgroundColor==null){
            return App.isDarkLookAndFeel() ? DEFAULT_DARK_BACKGROUND_COLOR : DEFAULT_BACKGROUND_COLOR;
        }
        return plotBackgroundColor;
    }    
    
    public void setPlotBackgroundColor(Color c) {
        plotBackgroundColor = c;
        chart.getXYPlot().setBackgroundPaint(getPlotBackgroundColor());
    }
    
    Color plotGridColor = null;
    public  void setPlotGridColor(Color color) {
        plotGridColor = color;
        Color c =  getPlotGridColor();
        chart.getXYPlot().setDomainGridlinePaint(c);
        chart.getXYPlot().setRangeGridlinePaint(c);
        chart.getXYPlot().setDomainMinorGridlinePaint(c);
        chart.getXYPlot().setRangeMinorGridlinePaint(c);        
        chart.getXYPlot().setOutlinePaint(c);
    }
        
    public Color getPlotGridColor() {
        if (plotGridColor==null){
            return App.isDarkLookAndFeel() ? DEFAULT_DARK_GRID_COLOR : DEFAULT_GRID_COLOR;
        }
        return plotGridColor;
    }
    
    
    Color axisTextColor = null;

    public void setAxisTextColor(Color color) {
        axisTextColor = color;
        Color c = getAxisTextColor();
        chart.getXYPlot().getDomainAxis().setTickLabelPaint(c);
        chart.getXYPlot().getDomainAxis().setLabelPaint(c);
        chart.getXYPlot().getRangeAxis().setLabelPaint(c);
        chart.getXYPlot().getRangeAxis().setTickLabelPaint(c);            
    }

    public Color getAxisTextColor() {
        if (axisTextColor == null){
            return App.isDarkLookAndFeel() ? DEFAULT_DARK_TEXT_COLOR : DEFAULT_TEXT_COLOR;
        }
        return axisTextColor;
    }
        
    
    public void setDomainAxisColor(Color c) {
        chart.getXYPlot().getDomainAxis().setLabelPaint(c == null ? getAxisTextColor() : c);
        chart.getXYPlot().getDomainAxis().setTickLabelPaint(c == null ? getAxisTextColor() : c);
        chart.getXYPlot().getDomainAxis().setTickMarkPaint(c == null ? systemTickMarkPaint : c);
        chart.getXYPlot().getDomainAxis().setAxisLinePaint(c == null ? systemAxisLinePaint : c);
    }

    public void setRangeAxisColor(Color c) {
        for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
            chart.getXYPlot().getRangeAxis(i).setLabelPaint(c == null ? getAxisTextColor() : c);
            chart.getXYPlot().getRangeAxis(i).setTickLabelPaint(c == null ? getAxisTextColor() : c);
            chart.getXYPlot().getRangeAxis(i).setTickMarkPaint(c == null ? systemTickMarkPaint : c);
            chart.getXYPlot().getRangeAxis(i).setAxisLinePaint(c == null ? systemAxisLinePaint : c);
        }
    }

    public void setRangeAxisColor(int index, Color c) {
        if (index < chart.getXYPlot().getRangeAxisCount()) {
            chart.getXYPlot().getRangeAxis(index).setLabelPaint(c == null ? getAxisTextColor() : c);
            chart.getXYPlot().getRangeAxis(index).setTickLabelPaint(c == null ? getAxisTextColor() : c);
            chart.getXYPlot().getRangeAxis(index).setTickMarkPaint(c == null ? systemTickMarkPaint : c);
            chart.getXYPlot().getRangeAxis(index).setAxisLinePaint(c == null ? systemAxisLinePaint : c);
        }
    }

    public void setGridXVisible(boolean major, boolean minor) {
        chart.getXYPlot().setDomainGridlinesVisible(major);
        chart.getXYPlot().setDomainMinorGridlinesVisible(minor);
        chart.getXYPlot().getDomainAxis().setMinorTickCount(minor ? 2 : 0);
    }

    public void setGridYVisible(boolean major, boolean minor) {
        chart.getXYPlot().setRangeGridlinesVisible(major);
        chart.getXYPlot().setRangeMinorGridlinesVisible(minor);
        for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
            chart.getXYPlot().getRangeAxis(i).setMinorTickCount(minor ? 2 : 0);
        }
    }

    public boolean isSeriesHidden(PlotSeries series) {
        return TRANSPARENT_COLOR.equals(getSeriesColor(series));
    }

    Map<PlotSeries, Color> hiddenPlots = new HashMap<>();

    public void setSeriesHidden(PlotSeries series, boolean hidden) {
        if (hidden != isSeriesHidden(series)) {
            if (hidden) {
                hiddenPlots.put(series, getSeriesColor(series));
                setSeriesColor(series, TRANSPARENT_COLOR);
            } else {
                setSeriesColor(series, hiddenPlots.get(series));
            }
        }
    }

    
    public void clear() {
        for (SeriesInfo s : series) {
            s.timeSeries.clear();
        }
        if (isShowing()) {
            repaint();
        }
    }

    public SeriesInfo addSeries(PlotSeries plotSeries, boolean enabled) {
        int index = (series.size() == 0) ? 0 : (series.get(series.size() - 1).index + 1);
        TimeSeries timeSeries = new TimeSeries(plotSeries.name);
        XYPlot plot = chart.getXYPlot();
        TimeSeriesCollection data = new TimeSeriesCollection();
        createAxis(index, plotSeries.units, plotSeries.isLogaritimic(), index == 0);
        var renderer = new ExtendedLineAndShapeRenderer();
        renderer.setLineExtension(enabled);
        renderer.setBaseShapesVisible(isMarkersVisible());
        plot.setRenderer(index, renderer);
        plot.setDataset(index, data);
        plot.mapDatasetToRangeAxis(index, index);
        setAxisSize(axisSize);
        SeriesInfo seriesInfo = new SeriesInfo(index, plotSeries, timeSeries, data);
        applyRange(index, seriesInfo.getRange());
        series.add(seriesInfo);

        checkSeriesDuration();
        data.addSeries(timeSeries);
        if (seriesInfo.plotSeries.getColor() != null) {
            seriesInfo.getRenderer().setSeriesPaint(0, seriesInfo.plotSeries.getColor());
        }
        index = getNumberOfSeries() - 1;
        setLineWidth(index, plotSeries.lineWidth);
        setNotify(index, false);
        setMaximumItemCount(index, plotSeries.numSamples);

        return seriesInfo;
    }
        
    public void setNotify(int index, boolean value) {
        if (index < series.size()) {
            series.get(index).timeSeries.setNotify(value);
        }
    }

    public void setNotify(boolean value) {
        for (SeriesInfo s : series) {
            s.timeSeries.setNotify(value);
        }
    }

    public void notifyUpdate() {
        if (isShowing()) {
            XYPlot plot = chart.getXYPlot();
            plot.configureDomainAxes();
            plot.configureRangeAxes();
            chartPanel.chartChanged(null);
        }
    }        
    
    public void setLineWidth(int width) {    
        for (int i = 0; i < getNumberOfSeries(); i++) {
            setLineWidth(i, width);            
        }
    }    
    
    void setLineWidth(int index, int width) {
        if (index < series.size()) {
            XYItemRenderer renderer = series.get(index).getRenderer();
            renderer.setSeriesStroke(0, new BasicStroke(width));
            double size = (double)Math.max(1, width+1);
            Shape marker = new Rectangle2D.Double(-size/2, -size/2, size, size);
            renderer.setSeriesShape(0, marker);
            
        }
    }

    public void setMaximumItemCount(int index, int count) {
        if (index < series.size()) {
            series.get(index).timeSeries.setMaximumItemCount(count > 0 ? count : Integer.MAX_VALUE);
        }
    }

    NumberAxis createAxis(int index, String label, boolean logarithmic, boolean visible) {
        XYPlot plot = chart.getXYPlot();
        NumberAxis axis;
        if (logarithmic) {
            axis = new LogarithmicAxis(null);
            ((LogarithmicAxis) axis).setAllowNegativesFlag(false);
            ((LogarithmicAxis) axis).setStrictValuesFlag(false);
            ((LogarithmicAxis) axis).setLog10TickLabelsFlag(true); //TODO: only used to axis Y
            ((LogarithmicAxis) axis).setAutoRangeMinimumSize(AUTO_RANGE_LOG_MINIMUM_SIZE);
            setAxisMargin(axis, 0);
        } else {
            axis = new NumberAxis();
            axis.setAutoRangeMinimumSize(AUTO_RANGE_MINIMUM_SIZE);
            setAxisMargin(axis, AXIS_MARGIN);
        }
        axis.setAutoRangeIncludesZero(false);

        axis.setLabelFont(labelFont);
        axis.setLabel(label);
        axis.setLabelInsets(RectangleInsets.ZERO_INSETS);
        axis.setTickLabelFont(tickLabelFont);
        axis.setLabelPaint(getAxisTextColor());
        axis.setTickLabelPaint(getAxisTextColor());
        plot.setRangeAxis(index, axis);
        plot.setRangeAxisLocation(index, AxisLocation.BOTTOM_OR_LEFT);
        axis.setVisible(visible);
        return axis;
    }

    void applyRange(int index, Range range) {
        if ((range == null) || autoScale) {
            setAxisAutoScale(index);
        } else {
            try {
                setAxisScale(index, range);
            } catch (IllegalArgumentException ex) {
                Logger.getLogger(PlotPanel.class.getName()).warning(() -> "Invalid range for series " + index + ": setting auto-scale");
                setAxisAutoScale(index);
            }
        }
    }

    protected void removeAllSeries() {
        for (SeriesInfo s : series) {
            s.data.removeAllSeries();
        }
        series.clear();
        selectedRenderer = null;
        selectedIndex = -1;
    }

    protected void removeSeries(PlotSeries plotSeries) {
        SeriesInfo removed = getSeriesInfo(plotSeries);
        if (removed != null) {
            removed.data.removeAllSeries();
            series.remove(removed);
            if (series.size() == 0) {
                selectedRenderer = null;
                selectedIndex = -1;
            }
        }
    }

    protected void removeSeries(int index) {
        if ((index > 0) && (index < series.size())) {
            SeriesInfo removed = series.get(index);
            removed.data.removeAllSeries();
            series.remove(index);
            if (series.size() == 0) {
                selectedRenderer = null;
                selectedIndex = -1;
            }
        }
    }

    public void updateSeries(PlotSeries series) {
        if (isShowing()) {
            repaint();
        }
    }

    protected void onRemovedAllSeries() {
        for (SeriesInfo s : series) {
            s.data.removeAllSeries();
        }
        series.clear();
    }

    protected void addDataPoint(int index, long time, double value) {
        getTimeSeries(index).addOrUpdate(new FixedMillisecond(new Date(time)), (Double.isNaN(value)) ? null : value);
    }

    public int getItemCount(int index) {
        TimeSeries s = getTimeSeries(index);
        return s.getItemCount();
    }

    public List<ChannelValue> getSeriesData(int index) {
        TimeSeries s = getTimeSeries(index);
        List<ChannelValue> ret = new ArrayList<>();
        Number last = null;
        for (TimeSeriesDataItem item : (List<TimeSeriesDataItem>) s.getItems()) {
            Double val;
            if (item.getValue() != null) {
                val = item.getValue().doubleValue();
            } else {
                val = Double.NaN;
            }
            if (!isSparse() || !val.equals(last)) {
                last = val;
                ret.add(new ChannelValue(val, item.getPeriod().getMiddleMillisecond(), false));
            }
        }
        return ret;
    }

    public ChannelValue getItem(int index, int itemIndex) {
        TimeSeries s = getTimeSeries(index);
        if (itemIndex == -1) {
            itemIndex = s.getItemCount() - 1;
        }
        if ((itemIndex < 0) || (itemIndex >= s.getItemCount())) {
            return null;
        }
        TimeSeriesDataItem item = s.getDataItem(itemIndex);
        return new ChannelValue((item.getValue() != null) ? item.getValue().doubleValue() : Double.NaN, item.getPeriod().getMiddleMillisecond(), false);
    }

    public String getSeriesName(int index) {
        TimeSeries s = getTimeSeries(index);
        return String.valueOf(s.getKey());
    }

    public void addPopupMenuItem(final JMenuItem item) {
        if (chartPanel != null) {
            if (item == null) {
                chartPanel.getPopupMenu().addSeparator();
            } else {
                chartPanel.getPopupMenu().add(item);
            }
        }
    }

    protected JPopupMenu getPopupMenu() {        
        return chartPanel.getPopupMenu();
    }

    public int getDurationMillis() {
        final XYPlot plot = chart.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        return (int) axis.getFixedAutoRange();
    }

    public void setDurationMillis(int duration) {
        final XYPlot plot = chart.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        axis.setAutoRange(true);
        axis.setFixedAutoRange(duration);
        checkSeriesDuration();

    }

    public boolean isLogarithmic(int index) {
        ValueAxis axis = chart.getXYPlot().getRangeAxis(index);
        if (axis != null) {
            return axis instanceof LogarithmicAxis;
        }
        return false;
    }

    public void setLogarithmic(int index, boolean value) {
        if (value != isLogarithmic(index)) {
            ValueAxis cur = chart.getXYPlot().getRangeAxis(index);
            if (cur != null) {
                boolean visible = cur.isVisible();
                Paint labelPaint = cur.getLabelPaint();            
                createAxis(index, cur.getLabel(), value, visible);
                applyRange(index, cur.isAutoRange() ? null : cur.getRange());                
                if (labelPaint instanceof Color c){
                    setRangeAxisColor(index,c); 
                }
            }
        }
    }

    XYItemRenderer selectedRenderer;
    int selectedIndex = -1;

    public void setActiveSeries(int index) {
        SeriesInfo selected = null;
        for (SeriesInfo s : series) {
            s.getAxis().setVisible(index == s.index);
            if (index == s.index) {
                selected = s;
            }
        }
        if ((selected != null) && bringSelectedToTheTop) {
            //Cannot customize Z order so repeat the selected in the end so that it will be always visible
            int aux_index = series.size();
            XYPlot plot = chart.getXYPlot();
            if (selectedRenderer != null) {
                plot.setRenderer(selectedIndex, selectedRenderer);
                plot.setRenderer(aux_index, null);
                plot.setDataset(aux_index, null);
                selectedRenderer = null;
            }
            if (index == 0) {
                chart.getXYPlot().setDatasetRenderingOrder(DatasetRenderingOrder.REVERSE);
            } else {
                chart.getXYPlot().setDatasetRenderingOrder(DatasetRenderingOrder.FORWARD);
                selectedRenderer = (XYLineAndShapeRenderer) plot.getRenderer(index);
                plot.setRenderer(aux_index, selectedRenderer);
                plot.setDataset(aux_index, selected.data);
                plot.mapDatasetToRangeAxis(aux_index, index);
                plot.setRenderer(index, null);
                selectedRenderer.setSeriesPaint(0, selected.plotSeries.getColor());
            }
        }
        //Update the scale
        ValueAxis cur = chart.getXYPlot().getRangeAxis(index);
        if (cur != null) {
            boolean visible = cur.isVisible();
            createAxis(index, cur.getLabel(), isLogarithmic(index), visible);
            applyRange(index, cur.isAutoRange() ? null : cur.getRange());
        }        
        selectedIndex = index;
    }

    public SeriesInfo getActiveSeries() {
        for (SeriesInfo s : series) {
            if (s.getAxis().isVisible()) {
                return s;
            }
        }
        return null;
    }

    public PlotSeries getPlotSeries(int index) {
        if (index < series.size()) {
            return series.get(index).plotSeries;
        }
        return null;
    }

    public int getActiveSeriesIndex() {
        return selectedIndex;
    }

    void checkSeriesDuration() {
        // Time-to-live based only in number of samples, not in time.
        /*
        int duration = getDurationMillis();
        if (isStarted() && (duration > 0)) {
            for (SeriesInfo s : series) {
                s.timeSeries.setMaximumItemAge(duration + 10000); //Can connect to last removed point, but only up to 10s 
            }
        } else {
            //Preserve points if paused
            for (SeriesInfo s : series) {
                s.timeSeries.setMaximumItemAge(Long.MAX_VALUE);
            }
        }
         */
    }

    public void setAxisAutoScale(int index) {
        ValueAxis axis = chart.getXYPlot().getRangeAxis(index);
        if (axis != null) {
            axis.setAutoRange(true);
        }
    }

    public void setAxisScale(int index, double min, double max) {
        Range range = new Range(min, max);
        setAxisScale(index, range);
    }

    public void setAxisScale(int index, Range range) {
        ValueAxis axis = chart.getXYPlot().getRangeAxis(index);
        if (axis != null) {
            axis.setRangeWithMargins(range, true, true);
        }
    }

    public void setAxisMargin(int index, double margin) {
        ValueAxis axis = chart.getXYPlot().getRangeAxis(index);
        setAxisMargin(axis, margin);
    }

    void setAxisMargin(ValueAxis axis, double margin) {
        axis.setLowerMargin(margin);
        axis.setUpperMargin(margin);
    }

    public void setTimeAxisLabel(String label) {
        final XYPlot plot = chart.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        axis.setLabel(label);
    }

    public String getTimeAxisLabel() {
        final XYPlot plot = chart.getXYPlot();
        ValueAxis axis = plot.getDomainAxis();
        return axis.getLabel();
    }

    protected Color getSeriesColor(PlotSeries plotSeries) {
        XYItemRenderer renderer = getSeriesInfo(plotSeries).getRenderer();
        if (renderer == null) {
            return null;
        }
        Paint p = renderer.getSeriesPaint(0);
        return p instanceof Color color ? color : null;
    }

    protected void setSeriesColor(PlotSeries plotSeries, Color color) {
        XYLineAndShapeRenderer renderer = getSeriesInfo(plotSeries).getRenderer();
        renderer.setSeriesPaint(0, color);
    }

    public Range getXAxisRange() {
        Range r = chart.getXYPlot().getDomainAxis().getRange();
        return new Range(r.getLowerBound(), r.getUpperBound());
    }

    public Range getYAxisRange() {
        Range r = chart.getXYPlot().getRangeAxis().getRange();
        return new Range(r.getLowerBound(), r.getUpperBound());
    }

    TimeSeries getTimeSeries(PlotSeries plotSeries) {
        for (SeriesInfo s : series) {
            if (s.plotSeries == plotSeries) {
                return s.timeSeries;
            }
        }
        return null;
    }

    public BufferedImage getSnapshot(Dimension size) {
        if (size == null) {
            size = new Dimension(PREFERRED_WIDTH, PREFERRED_HEIGHT);
        }
        return chart.createBufferedImage(size.width, size.height);
    }

    public JFreeChart getChart() {
        return chart;
    }

    //State
    public void start() {
        started = true;
        selectedRenderer = null;
        checkSeriesDuration();
    }

    public void pause() {
        addTerminators();
        started = false;
        selectedRenderer = null;
        checkSeriesDuration();
    }

    public boolean isStarted() {
        if (isClosed()) {
            return false;
        }
        return started;
    }

    boolean isClosed() {
        return closed;
    }

    public void close() {
        if (closed == false) {
            closed = true;
            clear();
        }
    }
    //Appending elements

    public void addTerminator(int index) {
        Long timestamp = getLastTimestamp(index);
        addTerminator(index, (timestamp == null) ? System.currentTimeMillis() : timestamp + 1);
    }

    public void addTerminators() {
        for (int i = 0; i < getNumberOfSeries(); i++) {
            addTerminator(i);
        }
    }

    public void addTerminator(int index, long time) {
        add(index, time, Double.NaN);
    }

    public void addTerminators(long time) {
        double[] terminators = new double[getNumberOfSeries()];
        Arrays.fill(terminators, Double.NaN);
        add(time, terminators);
    }

    public void add(double value) {
        add(0, value);
    }

    public void add(int index, double value) {
        add(index, System.currentTimeMillis(), value);
    }

    public void add(int index, long time, double value) {
        if (isStarted()) {
            addDataPoint(index, time, value);
        }
    }

    public void add(double[] values) {
        add(System.currentTimeMillis(), values);
    }

    public void add(long time, double[] values) {
        if (isStarted()) {
            if ((values != null) && (getNumberOfSeries() == values.length)) {
                if (scrollMode) {
                    if (!chart.getXYPlot().getDomainAxis().isAutoRange()) {
                        ChannelValue last = getItem(0, -1);
                        if (last != null) {
                            moveX(time - last.timestamp());
                        }
                    }
                }
                for (int i = 0; i < values.length; i++) {
                    addDataPoint(i, time, values[i]);
                }
            }
        }
    }

    //////////////
    public ChannelValue getLastItem(int index) {
        return getItem(index, -1);
    }

    public Double getLastValue(int index) {
        ChannelValue ret = getLastItem(index);
        return (ret == null) ? null : ret.value();
    }

    public Long getLastTimestamp(int index) {
        ChannelValue ret = getLastItem(index);
        return (ret == null) ? null : ret.timestamp();
    }

    //Configuration
    public void setMarkersVisible(boolean visible) {
        markersVisible = visible;
        for (int i = 0; i < getNumberOfSeries(); i++) {
            getRenderer(i).setBaseShapesVisible(visible);
        }
        updateTooltips();
        if (isShowing()) {
            repaint();
        }
    }

    void updateTooltips() {
        boolean tooltips = markersVisible && !scrollMode;
        for (int i = 0; i < getNumberOfSeries(); i++) {
            if (tooltips) {
                getRenderer(i).setBaseToolTipGenerator(new StandardXYToolTipGenerator("{0} {1} {2}", TIME_FORMAT, DECIMAL_FORMAT));
                chartPanel.setDisplayToolTips(true);
                chartPanel.getChartRenderingInfo().setEntityCollection(new StandardEntityCollection());
            } else {
                chartPanel.getChartRenderingInfo().setEntityCollection(null);
                chartPanel.setDisplayToolTips(false);
                getRenderer(i).setBaseToolTipGenerator(null);
            }
        }

    }

    public boolean isMarkersVisible() {
        return markersVisible;
    }

    public void setLegendVisible(boolean visible) {
        legendVisible = visible;
        if (chart.getLegend() != null) {
            chart.getLegend().setVisible(visible);
        }
        if (isShowing()) {
            repaint();
        }
    }

    public boolean isLegendVisible() {
        return legendVisible;
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 414, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 300, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
}
