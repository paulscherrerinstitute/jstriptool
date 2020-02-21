package ch.psi.jstriptool;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.Paint;
import java.awt.Shape;
import java.awt.event.ActionEvent;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
import org.jfree.chart.JFreeChart;
import org.jfree.chart.axis.AxisLocation;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.LogarithmicAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.axis.ValueAxis;
import org.jfree.chart.entity.StandardEntityCollection;
import org.jfree.chart.labels.StandardXYToolTipGenerator;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.data.Range;
import org.jfree.data.time.FixedMillisecond;
import org.jfree.data.time.TimeSeries;
import org.jfree.data.time.TimeSeriesCollection;
import org.jfree.data.time.TimeSeriesDataItem;
import org.jfree.data.xy.XYDataItem;
import org.jfree.ui.RectangleInsets;

public class PlotPanel extends javax.swing.JPanel {

    static final Logger logger = Logger.getLogger(PlotPanel.class.getName());
    static final double AUTO_RANGE_MINIMUM_SIZE = 1e-12; //TODO: if smaller there are plot repainting problem in scans in JFreeChart > 1.0.18
    static final double AUTO_RANGE_LOG_MINIMUM_SIZE = 1e-32;
    static final int PREFERRED_WIDTH = 600;
    static final int PREFERRED_HEIGHT = 400;
    protected static final String LINE_SEPARATOR = System.lineSeparator();
    protected static final String FIELD_SEPARATOR = "\t";
    static final double AXIS_MARGIN = 0.005;

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
        
        Range getRange(){
            return plotSeries.isFixedRange() ? new Range(plotSeries.getRangeMin(), plotSeries.getRangeMax()) : null;
        }
    }
    final List<SeriesInfo> series;

    final Shape marker;
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

    private final Paint systemBackgroundPaint;
    private final Paint systemGridlinePaint;
    private final Paint systemvOutlinePaint;
    private final Paint systemLabelPaint;
    private final Paint systemTickLabelPaint;
    private final Paint systemTickMarkPaint;
    private final Paint systemAxisLinePaint;

    public PlotPanel() {
        initComponents();

        series = new ArrayList<>();
        marker = new Rectangle2D.Double(-1, -1, 2, 2);
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
        plot.setBackgroundPaint(getPlotBackground());
        plot.setDomainGridlinePaint(getGridColor());
        plot.setRangeGridlinePaint(getGridColor());
        plot.setOutlinePaint(getOutlineColor());
        if (chart.getTitle() != null) {
            chart.getTitle().setPaint(getAxisTextColor());
        }
        if (chart.getLegend() != null) {
            chart.getLegend().setItemPaint(getAxisTextColor());
        }
        plot.getDomainAxis().setTickLabelPaint(getAxisTextColor());
        plot.getDomainAxis().setLabelPaint(getAxisTextColor());
        plot.getRangeAxis().setLabelPaint(getAxisTextColor());
        plot.getRangeAxis().setTickLabelPaint(getAxisTextColor());
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
        chartPanel = new ChartPanel(chart) {
            @Override
            public void restoreAutoRangeBounds() {
                super.restoreAutoRangeBounds();
                for (int i = 0; i < getNumberOfSeries(); i++) {
                    ValueAxis axis = plot.getRangeAxis(i);
                    Range range = getSeriesInfo(i).getRange();
                    if (range != null) {
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
        setMarkersVisible(false);
        setLegendVisible(false);
        setTimeAxisLabel(null);

        systemBackgroundPaint = chart.getXYPlot().getBackgroundPaint();
        systemGridlinePaint = chart.getXYPlot().getDomainGridlinePaint();
        systemvOutlinePaint = chart.getXYPlot().getOutlinePaint();
        systemLabelPaint = chart.getXYPlot().getDomainAxis().getLabelPaint();
        systemTickLabelPaint = chart.getXYPlot().getDomainAxis().getTickLabelPaint();
        systemTickMarkPaint = chart.getXYPlot().getDomainAxis().getTickMarkPaint();
        systemAxisLinePaint = chart.getXYPlot().getDomainAxis().getAxisLinePaint();

    }
    
    XYPlot getPlot(){
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
        if (scrollMode){
            moveX(-chart.getXYPlot().getDomainAxis().getRange().getLength()/2);
        }        
        getPlot().zoomDomainAxes(2.0, null, null);
    }                                              

    void zoomInX() {                                              
        getPlot().zoomDomainAxes(0.5, null, null);
        if (scrollMode){
            moveX(chart.getXYPlot().getDomainAxis().getRange().getLength()/2);
        }
    }                                             

    void zoomOutY() {                                               
        getPlot().zoomRangeAxes(2.0, null, null);
    }                                              

    void zoomInY() {                                              
        getPlot().zoomRangeAxes(0.5, null, null);
    }       

    void panLeft(){                                       
        try{
            //getPlot().panDomainAxes(-50.0, null, null);
            moveX(-((DateAxis) chart.getXYPlot().getDomainAxis()).getTickUnit().getSize());

        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }                                             

    void panRight(){                                                   
        try{
            //getPlot().panDomainAxes(50.0, null, null);
           moveX(((DateAxis) chart.getXYPlot().getDomainAxis()).getTickUnit().getSize());
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }                                              

    void panUp(){                                                  
        try{
            //plotPanel.getPlot().panRangeAxes(50.0, null, null);
            for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
                moveY(((NumberAxis) chart.getXYPlot().getRangeAxis(i)).getTickUnit().getSize(),i);
            }
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }                                           

    void panDown(){                                              
        try{
            //plotPanel.getPlot().panRangeAxes(-50.0, null, null);
            for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
                moveY(-((NumberAxis) chart.getXYPlot().getRangeAxis(i)).getTickUnit().getSize(),i);
            }
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }   
    
    void setScrollMode(boolean value){
        scrollMode = value;
        if (!value){
            if (chart.getXYPlot().getDomainAxis().isAutoRange()){
                chart.getXYPlot().getDomainAxis().setAutoRange(value);
                
            }
        }
        updateTooltips();
    }
    
    void resetZoom(){
        chartPanel.restoreAutoBounds();
    }
    
    void setAutoScale(boolean value){
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

    public void setAntiAlias(boolean value) {
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
        JMenuItem menuStopStart;
        JMenuItem menuReset;
        JMenu menuSeries;

        menuMarkers = new JCheckBoxMenuItem("Show Markers");
        menuMarkers.addActionListener((ActionEvent e) -> {
            setMarkersVisible(menuMarkers.isSelected());
        });

        menuStopStart = new JMenuItem("Stop");
        menuStopStart.addActionListener((ActionEvent e) -> {
            if (isStarted()) {
                stop();
            } else {
                start();
            }
        });
        menuReset = new JMenuItem("Reset");
        menuReset.addActionListener((ActionEvent e) -> {
            clear();
        });

        menuSeries = new JMenu("Series Visibility");

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

                menuStopStart.setText(isStarted() ? "Stop" : "Start");
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
        addPopupMenuItem(menuStopStart);
        addPopupMenuItem(menuReset);
        addPopupMenuItem(menuSeries);

        JMenuItem menuSaveTxt = new JMenuItem("TXT...");
        menuSaveTxt.addActionListener((ActionEvent e) -> {
            JFileChooser chooser = new JFileChooser();
            FileNameExtensionFilter filter = new FileNameExtensionFilter("Text files", "txt");
            chooser.setFileFilter(filter);
            if (chooser.showSaveDialog(PlotPanel.this) == JFileChooser.APPROVE_OPTION) {
                String fileName = chooser.getSelectedFile().getAbsolutePath();
                if (!fileName.contains(".")) {
                    fileName += ".txt";
                }
                try {
                    saveData(fileName);
                } catch (IOException ex) {
                    SwingUtils.showException(PlotPanel.this, ex);
                }
            }

        });
        try {
            JMenu menuSave = (JMenu) getPopupMenu().getComponent(3);
            menuSave.add(menuSaveTxt);
        } catch (Exception ex) {
            Logger.getLogger(PlotPanel.class.getName()).log(Level.INFO, null, ex);
        }
    }

    public void saveData(String filename) throws IOException {
        saveData(filename, isSparse());
    }

    public void saveData(String filename, boolean sparse) throws IOException {
        String data = getDataAsString();
        if (data != null) {
            Files.write(Paths.get(filename), data.getBytes());
        }
    }

    boolean sparse = true;

    public boolean isSparse() {
        return sparse;
    }

    public void setSparse(boolean value) {
        sparse = value;
    }

    protected String getDataAsString() {
        StringBuilder str = new StringBuilder(1024);

        if (isSparse()) {
            for (int i = 0; i < series.size(); i++) {
                List<TimestampedValue<Double>> values = getSeriesData(i);
                str.append("#Series: ").append(getSeriesName(i)).append(LINE_SEPARATOR);
                Double last = null;
                for (TimestampedValue<Double> item : values) {
                    Double val = item.getValue();
                    if (val == null) {
                        val = Double.NaN;
                    }
                    if (!val.equals(last)) {
                        last = val;
                        str.append(new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSS ").format(item.getTimestamp()));
                        str.append(String.valueOf(val));
                        str.append(LINE_SEPARATOR);
                    }
                }
                str.append(LINE_SEPARATOR);
            }
        } else {
            //Header
            int numSeries = series.size();
            str.append("Time").append(FIELD_SEPARATOR);
            for (int i = 0; i < numSeries; i++) {
                str.append(series.get(i).plotSeries.name)
                        .append(" [").append(series.get(i).plotSeries.units).append("]").
                        append(FIELD_SEPARATOR);
            }
            str.append(LINE_SEPARATOR);

            List<List<TimestampedValue<Double>>> values = new ArrayList<>();
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
                    List<TimestampedValue<Double>> data = values.get(j);
                    int index = i - offsets[j];
                    record[j] = (data.size() > index) && (index > 0) ? data.get(index).value : Double.NaN;
                }
                if (!Arrays.equals(last, record)) {
                    last = record;
                    str.append(new SimpleDateFormat("dd/MM/YY HH:mm:ss.SSS").format(values.get(0).get(i).getTimestamp()));
                    str.append(FIELD_SEPARATOR);
                    for (int j = 0; j < record.length; j++) {
                        str.append(series.get(j).plotSeries.toString(record[j], false, false, false)).append(FIELD_SEPARATOR);
                    }
                    str.append(LINE_SEPARATOR);
                }

            }
        }
        return str.toString();
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

    public void setPlotBackgroundColor(Color c) {
        chart.getXYPlot().setBackgroundPaint(c == null ? systemBackgroundPaint : c);
    }

    public void setPlotGridColor(Color c) {
        chart.getXYPlot().setDomainGridlinePaint(c == null ? systemGridlinePaint : c);
        chart.getXYPlot().setRangeGridlinePaint(c == null ? systemGridlinePaint : c);
        chart.getXYPlot().setDomainMinorGridlinePaint(c == null ? systemGridlinePaint : c);
        chart.getXYPlot().setRangeMinorGridlinePaint(c == null ? systemGridlinePaint : c);
    }

    public void setPlotOutlineColor(Color c) {
        chart.getXYPlot().setOutlinePaint(c == null ? systemvOutlinePaint : c);
    }

    public void setDomainAxisColor(Color c) {
        chart.getXYPlot().getDomainAxis().setLabelPaint(c == null ? systemLabelPaint : c);
        chart.getXYPlot().getDomainAxis().setTickLabelPaint(c == null ? systemTickLabelPaint : c);
        chart.getXYPlot().getDomainAxis().setTickMarkPaint(c == null ? systemTickMarkPaint : c);
        chart.getXYPlot().getDomainAxis().setAxisLinePaint(c == null ? systemAxisLinePaint : c);
    }

    public void setRangeAxisColor(Color c) {
        for (int i = 0; i < chart.getXYPlot().getRangeAxisCount(); i++) {
            chart.getXYPlot().getRangeAxis(i).setLabelPaint(c == null ? systemLabelPaint : c);
            chart.getXYPlot().getRangeAxis(i).setTickLabelPaint(c == null ? systemTickLabelPaint : c);
            chart.getXYPlot().getRangeAxis(i).setTickMarkPaint(c == null ? systemTickMarkPaint : c);
            chart.getXYPlot().getRangeAxis(i).setAxisLinePaint(c == null ? systemAxisLinePaint : c);
        }
    }

    public void setRangeAxisColor(int index, Color c) {
        if (index < chart.getXYPlot().getRangeAxisCount()) {
            chart.getXYPlot().getRangeAxis(index).setLabelPaint(c == null ? systemLabelPaint : c);
            chart.getXYPlot().getRangeAxis(index).setTickLabelPaint(c == null ? systemTickLabelPaint : c);
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
        return getSeriesColor(series).equals(TRANSPARENT);
    }

    Color TRANSPARENT = new Color(1, 0, 0, 0);
    Map<PlotSeries, Color> hiddenPlots = new HashMap<>();

    public void setSeriesHidden(PlotSeries series, boolean hidden) {
        if (hidden != isSeriesHidden(series)) {
            if (hidden) {
                hiddenPlots.put(series, getSeriesColor(series));
                setSeriesColor(series, TRANSPARENT);
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

    public SeriesInfo addSeries(PlotSeries plotSeries) {
        int index = (series.size() == 0) ? 0 : (series.get(series.size() - 1).index + 1);
        TimeSeries timeSeries = new TimeSeries(plotSeries.name);
        XYPlot plot = chart.getXYPlot();
        TimeSeriesCollection data = new TimeSeriesCollection();
        createAxis(index, plotSeries.units, plotSeries.isLogaritimic(), index == 0);
        XYLineAndShapeRenderer renderer = new XYLineAndShapeRenderer();
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
        seriesInfo.getRenderer().setSeriesShape(0, marker);
        if (seriesInfo.plotSeries.getColor() != null) {
            seriesInfo.getRenderer().setSeriesPaint(0, seriesInfo.plotSeries.getColor());
        }
        setLineWidth(getNumberOfSeries() - 1, plotSeries.lineWidth);
        setNotify(getNumberOfSeries() - 1, false);        
        setMaximumItemCount(getNumberOfSeries() - 1, plotSeries.numSamples);
        
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

    public void setLineWidth(int index, int width) {
        if (index < series.size()) {
            series.get(index).getRenderer().setSeriesStroke(0, new BasicStroke(width));
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
            ((LogarithmicAxis)axis).setAllowNegativesFlag(false);
            ((LogarithmicAxis)axis).setStrictValuesFlag(false);
            ((LogarithmicAxis)axis).setLog10TickLabelsFlag(true); //TODO: only used to axis Y
            ((LogarithmicAxis)axis).setAutoRangeMinimumSize(AUTO_RANGE_LOG_MINIMUM_SIZE);
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
            try{
                setAxisScale(index, range);
            } catch (IllegalArgumentException ex){
                logger.warning("Invalid range for series " + index + ": setting auto-scale");
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

    public List<TimestampedValue<Double>> getSeriesData(int index) {
        TimeSeries s = getTimeSeries(index);
        List<TimestampedValue<Double>> ret = new ArrayList<>();
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
                ret.add(new TimestampedValue<Double>(val, item.getPeriod().getMiddleMillisecond()));
            }
        }
        return ret;
    }

    public TimestampedValue<Double> getItem(int index, int itemIndex) {
        TimeSeries s = getTimeSeries(index);
        if (itemIndex == -1) {
            itemIndex = s.getItemCount() - 1;
        }
        if ((itemIndex < 0) || (itemIndex >= s.getItemCount())) {
            return null;
        }
        TimeSeriesDataItem item = s.getDataItem(itemIndex);
        return new TimestampedValue<Double>((item.getValue() != null) ? item.getValue().doubleValue() : Double.NaN, item.getPeriod().getMiddleMillisecond());
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
        ValueAxis cur = chart.getXYPlot().getRangeAxis(index);
        if (cur != null) {
            boolean visible = cur.isVisible();
            if (value != isLogarithmic(index)) {
                createAxis(index, cur.getLabel(), value, visible);
                applyRange(index, cur.isAutoRange() ? null : cur.getRange());
            }
        }
    }
    
    void updateLog(int index){
        ValueAxis cur = chart.getXYPlot().getRangeAxis(index);
        if (cur != null) {
            boolean visible = cur.isVisible();
            createAxis(index, cur.getLabel(), isLogarithmic(index), visible);
            applyRange(index, cur.isAutoRange() ? null : cur.getRange());
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
        updateLog(index);
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
        if (axis!=null){
            axis.setAutoRange(true);
        }
    }

    public void setAxisScale(int index, double min, double max) {
        Range range = new Range(min, max);
        setAxisScale(index, range);
    }

    public void setAxisScale(int index, Range range) {
        ValueAxis axis = chart.getXYPlot().getRangeAxis(index);
        if (axis!=null){
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
        return p instanceof Color ? (Color) p : null;
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

    public void stop() {
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
                if (scrollMode){
                    if (!chart.getXYPlot().getDomainAxis().isAutoRange()){
                        TimestampedValue<Double> last = getItem(0, -1);
                        if (last!=null){
                            moveX(time-last.getTimestamp());
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
    public TimestampedValue<Double> getLastItem(int index) {
        return getItem(index, -1);
    }

    public Double getLastValue(int index) {
        TimestampedValue<Double> ret = getLastItem(index);
        return (ret == null) ? null : ret.getValue();
    }

    public Long getLastTimestamp(int index) {
        TimestampedValue<Double> ret = getLastItem(index);
        return (ret == null) ? null : ret.getTimestamp();
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
    
    void updateTooltips(){
        boolean tooltips = markersVisible && !scrollMode;
        for (int i = 0; i < getNumberOfSeries(); i++) {
            if (tooltips){
                DecimalFormat vf = new DecimalFormat("0.##########");
                SimpleDateFormat tf = new SimpleDateFormat("HH:mm:ss.S");
                getRenderer(i).setBaseToolTipGenerator(new StandardXYToolTipGenerator("{0} {1} {2}", tf, vf));
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

    static Color plotBackground;  //Default is null: transparent

    public static void setPlotBackground(Color color) {
        plotBackground = color;
    }

    public static Color getPlotBackground() {
        return plotBackground;
    }

    static final Color DEFAULT_GRID_COLOR = App.isDark() ? new Color(187, 187, 187) : Color.LIGHT_GRAY;
    static Color gridColor = DEFAULT_GRID_COLOR;

    public static void setGridColor(Color color) {
        gridColor = (color == null) ? DEFAULT_GRID_COLOR : color;
    }

    public static Color getGridColor() {

        return gridColor;
    }

    static final Color DEFAULT_OUTLINE_COLOR = Color.GRAY;
    static Color outlineColor = DEFAULT_OUTLINE_COLOR;

    public static void setOutlineColor(Color color) {
        outlineColor = (color == null) ? DEFAULT_OUTLINE_COLOR : color;
    }

    public static Color getOutlineColor() {
        return outlineColor;
    }

    static Color axisTextColor = App.isDark() ? new Color(187, 187, 187) : Color.DARK_GRAY;

    public static void setAxisTextColor(Color color) {
        axisTextColor = color;
    }

    public static Color getAxisTextColor() {
        return axisTextColor;
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
