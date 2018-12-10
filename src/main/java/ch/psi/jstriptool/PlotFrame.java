package ch.psi.jstriptool;

import ch.psi.jstriptool.Config.Curve;
import ch.psi.jstriptool.Config.DeepColor;
import ch.psi.jstriptool.Config.GridVisibility;
import ch.psi.jstriptool.Config.LabelColorAxisY;
import ch.psi.jstriptool.Config.Scale;
import java.awt.Color;
import java.awt.Component;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.swing.JMenuItem;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import org.epics.ca.Channel;
import org.epics.ca.Context;
import org.epics.ca.Monitor;
import org.epics.ca.data.Timestamped;

/**
 *
 */
public class PlotFrame extends javax.swing.JFrame {

    static PlotFrame instance;
    final boolean blocking;

    /**
     * Creates new form MainFrame
     */
    public PlotFrame() {
        javax.swing.JPopupMenu.setDefaultLightWeightPopupEnabled(false);
        initComponents();
        instance = this;
        plotPanel.setDurationMillis(60000);
        plotPanel.addPopupMenuItem(null);
        JMenuItem menuShowConfig = new JMenuItem("Show Config Window");
        menuShowConfig.addActionListener((ActionEvent e) -> {
            App.configFrame.setVisible(true);
        });
        plotPanel.addPopupMenuItem(menuShowConfig);
        plotPanel.setSparse(false);//Values are appended together for all series

        blocking = App.isBlocking();
    }

    PlotPanel getPlot() {
        return plotPanel;
    }

    void addSeries(Curve c, Color color, int index) {
        numberOfSeries++;
        PlotSeries series = PlotSeries.create(c, color);
        series.setActive();
        plotPanel.addSeries(series);
        plotPanel.setLineWidth(plotPanel.getNumberOfSeries() - 1, config.graphLineWidth);
        plotPanel.setNotify(plotPanel.getNumberOfSeries() - 1, false);
        plotPanel.setMaximumItemCount(plotPanel.getNumberOfSeries() - 1, config.numSamples);
        panelSeriesSet.add(new SeriesPanel(series));
        panelSeriesSet.updateUI();
        repaint();

        if (!App.isSimulated()) {
            Channel<Double> channel = context.createChannel(c.name, Double.class);
            cache.put(channel, toTimestamped(Double.NaN, null));
            //channel.connect();
            channel.connectAsync().handle((ret, ex) -> {
                System.out.println("Connected to channel: " + c.name);
                if (!blocking) {
                    Monitor<Timestamped<Double>> monitor = channel.addMonitor(Timestamped.class, timestampedValue -> {
                        if (timestampedValue != null) {
                            if (c.plotStatus) {
                                cache.put(channel, timestampedValue);
                                if (config.sampleInterval <= 0) {
                                    addValue();
                                }
                            }
                        }
                    });
                }
                return ret;
            });
            synchronized (channels) {
                channels.add(channel);
            }
        }
    }

    public void removeSeries(int index) {
        if (plotPanel.getActiveSeriesIndex() == index) {
            setActive(plotPanel.getPlotSeries((index == 0) ? 1 : 0));
        }
        numberOfSeries--;
        plotPanel.removeSeries(index);
        panelSeriesSet.remove(index);
        panelSeriesSet.updateUI();
        repaint();
        if (!App.isSimulated()) {
            Channel channel = null;
            synchronized (channels) {
                channel = channels.get(index);
                channels.remove(index);
            }
            if (channel != null) {
                System.out.println("Disconnected channel: " + channel.getName());
                channel.close();
            }
        }
    }

    SeriesPanel[] getSeriesPanels() {
        return Arrays.asList(panelSeriesSet.getComponents()).toArray(new SeriesPanel[0]);
    }

    SeriesPanel getSeriesPanels(PlotSeries series) {
        for (SeriesPanel panel : getSeriesPanels()) {
            if (panel.series == series) {
                return panel;
            }
        }
        return null;
    }

    PlotSeries getActiveSeries() {
        for (Component c : panelSeriesSet.getComponents()) {
            if ((((SeriesPanel) c).series).isActive()) {
                return ((SeriesPanel) c).series;
            }
        }
        return null;
    }

    public void setActive(PlotSeries series) {
        if (!series.isActive()) {
            series.setActive();
            SeriesPanel panel = getSeriesPanels(series);
            if (panel != null) {
                panel.setActive();
            }
            //panelSeriesSet.repaint();
            updateAxisColor();
        }
    }

    void updateAxisColor() {

        if (config != null) {
            PlotSeries active = getActiveSeries();
            if ((active != null) && (config.axisYcolorStat == LabelColorAxisY.selectedCurve)) {
                plotPanel.setRangeAxisColor(plotPanel.getActiveSeriesIndex(), active.getColor());
            } else {
                plotPanel.setRangeAxisColor(config.foreground.toColor());
            }
            plotPanel.setDomainAxisColor(config.foreground.toColor());
        } else {
            plotPanel.setDomainAxisColor(null);
            plotPanel.setRangeAxisColor(null);
        }
    }

    public void clear() {
        plotPanel.removeAllSeries();
        panelSeriesSet.removeAll();
        SeriesPanel.active = null;
        updateAxisColor();
        repaint();

    }

    void updatePanelSeriesSet() {

        if (panelSeriesSet.isShowing()) {
            try {
                for (Component c : panelSeriesSet.getComponents()) {
                    ((SeriesPanel) c).update();
                }
            } catch (Exception ex) {
            }
        }
    }

    Timer redrawTimer;
    //java.util.Timer sampleTimer;
    ScheduledExecutorService sampleScheduler;
    Context context;
    final List<Channel<Double>> channels = new ArrayList<>();
    final Map<Channel<Double>, Timestamped<Double>> cache = new HashMap<>();
    //final ArrayList<Timestamped<Timestamped<Double>[]>> valueList = new ArrayList<>(); //Retain IOC time
    final ArrayList<Timestamped<double[]>> valueList = new ArrayList<>();
    volatile Config config;
    int numberOfSeries;

    void start() {
        stop();
        if (!App.isSimulated()) {
            System.out.println("Creating EPICS context");
            context = new Context(App.getCaProperties());
        }
        config = App.configFrame.config;
        numberOfSeries = 0;
        //Anti-aliasing
        plotPanel.setAntiAlias(App.isAntiAliasing());
        int series = (config == null) ? 0 : config.getNumberCurves();
        if (series > 0) {
            plotPanel.start();
            plotPanel.setPlotBackgroundColor(config.background.toColor());
            plotPanel.setPlotGridColor(config.grid.toColor());
            plotPanel.setDomainAxisColor(config.foreground.toColor());
            plotPanel.setGridXVisible(config.gridXon != GridVisibility.none, config.gridXon == GridVisibility.all);
            plotPanel.setGridYVisible(config.gridYon != GridVisibility.none, config.gridYon == GridVisibility.all);
            plotPanel.setDurationMillis(config.timespan * 1000);
            for (int i = 0; i < series; i++) {
                addSeries(config.curves[i], config.colors[i].toColor(), i);
            }
            startSampleTimer(true);
            startRedrawTimer(true);
            setActive(plotPanel.getPlotSeries(0));
        }
        updateAxisColor();
    }

    void startRedrawTimer(boolean triggerImmediately) {
        if (redrawTimer != null) {
            redrawTimer.stop();
            redrawTimer = null;
        }
        if (config.refreshInterval > 0) {
            redrawTimer = new Timer((int) (config.refreshInterval * 1000), (ActionEvent e) -> {
                plotValues();
            });
            if (triggerImmediately) {
                redrawTimer.setInitialDelay(0);
            }
            redrawTimer.start();
        }
    }

    void startSampleTimer(boolean triggerImmediately) {
        /*
        if (sampleTimer != null) {
            sampleTimer.cancel();
            sampleTimer = null;
        }  
         if (config.sampleInterval > 0) {
            sampleTimer = new java.util.Timer("Sample timer", true);
            
            sampleTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    addValue();
                }
            } 
            , triggerImmediately ? 0 : ((int) (config.sampleInterval * 1000)), ((int) (config.sampleInterval * 1000)));
        }
         */

        if (sampleScheduler != null) {
            sampleScheduler.shutdown();
            sampleScheduler = null;
        }
        if (config.sampleInterval > 0) {
            sampleScheduler = Executors.newSingleThreadScheduledExecutor();

            sampleScheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    addValue();
                }
            }, triggerImmediately ? 0 : ((int) (config.sampleInterval * 1000)), ((int) (config.sampleInterval * 1000)), TimeUnit.MILLISECONDS);
        }

    }

    void addValue() {
        try {
            if (!getPlot().isStarted()) {
                return;
            }
            long nanos = System.nanoTime();
            int series = getSeriesCount();
            double[] values = new double[series];
            if (App.isSimulated()) {
                for (int i = 0; i < series; i++) {
                    values[i] = Math.random() * (config.curves[i].max - config.curves[i].min) + config.curves[i].min;
                }
            } else {
                //Timestamped<Double>[] values = new Timestamped[series]; //Retain IOC time
                for (int i = 0; i < series; i++) {
                    try {
                        if (blocking) {
                            values[i] = channels.get(i).getAsync().get(1, TimeUnit.SECONDS);
                        } else {
                            values[i] = cache.get(channels.get(i)).getValue();
                        }
                    } catch (Exception ex) {
                        values[i] = Double.NaN;
                    }
                }
            }

            Timestamped<double[]> tv = toTimestamped(values, nanos);

            synchronized (valueList) {
                valueList.add(tv);
            }
            if (config.refreshInterval <= 0) {
                SwingUtilities.invokeLater(() -> plotValues());
            }
        } catch (Exception ex) {
        }
    }

    Timestamped toTimestamped(Object obj, Long nanos) {
        Timestamped tv = new Timestamped();
        //Timestamped<Timestamped<Double>[]> tv = new Timestamped<>(); //Retain IOC time
        tv.setValue(obj);
        if (nanos != null) {
            tv.setSeconds((long) (nanos / 1e9));
            tv.setNanos((int) (nanos % 1e9));
        }
        return tv;
    }
    int counterSample;

    void plotValues() {
        try {
            List<Timestamped<double[]>> values;
            //List<Timestamped<Timestamped<Double>[]>> values; //Retain IOC time
            synchronized (valueList) {
                values = (List<Timestamped<double[]>>) valueList.clone();
                //values = (List<Timestamped<Timestamped<Double>[]>>) valueList.clone(); //Retain IOC time
                valueList.clear();
            }
            if (getPlot().isStarted() & values.size() > 0) {
                for (Timestamped<double[]> value : values) {
                    getPlot().add(value.getMillis(), value.getValue());
                }
                //Retain IOC time
                /*
                for (Timestamped<Timestamped<Double>[]> value : values) {
                    if (value!=null){
                        for (int i=0; i< value.getValue().length; i++){
                            Timestamped<Double> td = value.getValue()[i];
                            getPlot().add(i, value.getMillis(), value.getValue());
                        }
                    }
                }
                 */
                getPlot().notifyUpdate();
            }
        } catch (Exception ex) {

        }
        counterSample++;
        if ((config.refreshInterval <= 0) || (config.refreshInterval * counterSample >= 0.5)) {
            updatePanelSeriesSet();
            counterSample = 0;
        }
    }

    void stop() {
        if (plotPanel != null) {
            plotPanel.stop();
        }
        if (sampleScheduler != null) {
            sampleScheduler.shutdown();
            sampleScheduler = null;
        }
        /*
        if (sampleTimer != null) {
            sampleTimer.cancel();
            sampleTimer = null;
        }     
         */
        if (redrawTimer != null) {
            redrawTimer.stop();
            redrawTimer = null;
        }
        if (context != null) {
            context.close();
            System.out.println("Closed EPICS context");
        }
        synchronized (channels) {
            channels.clear();
        }
        numberOfSeries = 0;
        cache.clear();
        config = null;
        clear();
    }

    int getSeriesCount() {
        return numberOfSeries;
        //synchronized (channels) {
        //    return channels.size();
        //}
    }

    boolean isStarted() {
        return (config != null) && (getSeriesCount() > 0);
    }

    public void setEnabled(int index, boolean value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].plotStatus = value;
            synchronized (channels) {
                if (value) {
                    channels.get(index).getAsync().handle((ret, ex) -> {
                        if (ex == null) {
                            cache.put(channels.get(index), toTimestamped(ret, null));
                        }
                        return ret;
                    });
                } else {
                    Timestamped<Double> tv = new Timestamped<>();
                    tv.setValue(Double.NaN);
                    cache.put(channels.get(index), toTimestamped(Double.NaN, null));
                }
            }
        }
    }

    public void setLog(int index, boolean value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].scale = value ? Scale.logarithmic : Scale.linear;
            plotPanel.series.get(index).plotSeries.setLogaritimic(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setPrec(int index, int value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].precision = value;
            plotPanel.series.get(index).plotSeries.setPrecision(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setMin(int index, double value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].min = value;
            plotPanel.series.get(index).plotSeries.setRangeMin(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setMax(int index, double value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].max = value;
            plotPanel.series.get(index).plotSeries.setRangeMax(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setUnits(int index, String value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].units = value;
            plotPanel.series.get(index).plotSeries.setUnits(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setDesc(int index, String value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].comment = value;
            plotPanel.series.get(index).plotSeries.setDesc(value);
            getSeriesPanels()[index].initialize();
        }
    }

    public void setColor(int index, Color value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.colors[index] = new DeepColor(value);
            plotPanel.series.get(index).plotSeries.setColor(value);
            getSeriesPanels()[index].initialize();
        }

    }

    public void setBackgroundColor(Color value) {
        if (config != null) {
            config.background = value == null ? new DeepColor() : new DeepColor(value);
            plotPanel.setPlotBackgroundColor(value);
        }
    }

    public void setGridColor(Color value) {
        if (config != null) {
            config.grid = value == null ? new DeepColor() : new DeepColor(value);
            plotPanel.setPlotGridColor(value);
            plotPanel.setPlotOutlineColor(value);
        }
    }

    public void setForegroundColor(Color value) {
        if (config != null) {
            config.foreground = value == null ? new DeepColor() : new DeepColor(value);
            updateAxisColor();
        }
    }

    public void setLabelColorAxisY(LabelColorAxisY value) {
        if (config != null) {
            config.axisYcolorStat = value;
            updateAxisColor();
        }
    }

    public void setGridX(GridVisibility value) {
        if (config != null) {
            config.gridXon = value;
            plotPanel.setGridXVisible(value != GridVisibility.none, value == GridVisibility.all);
        }
    }

    public void setGridY(GridVisibility value) {
        if (config != null) {
            config.gridYon = value;
            plotPanel.setGridYVisible(value != GridVisibility.none, value == GridVisibility.all);
        }
    }

    public void setLineWidth(int value) {
        if (config != null) {
            config.graphLineWidth = value;
            for (int i = 0; i < plotPanel.getNumberOfSeries(); i++) {
                plotPanel.setLineWidth(i, value);
            }
        }
    }

    public void setRedrawInterval(double value) {
        if (config != null) {
            if (config.refreshInterval != value) {
                config.refreshInterval = value;
                startRedrawTimer(false);
            }
        }
    }

    public void setSampleInterval(double value) {
        if (config != null) {
            if (config.sampleInterval != value) {
                config.sampleInterval = value;
                startSampleTimer(false);
            }
        }
    }

    public void setTimespan(int value) {
        if (config != null) {
            if (config.timespan != value) {
                config.timespan = value;
                plotPanel.setDurationMillis(value * 1000);
            }
        }
    }

    public void setNumSamples(int value) {
        if (config != null) {
            if (config.numSamples != value) {
                config.numSamples = value;
                for (int i = 0; i < plotPanel.getNumberOfSeries(); i++) {
                    plotPanel.setMaximumItemCount(i, value);
                }
            }
        }
    }

    public void addChannel(int index, String channelName, boolean enabled, boolean log,
            int precision, double min, double max, String units, String desc) {
        if (config != null) {
            if ((index >= 0) && (index <= config.getNumberCurves())) {
                if (isStarted() && (index < config.getNumberCurves())) {
                    stop();
                }
                Curve c = App.configFrame.config.insertCurve(index);
                c.name = channelName;
                c.plotStatus = enabled;
                c.scale = log ? Scale.logarithmic : Scale.linear;
                c.precision = precision;
                c.min = min;
                c.max = max;
                c.units = units;
                c.comment = desc;
                if (!isStarted()) {
                    start();
                } else {
                    addSeries(c, config.colors[index].toColor(), index);
                    setActive(plotPanel.getPlotSeries(index));
                }
            }
        }
    }

    public void removeChannel(int index) {
        if (config != null) {
            if ((index >= 0) && (index < config.getNumberCurves())) {
                if (((index < config.getNumberCurves() - 1)) || (config.getNumberCurves() == 1)) {
                    stop();
                } else {
                    removeSeries(index);
                }
                App.configFrame.config.removeCurve(index);

                if (!isStarted() && (App.configFrame.config.getNumberCurves() > 0)) {
                    start();
                }
            }
        }
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        plotPanel = new ch.psi.jstriptool.PlotPanel();
        panelSeriesSet = new javax.swing.JPanel();

        panelSeriesSet.setBorder(javax.swing.BorderFactory.createTitledBorder("Series"));
        panelSeriesSet.setLayout(new javax.swing.BoxLayout(panelSeriesSet, javax.swing.BoxLayout.PAGE_AXIS));

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 550, Short.MAX_VALUE)
                .addGap(0, 0, 0)
                .addComponent(panelSeriesSet, javax.swing.GroupLayout.PREFERRED_SIZE, 296, javax.swing.GroupLayout.PREFERRED_SIZE)
                .addGap(0, 0, 0))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 486, Short.MAX_VALUE)
            .addComponent(panelSeriesSet, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel panelSeriesSet;
    private ch.psi.jstriptool.PlotPanel plotPanel;
    // End of variables declaration//GEN-END:variables
}
