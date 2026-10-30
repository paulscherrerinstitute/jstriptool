package ch.psi.jstriptool;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import javax.swing.JMenuItem;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import org.apache.commons.lang3.tuple.Pair;

/**
 *
 */
public class GraphPanel extends javax.swing.JPanel {

    final boolean blocking;
    
    public GraphPanel() {
        javax.swing.JPopupMenu.setDefaultLightWeightPopupEnabled(false);        
        initComponents();
        plotPanel.setDurationMillis(60000);
        plotPanel.addPopupMenuItem(null);
        plotPanel.setAntiAliasing(App.isAntiAliasing());
        plotPanel.setMarkersVisible(App.isMarkersVisible());
        JMenuItem menuShowConfig = new JMenuItem("Show Config Window");
        menuShowConfig.addActionListener((ActionEvent e) -> {
            App.configFrame.setVisible(true);
        });
        plotPanel.addPopupMenuItem(menuShowConfig);    
        blocking = App.isBlocking();
    }
    
    PlotPanel getPlot() {
        return plotPanel;
    }

    void addSeries(Config.Curve c, Color color, int index) {
        numberOfSeries++;
        PlotSeries series = PlotSeries.create(c, color, config.graphLineWidth, config.numSamples);
        series.setActive();
        plotPanel.addSeries(series, c.isEnabled());
        panelSeriesSet.addSeries(series);
        repaint();

        if (!App.isSimulated()) {
            Channel pv = new Channel(c.name) {
                protected void onConnection() {
                    series.setConnected();
                    if (!blocking) {
                        if (c.isEnabled()) {
                            updateAsync(this);
                        }
                        setMonitor((value) -> {
                            addValue(this, value);
                        });
                    }

                }
            };
            cache.put(pv, null);
            synchronized (channels) {
                channels.add(pv);
            }
        }
    }
    
    void addValue(Channel pv, ChannelValue value){
        if (started() && (value!=null)) {
            if (config.getSamplingMode() == Config.SamplingMode.MONITOR) {
                synchronized (cache){
                    cache.put(pv, value);
                    updateValues();
                }
            } else {
                cache.put(pv, value);
            }
         }        
    }

    public void removeSeries(int index) {
        if (plotPanel.getActiveSeriesIndex() == index) {
            setActive(plotPanel.getPlotSeries((index == 0) ? 1 : 0));
        }
        numberOfSeries--;
        plotPanel.removeSeries(index);
        panelSeriesSet.removeSeries(index);
        repaint();
        if (!App.isSimulated()) {
            Channel channel = null;
            synchronized (channels) {
                channel = channels.get(index);
                channels.remove(index);
            }
            if (channel != null) {
                channel.close();
            }
        }
    }

    public void setActive(PlotSeries series) {
        if (!series.isActive()) {
            series.setActive();
            panelSeriesSet.setSeriesActive(series);
            updateAxisColor();
        }
    }

    void updateAxisColor() {
        if (config != null) {
            PlotSeries active = panelSeriesSet.getActiveSeries();
            if ((active != null) && (config.axisYcolorStat == Config.LabelColorAxisY.selectedCurve)) {
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
        panelSeriesSet.clear();
        updateAxisColor();
        repaint();

    }

    Timer redrawTimer;
    ScheduledExecutorService sampleScheduler;
    final List<Channel> channels = new ArrayList<>();
    final Map<Channel, ChannelValue> cache = new HashMap<>();
    final ArrayList<Pair<Long, ChannelValue[]>> valueList = new ArrayList<>();
    final Map<Channel, ChannelValue> valueListSparse = new HashMap<>();
    volatile Config config;
    int numberOfSeries;

    void start() {
        stop();
        config = App.getConfig();
        numberOfSeries = 0;        
        //Anti-aliasing        
        int series = (config == null) ? 0 : config.getNumberCurves();
        if (series > 0) {
            plotPanel.start();
            plotPanel.setPlotBackgroundColor(config.background.toColor());
            plotPanel.setPlotGridColor(config.grid.toColor());
            plotPanel.setDomainAxisColor(config.foreground.toColor());
            plotPanel.setGridXVisible(config.gridXon != Config.GridVisibility.none, config.gridXon == Config.GridVisibility.all);
            plotPanel.setGridYVisible(config.gridYon != Config.GridVisibility.none, config.gridYon == Config.GridVisibility.all);
            plotPanel.setDurationMillis(config.timespan * 1000);
            for (int i = 0; i < series; i++) {
                addSeries(config.curves[i], config.colors[i].toColor(), i);
            }
            startSampleTimer(true);
            startRedrawTimer(true);
            plotPanel.repaint();
            SwingUtilities.invokeLater(() -> {
                setActive(plotPanel.getPlotSeries(0));
            });

        }
        updateAxisColor();
        if (SwingUtilities.isEventDispatchThread()) {
            panelSeriesSet.update();
        }
    }
    
    boolean started(){
        return config!=null;
    }
    
    PlotPanel getPlotPanel(){
        return plotPanel;
    }
    
    public void reset() {
        plotPanel.clear();        
        try{
            for (int index = 0; index<channels.size(); index++) {
                if (config.curves[index].isEnabled()){
                    updateAsync(channels.get(index));
                }
            }
        } catch (Exception ex){
            Logger.getLogger(GraphFrame.class.getName()).warning(() -> "Error resetting plot: " + ex);
        }
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
        if (sampleScheduler != null) {
            sampleScheduler.shutdown();
            sampleScheduler = null;
        }
        if (config.getSamplingMode() == Config.SamplingMode.POLLING) {
            sampleScheduler = Executors.newSingleThreadScheduledExecutor();

            sampleScheduler.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    updateValues();
                }
            }, triggerImmediately ? 0 : ((int) (config.sampleInterval * 1000)), ((int) (config.sampleInterval * 1000)), TimeUnit.MILLISECONDS);
        }

    }

    void updateValues() {        
        try {
            if (!getPlot().isStarted()) {
                return;
            }
            long millis = System.currentTimeMillis();
            int series = getSeriesCount();
            ChannelValue[] values = new ChannelValue[series];
            for (int i = 0; i < series; i++) {
                ChannelValue value = null;
                try {                    
                    if (config.curves[i].isEnabled()) {
                        if (App.isSimulated()) {
                            double v = Math.random() * (config.curves[i].max - config.curves[i].min) + config.curves[i].min;
                            value = new ChannelValue(v, System.currentTimeMillis(), false);
                        } else {
                            if (blocking) {
                                value = channels.get(i).readValue();
                            } else {
                                value = cache.get(channels.get(i));
                            }
                        }                        
                    }
                } catch (Exception ex) {
                    String channelName = (i<channels.size()) ? channels.get(i).name : "UNDEFINED";
                    Logger.getLogger(GraphFrame.class.getName()).warning(() -> "Error adding channel: " + channelName + " value: " + ex);
                }
                values[i] = value;
            }
            if (App.timeMode.isSparse()){                
                cache.clear();
            }
            synchronized (valueList) {
                valueList.add(Pair.of(millis, values));
            }
            if (config.refreshInterval <= 0) {
                SwingUtilities.invokeLater(() -> plotValues());
            }
        } catch (Exception ex) {
        }

    }

    int counterSample;

    void plotValues() {
        try {
            ArrayList<Pair<Long, ChannelValue[]>> values;
            synchronized (valueList) {
                values = (ArrayList<Pair<Long, ChannelValue[]>>) valueList.clone();
                valueList.clear();
            }
            if (getPlot().isStarted() & values.size() > 0) {
                for (var val : values) {
                    var localTimestamp = val.getLeft();
                    var channelValues = val.getRight();
                    for (int i = 0; i < channelValues.length; i++) {
                        var channelValue = channelValues[i];                        
                        if (channelValue!=null){
                            var timestamp = App.timeMode.isLocalTime() ? localTimestamp : channelValue.timestamp();
                            getPlot().add(i, timestamp, channelValue.displayValue());
                        }
                    }
                }
                getPlot().notifyUpdate();
            }
        } catch (Exception ex) {

        }
        counterSample++;
        if ((config.refreshInterval <= 0) || (config.refreshInterval * counterSample >= 0.5)) {
            panelSeriesSet.update();
            counterSample = 0;
        }
    }

    void stop() {
        if (plotPanel != null) {
            plotPanel.pause();
        }
        if (sampleScheduler != null) {
            sampleScheduler.shutdown();
            sampleScheduler = null;
        }
        if (redrawTimer != null) {
            redrawTimer.stop();
            redrawTimer = null;
        }
        
        for (Channel channel: channels) {
            channel.close();
        }
        
        synchronized (channels) {
            channels.clear();
        }
        synchronized (valueList) {
            valueList.clear();
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

    void updateAsync(Channel channel) {
        channel.getAsync().handle((value, ex) -> {
            if (ex == null) {
                addValue(channel, value);
            }
            return value;
        });
    }

    public void setEnabled(int index, boolean value) {
        if (isStarted() && index < config.getNumberCurves()) {
            boolean enabled = config.curves[index].isEnabled();
            if (enabled!=value){
                if (App.timeMode.isLocalTime() && !value) {
                    addValue(channels.get(index), ChannelValue.empty()); 
                    updateValues();
                }                
                config.curves[index].plotStatus = value;
                plotPanel.series.get(index).setLineExtension(value);
                if (App.timeMode.isLocalTime() && value) {
                    updateAsync(channels.get(index));
                }
            }
        }
    }

    public void setLog(int index, boolean value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].scale = value ? Config.Scale.logarithmic : Config.Scale.linear;
            plotPanel.series.get(index).plotSeries.setLogaritimic(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setPrec(int index, int value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].precision = value;
            plotPanel.series.get(index).plotSeries.setPrecision(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setMin(int index, double value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].min = value;
            plotPanel.series.get(index).plotSeries.setRangeMin(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setMax(int index, double value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].max = value;
            plotPanel.series.get(index).plotSeries.setRangeMax(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setUnits(int index, String value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].units = value;
            plotPanel.series.get(index).plotSeries.setUnits(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setDesc(int index, String value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].comment = value;
            plotPanel.series.get(index).plotSeries.setDesc(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setColor(int index, Color value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.colors[index] = new Config.DeepColor(value);
            plotPanel.series.get(index).plotSeries.setColor(value);
            panelSeriesSet.updateSeries(index);
        }
    }

    public void setBackgroundColor(Color value) {
        if (config != null) {
            config.background = value == null ? new Config.DeepColor() : new Config.DeepColor(value);
            plotPanel.setPlotBackgroundColor(value);
        }
    }

    public void setGridColor(Color value) {
        if (config != null) {
            config.grid = value == null ? new Config.DeepColor() : new Config.DeepColor(value);
            plotPanel.setPlotGridColor(value);
        }
    }

    public void setForegroundColor(Color value) {
        if (config != null) {
            config.foreground = value == null ? new Config.DeepColor() : new Config.DeepColor(value);
            updateAxisColor();
        }
    }

    public void setLabelColorAxisY(Config.LabelColorAxisY value) {
        if (config != null) {
            config.axisYcolorStat = value;
            updateAxisColor();
        }
    }

    public void setGridX(Config.GridVisibility value) {
        if (config != null) {
            config.gridXon = value;
            plotPanel.setGridXVisible(value != Config.GridVisibility.none, value == Config.GridVisibility.all);
        }
    }

    public void setGridY(Config.GridVisibility value) {
        if (config != null) {
            config.gridYon = value;
            plotPanel.setGridYVisible(value != Config.GridVisibility.none, value == Config.GridVisibility.all);
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
                Config.Curve c = config.insertCurve(index);
                c.name = channelName;
                c.plotStatus = enabled;
                c.scale = log ? Config.Scale.logarithmic : Config.Scale.linear;
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
                config.removeCurve(index);

                if (!isStarted() && (config.getNumberCurves() > 0)) {
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

        panelMain = new javax.swing.JPanel();
        panelSeriesSet = new ch.psi.jstriptool.SeriesSetPanel();
        plotPanel = new ch.psi.jstriptool.PlotPanel();

        setPreferredSize(new java.awt.Dimension(600, 400));
        setLayout(new java.awt.BorderLayout());

        panelMain.setPreferredSize(new java.awt.Dimension(828, 100));

        javax.swing.GroupLayout panelSeriesSetLayout = new javax.swing.GroupLayout(panelSeriesSet);
        panelSeriesSet.setLayout(panelSeriesSetLayout);
        panelSeriesSetLayout.setHorizontalGroup(
            panelSeriesSetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 301, Short.MAX_VALUE)
        );
        panelSeriesSetLayout.setVerticalGroup(
            panelSeriesSetLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 0, Short.MAX_VALUE)
        );

        plotPanel.setPreferredSize(new java.awt.Dimension(300, 100));

        javax.swing.GroupLayout panelMainLayout = new javax.swing.GroupLayout(panelMain);
        panelMain.setLayout(panelMainLayout);
        panelMainLayout.setHorizontalGroup(
            panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelMainLayout.createSequentialGroup()
                .addGap(0, 0, 0)
                .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 283, Short.MAX_VALUE)
                .addGap(0, 0, 0)
                .addComponent(panelSeriesSet, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        );
        panelMainLayout.setVerticalGroup(
            panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelMainLayout.createSequentialGroup()
                .addGap(0, 0, 0)
                .addGroup(panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 409, Short.MAX_VALUE)
                    .addComponent(panelSeriesSet, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addGap(0, 0, 0))
        );

        add(panelMain, java.awt.BorderLayout.CENTER);
    }// </editor-fold>//GEN-END:initComponents


    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JPanel panelMain;
    private ch.psi.jstriptool.SeriesSetPanel panelSeriesSet;
    private ch.psi.jstriptool.PlotPanel plotPanel;
    // End of variables declaration//GEN-END:variables
}
