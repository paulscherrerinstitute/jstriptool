package ch.psi.jstriptool;

import ch.psi.jstriptool.Config.Curve;
import ch.psi.jstriptool.Config.DeepColor;
import ch.psi.jstriptool.Config.GridVisibility;
import ch.psi.jstriptool.Config.LabelColorAxisY;
import ch.psi.jstriptool.Config.Scale;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
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
import org.epics.ca.data.AlarmSeverity;
import org.epics.ca.data.Timestamped;

/**
 *
 */
public class PlotFrame extends javax.swing.JFrame {

    static PlotFrame instance;
    final boolean blocking;
    final boolean hideInvalid;

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
        hideInvalid = App.getHideInvalid();
        
        //Not implemented
        buttonRedraw.setVisible(false);
        //buttonMode.setVisible(false);
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
        panelSeriesSet.addSeries(series);
        repaint();

        if (!App.isSimulated()) {
            Channel<Double> channel = ((c.name!=null) && (!c.name.isEmpty())) ? context.createChannel(c.name, Double.class) : null;
            if (channel!=null){
                //channel.connect();
                channel.connectAsync().handle((ret, ex) -> {
                    System.out.println("Connected to channel: " + c.name);
                    series.setConnected();
                    if (!blocking) {
                        if (c.isEnabled()) {
                            updateAsync(channel);
                        }
                        Monitor<Timestamped<Double>> monitor = channel.addMonitor(Timestamped.class, timestampedValue -> {
                            if (timestampedValue != null) {
                                if (c.isEnabled()) {
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
            }
            cache.put(channel, toTimestamped(Double.NaN, null));
            //channel.connect();
            
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
        panelSeriesSet.removeSeries(index);
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
        panelSeriesSet.clear();
        updateAxisColor();
        repaint();

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
            plotPanel.repaint();
             SwingUtilities.invokeLater(() -> {
                 
                    setActive(plotPanel.getPlotSeries(0));
             });
           
        }
        updateAxisColor();
        if (SwingUtilities.isEventDispatchThread()){
            panelSeriesSet.update();
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
            long millis = System.currentTimeMillis();
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
                            if (config.curves[i].isEnabled()){
                                values[i] = channels.get(i).getAsync().get(1, TimeUnit.SECONDS);
                            } else {
                                values[i] = Double.NaN;
                            }
                        } else {
                            Timestamped<Double> t = cache.get(channels.get(i));
                            values[i] = (hideInvalid && (t.getAlarmSeverity() == AlarmSeverity.INVALID_ALARM)) ? Double.NaN: t.getValue();
                        }
                    } catch (Exception ex) {
                        values[i] = Double.NaN;
                    }
                }
            }

            Timestamped<double[]> tv = toTimestamped(values, millis);

            synchronized (valueList) {
                valueList.add(tv);
            }
            if (config.refreshInterval <= 0) {
                SwingUtilities.invokeLater(() -> plotValues());
            }
        } catch (Exception ex) {
        }
    }

    Timestamped toTimestamped(Object obj, Long millis) {
        Timestamped tv = new Timestamped();
        //Timestamped<Timestamped<Double>[]> tv = new Timestamped<>(); //Retain IOC time}
        tv.setValue(obj);
        if (obj != null){
            tv.setAlarmSeverity(AlarmSeverity.NO_ALARM);
        }
        if (millis != null) {
            tv.setSeconds((long) (millis / 1e3));
            tv.setNanos((int) ((millis % 1e3) * 1e6));
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
            panelSeriesSet.update();
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
    
    void updateAsync(Channel channel){
        channel.getAsync(Timestamped.class).handle((value, ex) -> {
            if (ex == null) {
                    cache.put(channel, (Timestamped)value);
                }
            return value;
        });    
    }

    public void setEnabled(int index, boolean value) {
        if (isStarted() && index < config.getNumberCurves()) {
            config.curves[index].plotStatus = value;
            synchronized (channels) {
                if (value) {
                    updateAsync(channels.get(index));
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
            config.colors[index] = new DeepColor(value);
            plotPanel.series.get(index).plotSeries.setColor(value);
            panelSeriesSet.updateSeries(index);
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

        jToolBar1 = new javax.swing.JToolBar();
        jToolBar2 = new javax.swing.JToolBar();
        panelMain = new javax.swing.JPanel();
        panelSeriesSet = new ch.psi.jstriptool.SeriesSetPanel();
        plotPanel = new ch.psi.jstriptool.PlotPanel();
        toolBar = new javax.swing.JToolBar();
        jSeparator5 = new javax.swing.JToolBar.Separator();
        buttonPanLeft = new javax.swing.JButton();
        buttonPanRight = new javax.swing.JButton();
        buttonPanUp = new javax.swing.JButton();
        buttonPanDown = new javax.swing.JButton();
        jSeparator1 = new javax.swing.JToolBar.Separator();
        buttonZoomInX = new javax.swing.JButton();
        buttonZoomOutX = new javax.swing.JButton();
        buttonZoomInY = new javax.swing.JButton();
        buttonZoomOutY = new javax.swing.JButton();
        jSeparator2 = new javax.swing.JToolBar.Separator();
        buttonResetGraph = new javax.swing.JButton();
        buttonRedraw = new javax.swing.JButton();
        jSeparator3 = new javax.swing.JToolBar.Separator();
        buttonAutoScale = new javax.swing.JToggleButton();
        buttonMode = new javax.swing.JToggleButton();

        jToolBar1.setRollover(true);

        jToolBar2.setRollover(true);

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
                .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 529, Short.MAX_VALUE)
                .addGap(0, 0, 0)
                .addComponent(panelSeriesSet, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        );
        panelMainLayout.setVerticalGroup(
            panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelMainLayout.createSequentialGroup()
                .addGap(0, 0, 0)
                .addGroup(panelMainLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(plotPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 475, Short.MAX_VALUE)
                    .addComponent(panelSeriesSet, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addGap(0, 0, 0))
        );

        toolBar.setFloatable(false);
        toolBar.setPreferredSize(new java.awt.Dimension(405, 25));
        toolBar.add(jSeparator5);

        buttonPanLeft.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/PanLeft.png"))); // NOI18N
        buttonPanLeft.setText(" ");
        buttonPanLeft.setToolTipText("Pan Left");
        buttonPanLeft.setFocusable(false);
        buttonPanLeft.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonPanLeft.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonPanLeftActionPerformed(evt);
            }
        });
        toolBar.add(buttonPanLeft);

        buttonPanRight.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/PanRight.png"))); // NOI18N
        buttonPanRight.setText(" ");
        buttonPanRight.setToolTipText("Pan Right");
        buttonPanRight.setFocusable(false);
        buttonPanRight.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonPanRight.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonPanRightActionPerformed(evt);
            }
        });
        toolBar.add(buttonPanRight);

        buttonPanUp.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/PanUp.png"))); // NOI18N
        buttonPanUp.setText(" ");
        buttonPanUp.setToolTipText("Pan Up");
        buttonPanUp.setFocusable(false);
        buttonPanUp.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonPanUp.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonPanUpActionPerformed(evt);
            }
        });
        toolBar.add(buttonPanUp);

        buttonPanDown.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/PanDown.png"))); // NOI18N
        buttonPanDown.setText(" ");
        buttonPanDown.setToolTipText("Pan Down");
        buttonPanDown.setFocusable(false);
        buttonPanDown.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonPanDown.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonPanDownActionPerformed(evt);
            }
        });
        toolBar.add(buttonPanDown);
        toolBar.add(jSeparator1);

        buttonZoomInX.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ZoomInX.png"))); // NOI18N
        buttonZoomInX.setText(" ");
        buttonZoomInX.setToolTipText("Zoom In X");
        buttonZoomInX.setFocusable(false);
        buttonZoomInX.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonZoomInX.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonZoomInXActionPerformed(evt);
            }
        });
        toolBar.add(buttonZoomInX);

        buttonZoomOutX.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ZoomOutX.png"))); // NOI18N
        buttonZoomOutX.setText(" ");
        buttonZoomOutX.setToolTipText("Zoom Out X");
        buttonZoomOutX.setFocusable(false);
        buttonZoomOutX.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonZoomOutX.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonZoomOutXActionPerformed(evt);
            }
        });
        toolBar.add(buttonZoomOutX);

        buttonZoomInY.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ZoomInY.png"))); // NOI18N
        buttonZoomInY.setText(" ");
        buttonZoomInY.setToolTipText("Zoom In Y");
        buttonZoomInY.setFocusable(false);
        buttonZoomInY.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonZoomInY.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonZoomInYActionPerformed(evt);
            }
        });
        toolBar.add(buttonZoomInY);

        buttonZoomOutY.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ZoomOutY.png"))); // NOI18N
        buttonZoomOutY.setText(" ");
        buttonZoomOutY.setToolTipText("Zoom Out Y");
        buttonZoomOutY.setFocusable(false);
        buttonZoomOutY.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonZoomOutY.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonZoomOutYActionPerformed(evt);
            }
        });
        toolBar.add(buttonZoomOutY);
        toolBar.add(jSeparator2);

        buttonResetGraph.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/Reset.png"))); // NOI18N
        buttonResetGraph.setText(" ");
        buttonResetGraph.setToolTipText("ResetGraph");
        buttonResetGraph.setFocusable(false);
        buttonResetGraph.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonResetGraph.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonResetGraphActionPerformed(evt);
            }
        });
        toolBar.add(buttonResetGraph);

        buttonRedraw.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/Redraw.png"))); // NOI18N
        buttonRedraw.setText(" ");
        buttonRedraw.setToolTipText("ResetGraph");
        buttonRedraw.setFocusable(false);
        buttonRedraw.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonRedraw.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonRedrawActionPerformed(evt);
            }
        });
        toolBar.add(buttonRedraw);
        toolBar.add(jSeparator3);

        buttonAutoScale.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/AutoScaleOff.png"))); // NOI18N
        buttonAutoScale.setToolTipText("AutoScale");
        buttonAutoScale.setFocusable(false);
        buttonAutoScale.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonAutoScale.setVerticalTextPosition(javax.swing.SwingConstants.BOTTOM);
        buttonAutoScale.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonAutoScaleActionPerformed(evt);
            }
        });
        toolBar.add(buttonAutoScale);

        buttonMode.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ScrollingMode.png"))); // NOI18N
        buttonMode.setToolTipText("ScrollingMode");
        buttonMode.setFocusable(false);
        buttonMode.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        buttonMode.setVerticalTextPosition(javax.swing.SwingConstants.BOTTOM);
        buttonMode.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonModeActionPerformed(evt);
            }
        });
        toolBar.add(buttonMode);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(panelMain, javax.swing.GroupLayout.DEFAULT_SIZE, 830, Short.MAX_VALUE)
            .addComponent(toolBar, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, layout.createSequentialGroup()
                .addComponent(panelMain, javax.swing.GroupLayout.DEFAULT_SIZE, 475, Short.MAX_VALUE)
                .addGap(0, 0, 0)
                .addComponent(toolBar, javax.swing.GroupLayout.PREFERRED_SIZE, 25, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void buttonPanLeftActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonPanLeftActionPerformed
        try{
            plotPanel.panLeft();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonPanLeftActionPerformed

    private void buttonPanRightActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonPanRightActionPerformed
        try{
            plotPanel.panRight();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonPanRightActionPerformed

    private void buttonPanUpActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonPanUpActionPerformed
        try{
            plotPanel.panUp();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonPanUpActionPerformed

    private void buttonPanDownActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonPanDownActionPerformed
        try{
           plotPanel.panDown();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonPanDownActionPerformed

    private void buttonZoomOutXActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonZoomOutXActionPerformed
        try{
            plotPanel.zoomOutX();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonZoomOutXActionPerformed

    private void buttonZoomInXActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonZoomInXActionPerformed
        try{
            plotPanel.zoomInX();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonZoomInXActionPerformed

    private void buttonZoomOutYActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonZoomOutYActionPerformed
        try{
            plotPanel.zoomOutY();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonZoomOutYActionPerformed

    private void buttonZoomInYActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonZoomInYActionPerformed
        try{
            plotPanel.zoomInY();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonZoomInYActionPerformed

    private void buttonResetGraphActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonResetGraphActionPerformed
        try{
            plotPanel.resetZoom();
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonResetGraphActionPerformed

    private void buttonRedrawActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonRedrawActionPerformed
        // TODO add your handling code here:
    }//GEN-LAST:event_buttonRedrawActionPerformed

    private void buttonAutoScaleActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonAutoScaleActionPerformed
        try{
            if (buttonAutoScale.isSelected()){
                buttonAutoScale.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/AutoScaleOn.png")));
                plotPanel.setAutoScale(true);
            } else {
                buttonAutoScale.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/AutoScaleOff.png")));
                plotPanel.setAutoScale(false);
            }
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonAutoScaleActionPerformed

    private void buttonModeActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonModeActionPerformed
        try{
            plotPanel.setScrollMode(!buttonMode.isSelected());
            if (buttonMode.isSelected()){
                buttonMode.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/PanningMode.png")));
            } else {
                buttonMode.setIcon(new javax.swing.ImageIcon(getClass().getResource("/ch/psi/jstriptool/ScrollingMode.png")));
            }
        } catch (Exception ex){
            SwingUtils.showException(this, ex);
        }        
    }//GEN-LAST:event_buttonModeActionPerformed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JToggleButton buttonAutoScale;
    private javax.swing.JToggleButton buttonMode;
    private javax.swing.JButton buttonPanDown;
    private javax.swing.JButton buttonPanLeft;
    private javax.swing.JButton buttonPanRight;
    private javax.swing.JButton buttonPanUp;
    private javax.swing.JButton buttonRedraw;
    private javax.swing.JButton buttonResetGraph;
    private javax.swing.JButton buttonZoomInX;
    private javax.swing.JButton buttonZoomInY;
    private javax.swing.JButton buttonZoomOutX;
    private javax.swing.JButton buttonZoomOutY;
    private javax.swing.JToolBar.Separator jSeparator1;
    private javax.swing.JToolBar.Separator jSeparator2;
    private javax.swing.JToolBar.Separator jSeparator3;
    private javax.swing.JToolBar.Separator jSeparator5;
    private javax.swing.JToolBar jToolBar1;
    private javax.swing.JToolBar jToolBar2;
    private javax.swing.JPanel panelMain;
    private ch.psi.jstriptool.SeriesSetPanel panelSeriesSet;
    private ch.psi.jstriptool.PlotPanel plotPanel;
    private javax.swing.JToolBar toolBar;
    // End of variables declaration//GEN-END:variables
}
