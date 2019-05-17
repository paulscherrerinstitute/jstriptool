package ch.psi.jstriptool;

import ch.psi.jstriptool.Config.Curve;
import ch.psi.jstriptool.Config.GridVisibility;
import ch.psi.jstriptool.Config.LabelColorAxisY;
import ch.psi.jstriptool.Config.Scale;
import java.awt.Color;
import java.awt.Component;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;
import java.io.IOException;
import java.util.EventObject;
import java.util.concurrent.TimeUnit;
import javax.swing.AbstractCellEditor;
import javax.swing.JColorChooser;
import javax.swing.JComponent;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JTable;
import javax.swing.JTextField;
import javax.swing.event.TableModelEvent;
import javax.swing.event.TableModelListener;
import javax.swing.filechooser.FileNameExtensionFilter;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.DefaultTableModel;
import javax.swing.table.TableCellEditor;
import javax.swing.table.TableColumn;
import org.epics.ca.Channel;
import org.epics.ca.Context;
import org.epics.ca.data.Graphic;

/**
 *
 */
public class ConfigFrame extends javax.swing.JFrame {

    final DefaultTableModel modelSeries;
    Config config;
    File file;

    final int COLUMN_NAME = 0;
    final int COLUMN_ENABLED = 1;
    final int COLUMN_LOG = 2;
    final int COLUMN_PREC = 3;
    final int COLUMN_MIN = 4;
    final int COLUMN_MAX = 5;
    final int COLUMN_UNITS = 6;
    final int COLUMN_DESC = 7;
    final int COLUMN_COLORS = 8;

    final int[] PREFERRED_WIDTHS = new int[]{225, 60, 60, 60, 60, 60, 60, 60, 60};

    /**
     * Creates new form ConfigFrame
     */
    public ConfigFrame() {
        initComponents();
        modelSeries = (DefaultTableModel) tableSeries.getModel();
        modelSeries.addTableModelListener(modelSeriesListener);
        clear();

    }

    Config getConfig() {
        return config;
    }

    boolean isPlotting() {
        return (App.plotFrame.isStarted());
    }

    TableModelListener modelSeriesListener = new TableModelListener() {
        @Override
        public void tableChanged(TableModelEvent e) {
            if (isPlotting()) {
                try {
                    if (e.getFirstRow() == e.getLastRow()) {
                        int col = e.getColumn();
                        int index = e.getFirstRow();
                        Object value = modelSeries.getValueAt(index, col);

                        switch (col) {
                            case COLUMN_ENABLED:
                                App.plotFrame.setEnabled(index, (value == null) ? false : (Boolean) value);
                                break;
                            case COLUMN_LOG:
                                App.plotFrame.setLog(index, (value == null) ? false : (Boolean) value);
                                break;
                            case COLUMN_PREC:
                                App.plotFrame.setPrec(index, (value == null) ? 0 : (Integer) value);
                                break;
                            case COLUMN_MIN:
                                App.plotFrame.setMin(index, (value == null) ? Double.NaN : (Double) value);
                                break;
                            case COLUMN_MAX:
                                App.plotFrame.setMax(index, (value == null) ? Double.NaN : (Double) value);
                                break;
                            case COLUMN_UNITS:
                                App.plotFrame.setUnits(index, (value == null) ? "" : (String) value);
                                break;
                            case COLUMN_DESC:
                                App.plotFrame.setDesc(index, (value == null) ? "" : (String) value);
                                break;
                            case COLUMN_COLORS:
                                App.plotFrame.setColor(index, (value == null) ? Color.BLACK : Config.getColorFromString((String) value));
                        }
                    }
                } catch (Exception ex) {
                    SwingUtils.showException(ConfigFrame.this, ex);
                }
            }
        }
    };

    boolean isSeriesTableRowEditable(int row, int column) {
        return (column > COLUMN_NAME);
    }

    protected void updateButtons() {
        boolean editing = true;
        int rows = modelSeries.getRowCount();
        int cur = tableSeries.getSelectedRow();
        buttonUp.setEnabled((rows > 0) && (cur > 0) && editing);
        buttonDown.setEnabled((rows > 0) && (cur >= 0) && (cur < (rows - 1)) && editing);
        buttonDelete.setEnabled((rows > 0) && (cur >= 0) && editing);
        buttonInsert.setEnabled((modelSeries.getRowCount() < Config.MAX_NUMBER_PLOTS) && editing);
    }

    void initializeTable() {
        //Fix bug of nimbus rendering Boolean in table
        ((JComponent) tableSeries.getDefaultRenderer(Boolean.class)).setOpaque(true);

        TableColumn colName = tableSeries.getColumnModel().getColumn(COLUMN_NAME);
        colName.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_NAME]);

        TableColumn colEnabled = tableSeries.getColumnModel().getColumn(COLUMN_ENABLED);
        colEnabled.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_ENABLED]);
        //colEnabled.setResizable(true);

        TableColumn colLog = tableSeries.getColumnModel().getColumn(COLUMN_LOG);
        colLog.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_LOG]);
        //colLog.setResizable(true);

        TableColumn colPrecision = tableSeries.getColumnModel().getColumn(COLUMN_PREC);
        colPrecision.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_PREC]);

        TableColumn colMin = tableSeries.getColumnModel().getColumn(COLUMN_MIN);
        colMin.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_MIN]);

        TableColumn colMax = tableSeries.getColumnModel().getColumn(COLUMN_MAX);
        colMax.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_MAX]);

        TableColumn colUnits = tableSeries.getColumnModel().getColumn(COLUMN_UNITS);
        colUnits.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_UNITS]);

        TableColumn colDesc = tableSeries.getColumnModel().getColumn(COLUMN_DESC);
        colDesc.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_DESC]);

        TableColumn colColors = tableSeries.getColumnModel().getColumn(COLUMN_COLORS);
        colColors.setPreferredWidth(PREFERRED_WIDTHS[COLUMN_COLORS]);
        class ColorEditor extends AbstractCellEditor implements TableCellEditor {

            private final JTextField field = new JTextField();
            private Color color;

            ColorEditor() {
                field.setBorder(null);
                field.addMouseListener(new MouseAdapter() {
                    @Override
                    public void mousePressed(MouseEvent e) {
                        if (e.getClickCount() == 2) {
                            Color ret = JColorChooser.showDialog(ConfigFrame.this, "Choose a Color - Click 'Cancel for default", color);
                            if (ret != null) {
                                color = ret;
                            }
                            field.setBackground(color);
                            stopCellEditing();
                        }
                    }
                });
            }

            @Override
            public Component getTableCellEditorComponent(JTable table, Object value, boolean isSelected, int row, int column) {
                color = Config.getColorFromString((String) value);
                field.setBackground(color);
                field.setEditable(false);
                return field;
            }

            @Override
            public Object getCellEditorValue() {
                return (color == null) ? "" : color.getRed() + "," + color.getGreen() + "," + color.getBlue();
            }

            @Override
            public boolean isCellEditable(EventObject ev) {
                if (ev instanceof MouseEvent) {
                    return ((MouseEvent) ev).getClickCount() >= 2;
                }
                return true;
            }

            @Override
            public boolean shouldSelectCell(EventObject ev) {
                return false;
            }
        }
        colColors.setCellEditor(new ColorEditor());

        colColors.setCellRenderer(new DefaultTableCellRenderer() {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column) {
                Component comp = super.getTableCellRendererComponent(table, "", isSelected, hasFocus, row, column);
                Color color = Config.getColorFromString((String) value);
                ((JLabel) comp).setBackground(color);
                ((JLabel) comp).setEnabled(false);
                return comp;
            }
        });

    }

    void clear() {
        config = App.defaultConfig == null ? new Config() : App.defaultConfig.clone();
        file = null;
        updateWindow();
        initializeTable();
        modelSeries.setRowCount(0);
        setTitle("JStripTool");
        if (App.plotFrame != null) {
            App.plotFrame.setTitle("JStripTool");
            App.plotFrame.stop();
        }
    }

    void open(File file) throws IOException {
        clear();
        Config config = App.defaultConfig == null ? new Config() : App.defaultConfig.clone();
        config.open(file);
        this.file = file;
        this.config = config;
        this.setTitle(file.getName());
        updateWindow();
        App.plotFrame.setVisible(true);
        App.plotFrame.setTitle(file.getName());
        App.plotFrame.setVisible(true);
        App.plotFrame.start();
    }

    void save(File file) throws IOException {
        //updateConfig();
        config.save(file);
        this.file = file;
        this.setTitle(file.getName());
        App.plotFrame.setTitle(file.getName());
    }

    void updateColors() {
        for (int i = 0; i < modelSeries.getRowCount(); i++) {
            modelSeries.setValueAt(Config.getStringFromColor(config.colors[i].toColor()), i, COLUMN_COLORS);
        }
    }

    void updateWindow() {
        panelColorForeground.setBackground(config.foreground.toColor());
        panelColorBackground.setBackground(config.background.toColor());
        panelColorGrid.setBackground(config.grid.toColor());
        comboGridX.setSelectedIndex(config.gridXon.ordinal());
        comboGridY.setSelectedIndex(config.gridYon.ordinal());
        comboColorY.setSelectedIndex(config.axisYcolorStat.ordinal());
        spinnerLineWidth.setValue(config.graphLineWidth);
        spinnerSampleInterval.setValue(config.sampleInterval);
        spinnerRedrawInterval.setValue(config.refreshInterval);
        spinnerBufferSize.setValue(config.numSamples);

        spinnerHour.setValue(config.timespan / 3600);
        spinnerMin.setValue(config.timespan / 60);
        spinnerSec.setValue(config.timespan % 60);

        if (modelSeries.getRowCount() != config.getNumberCurves()) {
            modelSeries.setRowCount(config.getNumberCurves());
        }
        for (int i = 0; i < config.getNumberCurves(); i++) {
            Curve c = config.curves[i];
            modelSeries.setValueAt(c.name, i, COLUMN_NAME);
            modelSeries.setValueAt((c.plotStatus == null) || (c.plotStatus), i, COLUMN_ENABLED);
            modelSeries.setValueAt(c.scale == Scale.logarithmic, i, COLUMN_LOG);
            modelSeries.setValueAt(c.precision, i, COLUMN_PREC);
            modelSeries.setValueAt(c.min, i, COLUMN_MIN);
            modelSeries.setValueAt(c.max, i, COLUMN_MAX);
            modelSeries.setValueAt(c.units == null ? "" : c.units, i, COLUMN_UNITS);
            modelSeries.setValueAt(c.comment == null ? "" : c.comment, i, COLUMN_DESC);
            modelSeries.setValueAt(Config.getStringFromColor(config.colors[i].toColor()), i, COLUMN_COLORS);
        }
        updateButtons();
    }

    /*
    void updateConfig() {
        config.foreground = panelColorForeground.getBackground() == null ? new DeepColor() : new DeepColor(panelColorForeground.getBackground());
        config.background = panelColorBackground.getBackground() == null ? new DeepColor() :new DeepColor(panelColorBackground.getBackground());
        config.grid = panelColorGrid.getBackground() == null ? new DeepColor() : new DeepColor(panelColorGrid.getBackground());
        config.gridXon = GridVisibility.values()[comboGridX.getSelectedIndex()];
        config.gridYon = GridVisibility.values()[comboGridY.getSelectedIndex()];
        config.axisYcolorStat = LabelColorAxisY.values()[comboColorY.getSelectedIndex()];
        config.sampleInterval = (Double) spinnerSampleInterval.getValue();
        config.refreshInterval = (Double) spinnerRedrawInterval.getValue();
        config.timespan = ((Integer) spinnerHour.getValue()) * 3600 + ((Integer) spinnerMin.getValue()) * 60 + ((Integer) spinnerSec.getValue());

        config.curves = new Curve[Config.MAX_NUMBER_PLOTS];
        for (int i = 0; i < modelSeries.getRowCount(); i++) {
            Curve c = new Curve();
            c.name = (String) modelSeries.getValueAt(i, COLUMN_NAME);
            c.plotStatus = (Boolean) modelSeries.getValueAt(i, COLUMN_ENABLED);
            c.scale = ((Boolean) modelSeries.getValueAt(i, COLUMN_LOG)) ? Scale.logarithmic : Scale.linear;
            c.precision = (Integer) modelSeries.getValueAt(i, COLUMN_PREC);
            c.min = (Double) modelSeries.getValueAt(i, COLUMN_MIN);
            c.max = (Double) modelSeries.getValueAt(i, COLUMN_MAX);
            c.units = (String) modelSeries.getValueAt(i, COLUMN_UNITS);
            c.comment = (String) modelSeries.getValueAt(i, COLUMN_DESC);
            config.curves[i] = c;
            config.colors[i] = new DeepColor(Config.getColorFromString((String) modelSeries.getValueAt(i, COLUMN_COLORS)));
        }
    }
     */
    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        jMenuBar1 = new javax.swing.JMenuBar();
        jMenu1 = new javax.swing.JMenu();
        jMenu2 = new javax.swing.JMenu();
        panelSeries = new javax.swing.JPanel();
        jScrollPane1 = new javax.swing.JScrollPane();
        tableSeries = new JTable() {
            @Override
            public boolean isCellEditable(int row, int column) {
                return isSeriesTableRowEditable(row, column);
            };
        };
        buttonDelete = new javax.swing.JButton();
        buttonUp = new javax.swing.JButton();
        buttonInsert = new javax.swing.JButton();
        buttonDown = new javax.swing.JButton();
        jPanel1 = new javax.swing.JPanel();
        jPanel2 = new javax.swing.JPanel();
        jLabel1 = new javax.swing.JLabel();
        jLabel2 = new javax.swing.JLabel();
        jLabel3 = new javax.swing.JLabel();
        jLabel4 = new javax.swing.JLabel();
        spinnerRedrawInterval = new javax.swing.JSpinner();
        spinnerBufferSize = new javax.swing.JSpinner();
        spinnerSampleInterval = new javax.swing.JSpinner();
        spinnerHour = new javax.swing.JSpinner();
        jLabel5 = new javax.swing.JLabel();
        spinnerMin = new javax.swing.JSpinner();
        jLabel6 = new javax.swing.JLabel();
        spinnerSec = new javax.swing.JSpinner();
        jPanel3 = new javax.swing.JPanel();
        panelColorGrid = new javax.swing.JPanel();
        jLabel17 = new javax.swing.JLabel();
        panelColorForeground = new javax.swing.JPanel();
        panelColorBackground = new javax.swing.JPanel();
        jLabel15 = new javax.swing.JLabel();
        jLabel18 = new javax.swing.JLabel();
        buttonSystemColors = new javax.swing.JButton();
        jLabel20 = new javax.swing.JLabel();
        comboColorY = new javax.swing.JComboBox<>();
        comboGridX = new javax.swing.JComboBox<>();
        jLabel21 = new javax.swing.JLabel();
        spinnerLineWidth = new javax.swing.JSpinner();
        jLabel19 = new javax.swing.JLabel();
        jLabel16 = new javax.swing.JLabel();
        comboGridY = new javax.swing.JComboBox<>();
        jMenuBar2 = new javax.swing.JMenuBar();
        menuFile = new javax.swing.JMenu();
        menuLoad = new javax.swing.JMenuItem();
        jSeparator1 = new javax.swing.JPopupMenu.Separator();
        menuSaveAs = new javax.swing.JMenuItem();
        menuSave = new javax.swing.JMenuItem();
        jSeparator2 = new javax.swing.JPopupMenu.Separator();
        menuClear = new javax.swing.JMenuItem();
        jSeparator3 = new javax.swing.JPopupMenu.Separator();
        menuExit = new javax.swing.JMenuItem();
        menuWindow = new javax.swing.JMenu();
        menuShowGraph = new javax.swing.JMenuItem();
        jSeparator4 = new javax.swing.JPopupMenu.Separator();
        menuHideConfig = new javax.swing.JMenuItem();
        menuHelp = new javax.swing.JMenu();
        menuAbout = new javax.swing.JMenuItem();

        jMenu1.setText("File");
        jMenuBar1.add(jMenu1);

        jMenu2.setText("Edit");
        jMenuBar1.add(jMenu2);

        panelSeries.setBorder(javax.swing.BorderFactory.createTitledBorder("Curves"));

        tableSeries.setModel(new javax.swing.table.DefaultTableModel(
            new Object [][] {

            },
            new String [] {
                "Name", "Enabled", "Log", "Precision", "Min", "Max", "Units", "Desc", "Color"
            }
        ) {
            Class[] types = new Class [] {
                java.lang.String.class, java.lang.Boolean.class, java.lang.Boolean.class, java.lang.Integer.class, java.lang.Double.class, java.lang.Double.class, java.lang.String.class, java.lang.String.class, java.lang.Object.class
            };
            boolean[] canEdit = new boolean [] {
                false, true, true, true, true, true, true, true, true
            };

            public Class getColumnClass(int columnIndex) {
                return types [columnIndex];
            }

            public boolean isCellEditable(int rowIndex, int columnIndex) {
                return canEdit [columnIndex];
            }
        });
        tableSeries.setSelectionMode(javax.swing.ListSelectionModel.SINGLE_SELECTION);
        tableSeries.getTableHeader().setReorderingAllowed(false);
        tableSeries.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseReleased(java.awt.event.MouseEvent evt) {
                tableSeriesMouseReleased(evt);
            }
        });
        tableSeries.addKeyListener(new java.awt.event.KeyAdapter() {
            public void keyReleased(java.awt.event.KeyEvent evt) {
                tableSeriesKeyReleased(evt);
            }
        });
        jScrollPane1.setViewportView(tableSeries);

        buttonDelete.setText("Delete");
        buttonDelete.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonDeleteActionPerformed(evt);
            }
        });

        buttonUp.setText("Move Up");
        buttonUp.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonUpActionPerformed(evt);
            }
        });

        buttonInsert.setText("Insert");
        buttonInsert.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonInsertActionPerformed(evt);
            }
        });

        buttonDown.setText("Move Down");
        buttonDown.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonDownActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout panelSeriesLayout = new javax.swing.GroupLayout(panelSeries);
        panelSeries.setLayout(panelSeriesLayout);
        panelSeriesLayout.setHorizontalGroup(
            panelSeriesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelSeriesLayout.createSequentialGroup()
                .addContainerGap()
                .addGroup(panelSeriesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jScrollPane1)
                    .addGroup(panelSeriesLayout.createSequentialGroup()
                        .addGap(0, 0, Short.MAX_VALUE)
                        .addComponent(buttonUp)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(buttonDown)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(buttonInsert)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(buttonDelete)
                        .addGap(0, 0, Short.MAX_VALUE)))
                .addContainerGap())
        );

        panelSeriesLayout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {buttonDelete, buttonDown, buttonInsert, buttonUp});

        panelSeriesLayout.setVerticalGroup(
            panelSeriesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(panelSeriesLayout.createSequentialGroup()
                .addGap(2, 2, 2)
                .addComponent(jScrollPane1, javax.swing.GroupLayout.DEFAULT_SIZE, 189, Short.MAX_VALUE)
                .addGap(4, 4, 4)
                .addGroup(panelSeriesLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(buttonDelete)
                    .addComponent(buttonInsert)
                    .addComponent(buttonDown)
                    .addComponent(buttonUp))
                .addContainerGap())
        );

        jPanel1.setBorder(javax.swing.BorderFactory.createTitledBorder("Controls"));

        jPanel2.setBorder(javax.swing.BorderFactory.createTitledBorder("Time Controls"));

        jLabel1.setText("Time Span:");

        jLabel2.setText("Buffer Size (samples):");

        jLabel3.setText("Redraw Interval (s):");

        jLabel4.setText("Sample Interval (s):");

        spinnerRedrawInterval.setModel(new javax.swing.SpinnerNumberModel(1.0d, 0.0d, 100.0d, 0.1d));
        spinnerRedrawInterval.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerRedrawIntervalStateChanged(evt);
            }
        });

        spinnerBufferSize.setModel(new javax.swing.SpinnerNumberModel(65535, 0, 1000000, 1));
        spinnerBufferSize.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerBufferSizeStateChanged(evt);
            }
        });

        spinnerSampleInterval.setModel(new javax.swing.SpinnerNumberModel(1.0d, 0.0d, 100.0d, 0.1d));
        spinnerSampleInterval.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerSampleIntervalStateChanged(evt);
            }
        });

        spinnerHour.setModel(new javax.swing.SpinnerNumberModel(0, 0, 99, 1));
        spinnerHour.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerHourStateChanged(evt);
            }
        });

        jLabel5.setText(":");

        spinnerMin.setModel(new javax.swing.SpinnerNumberModel(5, 0, 59, 1));
        spinnerMin.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerHourStateChanged(evt);
            }
        });

        jLabel6.setText(":");

        spinnerSec.setModel(new javax.swing.SpinnerNumberModel(0, 0, 59, 1));
        spinnerSec.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerHourStateChanged(evt);
            }
        });

        javax.swing.GroupLayout jPanel2Layout = new javax.swing.GroupLayout(jPanel2);
        jPanel2.setLayout(jPanel2Layout);
        jPanel2Layout.setHorizontalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING, false)
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(jLabel2)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(spinnerBufferSize))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(jLabel1)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(spinnerHour, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, 0)
                        .addComponent(jLabel5)
                        .addGap(0, 0, 0)
                        .addComponent(spinnerMin, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addGap(0, 0, 0)
                        .addComponent(jLabel6)
                        .addGap(0, 0, 0)
                        .addComponent(spinnerSec, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(jLabel3)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(spinnerRedrawInterval))
                    .addGroup(jPanel2Layout.createSequentialGroup()
                        .addComponent(jLabel4)
                        .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                        .addComponent(spinnerSampleInterval)))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel2Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {jLabel1, jLabel2, jLabel3, jLabel4});

        jPanel2Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {spinnerHour, spinnerMin, spinnerSec});

        jPanel2Layout.setVerticalGroup(
            jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel2Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(spinnerMin, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jLabel6)
                        .addComponent(spinnerSec, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                    .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                        .addComponent(jLabel1)
                        .addComponent(spinnerHour, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                        .addComponent(jLabel5)))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel2)
                    .addComponent(spinnerBufferSize, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel4, javax.swing.GroupLayout.PREFERRED_SIZE, 16, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(spinnerSampleInterval, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel2Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.BASELINE)
                    .addComponent(jLabel3)
                    .addComponent(spinnerRedrawInterval, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel3.setBorder(javax.swing.BorderFactory.createTitledBorder("Graph Options"));

        panelColorGrid.setBorder(new javax.swing.border.SoftBevelBorder(javax.swing.border.BevelBorder.RAISED));
        panelColorGrid.setPreferredSize(new java.awt.Dimension(44, 23));
        panelColorGrid.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                panelColorGridMouseClicked(evt);
            }
        });

        javax.swing.GroupLayout panelColorGridLayout = new javax.swing.GroupLayout(panelColorGrid);
        panelColorGrid.setLayout(panelColorGridLayout);
        panelColorGridLayout.setHorizontalGroup(
            panelColorGridLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 44, Short.MAX_VALUE)
        );
        panelColorGridLayout.setVerticalGroup(
            panelColorGridLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 20, Short.MAX_VALUE)
        );

        jLabel17.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel17.setText("Grid:");

        panelColorForeground.setBorder(new javax.swing.border.SoftBevelBorder(javax.swing.border.BevelBorder.RAISED));
        panelColorForeground.setPreferredSize(new java.awt.Dimension(44, 23));
        panelColorForeground.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                panelColorForegroundMouseClicked(evt);
            }
        });

        javax.swing.GroupLayout panelColorForegroundLayout = new javax.swing.GroupLayout(panelColorForeground);
        panelColorForeground.setLayout(panelColorForegroundLayout);
        panelColorForegroundLayout.setHorizontalGroup(
            panelColorForegroundLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 44, Short.MAX_VALUE)
        );
        panelColorForegroundLayout.setVerticalGroup(
            panelColorForegroundLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 20, Short.MAX_VALUE)
        );

        panelColorBackground.setBorder(new javax.swing.border.SoftBevelBorder(javax.swing.border.BevelBorder.RAISED));
        panelColorBackground.setPreferredSize(new java.awt.Dimension(44, 23));
        panelColorBackground.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mouseClicked(java.awt.event.MouseEvent evt) {
                panelColorBackgroundMouseClicked(evt);
            }
        });

        javax.swing.GroupLayout panelColorBackgroundLayout = new javax.swing.GroupLayout(panelColorBackground);
        panelColorBackground.setLayout(panelColorBackgroundLayout);
        panelColorBackgroundLayout.setHorizontalGroup(
            panelColorBackgroundLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 44, Short.MAX_VALUE)
        );
        panelColorBackgroundLayout.setVerticalGroup(
            panelColorBackgroundLayout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 20, Short.MAX_VALUE)
        );

        jLabel15.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel15.setText("Background:");

        jLabel18.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel18.setText("Foreground:");

        buttonSystemColors.setText("System Colors");
        buttonSystemColors.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                buttonSystemColorsActionPerformed(evt);
            }
        });

        jLabel20.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel20.setText("Y-grid lines:");

        comboColorY.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "Foreground", "Selected curve" }));
        comboColorY.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                comboColorYActionPerformed(evt);
            }
        });

        comboGridX.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "None", "Some", "All" }));
        comboGridX.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                comboGridXActionPerformed(evt);
            }
        });

        jLabel21.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel21.setText("Line width:");

        spinnerLineWidth.setModel(new javax.swing.SpinnerNumberModel(2, 0, 10, 1));
        spinnerLineWidth.addChangeListener(new javax.swing.event.ChangeListener() {
            public void stateChanged(javax.swing.event.ChangeEvent evt) {
                spinnerLineWidthStateChanged(evt);
            }
        });

        jLabel19.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel19.setText("X-grid lines:");

        jLabel16.setHorizontalAlignment(javax.swing.SwingConstants.TRAILING);
        jLabel16.setText("Y label color: ");

        comboGridY.setModel(new javax.swing.DefaultComboBoxModel<>(new String[] { "None", "Some", "All" }));
        comboGridY.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                comboGridYActionPerformed(evt);
            }
        });

        javax.swing.GroupLayout jPanel3Layout = new javax.swing.GroupLayout(jPanel3);
        jPanel3.setLayout(jPanel3Layout);
        jPanel3Layout.setHorizontalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(javax.swing.GroupLayout.Alignment.TRAILING, jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.TRAILING)
                        .addGroup(jPanel3Layout.createSequentialGroup()
                            .addComponent(jLabel17)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(panelColorGrid, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addGroup(jPanel3Layout.createSequentialGroup()
                            .addComponent(jLabel15)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(panelColorBackground, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE))
                        .addGroup(jPanel3Layout.createSequentialGroup()
                            .addComponent(jLabel18)
                            .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                            .addComponent(panelColorForeground, javax.swing.GroupLayout.PREFERRED_SIZE, 50, javax.swing.GroupLayout.PREFERRED_SIZE)))
                    .addComponent(buttonSystemColors))
                .addGap(18, 18, Short.MAX_VALUE)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.CENTER)
                    .addComponent(jLabel19)
                    .addComponent(jLabel20)
                    .addComponent(jLabel16)
                    .addComponent(jLabel21))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(spinnerLineWidth, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboColorY, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboGridY, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboGridX, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel3Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {jLabel15, jLabel17, jLabel18});

        jPanel3Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {panelColorBackground, panelColorForeground, panelColorGrid});

        jPanel3Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {jLabel16, jLabel19, jLabel20, jLabel21});

        jPanel3Layout.linkSize(javax.swing.SwingConstants.HORIZONTAL, new java.awt.Component[] {comboGridX, comboGridY});

        jPanel3Layout.setVerticalGroup(
            jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel3Layout.createSequentialGroup()
                .addContainerGap()
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.CENTER)
                    .addComponent(panelColorForeground, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel18, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboGridX, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel19, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.CENTER)
                    .addComponent(panelColorBackground, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel15, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel20, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboGridY, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.CENTER)
                    .addComponent(panelColorGrid, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel17, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(comboColorY, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(jLabel16, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addGroup(jPanel3Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.CENTER)
                    .addComponent(buttonSystemColors)
                    .addComponent(jLabel21, javax.swing.GroupLayout.PREFERRED_SIZE, 26, javax.swing.GroupLayout.PREFERRED_SIZE)
                    .addComponent(spinnerLineWidth, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
                .addContainerGap(javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        jPanel3Layout.linkSize(javax.swing.SwingConstants.VERTICAL, new java.awt.Component[] {panelColorBackground, panelColorForeground, panelColorGrid});

        javax.swing.GroupLayout jPanel1Layout = new javax.swing.GroupLayout(jPanel1);
        jPanel1.setLayout(jPanel1Layout);
        jPanel1Layout.setHorizontalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addContainerGap())
        );
        jPanel1Layout.setVerticalGroup(
            jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(jPanel1Layout.createSequentialGroup()
                .addGroup(jPanel1Layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
                    .addComponent(jPanel2, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                    .addComponent(jPanel3, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
                .addContainerGap())
        );

        menuFile.setText("File");

        menuLoad.setText("Load");
        menuLoad.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuLoadActionPerformed(evt);
            }
        });
        menuFile.add(menuLoad);
        menuFile.add(jSeparator1);

        menuSaveAs.setText("Save As...");
        menuSaveAs.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuSaveAsActionPerformed(evt);
            }
        });
        menuFile.add(menuSaveAs);

        menuSave.setText("Save");
        menuSave.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuSaveActionPerformed(evt);
            }
        });
        menuFile.add(menuSave);
        menuFile.add(jSeparator2);

        menuClear.setText("Clear");
        menuClear.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuClearActionPerformed(evt);
            }
        });
        menuFile.add(menuClear);
        menuFile.add(jSeparator3);

        menuExit.setText("Exit");
        menuExit.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuExitActionPerformed(evt);
            }
        });
        menuFile.add(menuExit);

        jMenuBar2.add(menuFile);

        menuWindow.setText("Window");

        menuShowGraph.setText("Show Graph Window");
        menuShowGraph.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuShowGraphActionPerformed(evt);
            }
        });
        menuWindow.add(menuShowGraph);
        menuWindow.add(jSeparator4);

        menuHideConfig.setText("Hide Config Window");
        menuHideConfig.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuHideConfigActionPerformed(evt);
            }
        });
        menuWindow.add(menuHideConfig);

        jMenuBar2.add(menuWindow);

        menuHelp.setText("Help");

        menuAbout.setText("About");
        menuAbout.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                menuAboutActionPerformed(evt);
            }
        });
        menuHelp.add(menuAbout);

        jMenuBar2.add(menuHelp);

        setJMenuBar(jMenuBar2);

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(panelSeries, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
            .addGroup(layout.createSequentialGroup()
                .addContainerGap()
                .addComponent(jPanel1, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGroup(layout.createSequentialGroup()
                .addComponent(panelSeries, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
                .addPreferredGap(javax.swing.LayoutStyle.ComponentPlacement.RELATED)
                .addComponent(jPanel1, javax.swing.GroupLayout.PREFERRED_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.PREFERRED_SIZE))
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void tableSeriesMouseReleased(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_tableSeriesMouseReleased
        updateButtons();
    }//GEN-LAST:event_tableSeriesMouseReleased

    private void tableSeriesKeyReleased(java.awt.event.KeyEvent evt) {//GEN-FIRST:event_tableSeriesKeyReleased
        updateButtons();
    }//GEN-LAST:event_tableSeriesKeyReleased

    private void buttonDeleteActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonDeleteActionPerformed
        if (modelSeries.getRowCount() > 0) {
            int index = tableSeries.getSelectedRow();
            modelSeries.removeRow(Math.max(index, 0));
            App.plotFrame.removeChannel(index);
            updateButtons();
            updateColors();
        }
    }//GEN-LAST:event_buttonDeleteActionPerformed

    private void buttonUpActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonUpActionPerformed
        try {
            int rows = modelSeries.getRowCount();
            int cur = tableSeries.getSelectedRow();
            App.plotFrame.stop();
            //modelSeries.moveRow(cur, cur, cur - 1);
            Curve curve = config.curves[cur - 1];
            config.curves[cur - 1] = config.curves[cur];
            config.curves[cur] = curve;

            tableSeries.setRowSelectionInterval(cur - 1, cur - 1);
            updateButtons();
            updateColors();

            updateWindow();
            App.plotFrame.start();

        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonUpActionPerformed

    private void buttonInsertActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonInsertActionPerformed
        if (modelSeries.getRowCount() < Config.MAX_NUMBER_PLOTS) {
            String channelName = SwingUtils.getString(this, "Enter channel name", "");
            if ((channelName != null) && !(channelName.trim().isEmpty())) {
                channelName = channelName.trim();

                try (Context context = new Context(App.getCaProperties())) {
                    Channel<Double> channel = context.createChannel(channelName, Double.class);
                    channel.connectAsync().get(2, TimeUnit.SECONDS);
                    Graphic g = channel.get(Graphic.class);
                    String units = g.getUnits() == null ? "" : g.getUnits();
                    Integer precision = ((Number) g.getPrecision()).intValue();
                    Double min = ((Number) g.getLowerDisplay()).doubleValue();
                    Double max = ((Number) g.getUpperDisplay()).doubleValue();
                    if (max <= min) {
                        max = min + 1;
                    }

                    String desc = "";
                    try {
                        Channel<String> channelDesc = context.createChannel(channelName + ".DESC", String.class);
                        channelDesc.connectAsync().get(2, TimeUnit.SECONDS);
                        desc = channelDesc.get();
                    } catch (Exception ex) {
                    }

                    Object[] data = new Object[]{channelName, Boolean.TRUE, Boolean.FALSE,
                        precision, min, max, units, desc,
                        Config.getStringFromColor(config.colors[modelSeries.getRowCount()].toColor())};
                    int index = modelSeries.getRowCount();
                    //if (tableSeries.getSelectedRow() >= 0) {
                    //    index = tableSeries.getSelectedRow() + 1;
                    //    modelSeries.insertRow(tableSeries.getSelectedRow() + 1, data);
                    //} else {
                    modelSeries.addRow(data);
                    //}
                    modelSeries.fireTableDataChanged();
                    updateButtons();
                    updateColors();
                    App.plotFrame.addChannel(index, channelName, true, false, precision, min, max, units, desc);
                } catch (Exception ex) {
                    SwingUtils.showException(this, ex);
                }
            }
        }
    }//GEN-LAST:event_buttonInsertActionPerformed

    private void buttonDownActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonDownActionPerformed
        try {
            int rows = modelSeries.getRowCount();
            int cur = tableSeries.getSelectedRow();

            updateButtons();

            App.plotFrame.stop();
            //modelSeries.moveRow(cur, cur, cur + 1);
            Curve curve = config.curves[cur + 1];
            config.curves[cur + 1] = config.curves[cur];
            config.curves[cur] = curve;

            tableSeries.setRowSelectionInterval(cur + 1, cur + 1);
            updateButtons();
            updateColors();

            updateWindow();
            App.plotFrame.start();
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonDownActionPerformed

    private void panelColorBackgroundMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_panelColorBackgroundMouseClicked
        Color c = JColorChooser.showDialog(this, "Choose a Color", getConfig().background.toColor());
        if (c != null) {
            panelColorBackground.setBackground(c);
            App.plotFrame.setBackgroundColor(c);
        }
    }//GEN-LAST:event_panelColorBackgroundMouseClicked

    private void panelColorGridMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_panelColorGridMouseClicked
        Color c = JColorChooser.showDialog(this, "Choose a Color", getConfig().grid.toColor());
        if (c != null) {
            panelColorGrid.setBackground(c);
            App.plotFrame.setGridColor(c);
        }
    }//GEN-LAST:event_panelColorGridMouseClicked

    private void panelColorForegroundMouseClicked(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_panelColorForegroundMouseClicked
        Color c = JColorChooser.showDialog(this, "Choose a Color", getConfig().foreground.toColor());
        if (c != null) {
            panelColorForeground.setBackground(c);
            App.plotFrame.setForegroundColor(c);
        }
    }//GEN-LAST:event_panelColorForegroundMouseClicked

    private void menuSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuSaveActionPerformed
        if (file == null) {
            menuSaveAsActionPerformed(null);

        } else {
            try {
                save(file);
            } catch (Exception ex) {
                SwingUtils.showException(this, ex);
            }
        }
    }//GEN-LAST:event_menuSaveActionPerformed

    private void menuClearActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuClearActionPerformed
        clear();
    }//GEN-LAST:event_menuClearActionPerformed

    private void menuHideConfigActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuHideConfigActionPerformed
        App.plotFrame.setVisible(true);
        setVisible(false);
    }//GEN-LAST:event_menuHideConfigActionPerformed

    private void menuLoadActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuLoadActionPerformed
        try {
            JFileChooser chooser = new JFileChooser(App.getHome());
            FileNameExtensionFilter filter = new FileNameExtensionFilter("Striptool files", Config.EXTENSION);
            chooser.setFileFilter(filter);
            int rVal = chooser.showOpenDialog(this);
            if (rVal == JFileChooser.APPROVE_OPTION) {
                open(chooser.getSelectedFile());
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_menuLoadActionPerformed

    private void menuSaveAsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuSaveAsActionPerformed
        try {
            JFileChooser chooser = new JFileChooser(App.getHome());
            FileNameExtensionFilter filter = new FileNameExtensionFilter("Striptool files", Config.EXTENSION);
            chooser.setFileFilter(filter);
            int rVal = chooser.showSaveDialog(this);
            if (rVal == JFileChooser.APPROVE_OPTION) {
                File file = chooser.getSelectedFile();
                if (!file.getName().contains(".")) {
                    file = new File(file.getPath() + "." + Config.EXTENSION);
                }
                save(file);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_menuSaveAsActionPerformed

    private void menuExitActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuExitActionPerformed
        System.exit(0);
    }//GEN-LAST:event_menuExitActionPerformed

    private void comboColorYActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboColorYActionPerformed
        try {
            if (App.plotFrame != null) {
                App.plotFrame.setLabelColorAxisY(LabelColorAxisY.values()[comboColorY.getSelectedIndex()]);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_comboColorYActionPerformed

    private void buttonSystemColorsActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_buttonSystemColorsActionPerformed
        try {
            panelColorForeground.setBackground(null);
            App.plotFrame.setForegroundColor(null);
            panelColorBackground.setBackground(null);
            App.plotFrame.setBackgroundColor(null);
            panelColorGrid.setBackground(null);
            App.plotFrame.setGridColor(null);
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_buttonSystemColorsActionPerformed

    private void spinnerLineWidthStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_spinnerLineWidthStateChanged
        try {
            App.plotFrame.setLineWidth((Integer) spinnerLineWidth.getValue());
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }

    }//GEN-LAST:event_spinnerLineWidthStateChanged

    private void comboGridXActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboGridXActionPerformed
        try {
            if (App.plotFrame != null) {
                App.plotFrame.setGridX(GridVisibility.values()[comboGridX.getSelectedIndex()]);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_comboGridXActionPerformed

    private void comboGridYActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_comboGridYActionPerformed
        try {
            if (App.plotFrame != null) {
                App.plotFrame.setGridY(GridVisibility.values()[comboGridY.getSelectedIndex()]);
            }
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_comboGridYActionPerformed

    private void spinnerRedrawIntervalStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_spinnerRedrawIntervalStateChanged
        try {
            App.plotFrame.setRedrawInterval((Double) spinnerRedrawInterval.getValue());
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_spinnerRedrawIntervalStateChanged

    private void spinnerSampleIntervalStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_spinnerSampleIntervalStateChanged
        try {
            App.plotFrame.setSampleInterval((Double) spinnerSampleInterval.getValue());
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_spinnerSampleIntervalStateChanged

    private void spinnerBufferSizeStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_spinnerBufferSizeStateChanged
        try {
            App.plotFrame.setNumSamples((Integer) spinnerBufferSize.getValue());
        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_spinnerBufferSizeStateChanged

    private void spinnerHourStateChanged(javax.swing.event.ChangeEvent evt) {//GEN-FIRST:event_spinnerHourStateChanged
        try {
            int timespan = (Integer) spinnerSec.getValue();
            timespan += ((Integer) spinnerMin.getValue()) * 60;
            timespan += ((Integer) spinnerHour.getValue()) * 3600;
            App.plotFrame.setTimespan(timespan);

        } catch (Exception ex) {
            SwingUtils.showException(this, ex);
        }
    }//GEN-LAST:event_spinnerHourStateChanged

    private void menuShowGraphActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuShowGraphActionPerformed
        App.plotFrame.setVisible(true);
    }//GEN-LAST:event_menuShowGraphActionPerformed

    private void menuAboutActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuAboutActionPerformed
        DialogAbout aboutDialog = new DialogAbout(this, true);
        aboutDialog.setDefaultCloseOperation(javax.swing.WindowConstants.DISPOSE_ON_CLOSE);
        aboutDialog.setLocationRelativeTo(this);
        aboutDialog.setVisible(true);
    }//GEN-LAST:event_menuAboutActionPerformed

    /**
     * @param args the command line arguments
     */
    public static void main(String args[]) {
        /* Set the Nimbus look and feel */
        //<editor-fold defaultstate="collapsed" desc=" Look and feel setting code (optional) ">
        /* If Nimbus (introduced in Java SE 6) is not available, stay with the default look and feel.
         * For details see http://download.oracle.com/javase/tutorial/uiswing/lookandfeel/plaf.html 
         */
        try {
            for (javax.swing.UIManager.LookAndFeelInfo info : javax.swing.UIManager.getInstalledLookAndFeels()) {
                if ("Nimbus".equals(info.getName())) {
                    javax.swing.UIManager.setLookAndFeel(info.getClassName());
                    break;
                }
            }
        } catch (ClassNotFoundException ex) {
            java.util.logging.Logger.getLogger(ConfigFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (InstantiationException ex) {
            java.util.logging.Logger.getLogger(ConfigFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (IllegalAccessException ex) {
            java.util.logging.Logger.getLogger(ConfigFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        } catch (javax.swing.UnsupportedLookAndFeelException ex) {
            java.util.logging.Logger.getLogger(ConfigFrame.class.getName()).log(java.util.logging.Level.SEVERE, null, ex);
        }
        //</editor-fold>

        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new ConfigFrame().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JButton buttonDelete;
    private javax.swing.JButton buttonDown;
    private javax.swing.JButton buttonInsert;
    private javax.swing.JButton buttonSystemColors;
    private javax.swing.JButton buttonUp;
    private javax.swing.JComboBox<String> comboColorY;
    private javax.swing.JComboBox<String> comboGridX;
    private javax.swing.JComboBox<String> comboGridY;
    private javax.swing.JLabel jLabel1;
    private javax.swing.JLabel jLabel15;
    private javax.swing.JLabel jLabel16;
    private javax.swing.JLabel jLabel17;
    private javax.swing.JLabel jLabel18;
    private javax.swing.JLabel jLabel19;
    private javax.swing.JLabel jLabel2;
    private javax.swing.JLabel jLabel20;
    private javax.swing.JLabel jLabel21;
    private javax.swing.JLabel jLabel3;
    private javax.swing.JLabel jLabel4;
    private javax.swing.JLabel jLabel5;
    private javax.swing.JLabel jLabel6;
    private javax.swing.JMenu jMenu1;
    private javax.swing.JMenu jMenu2;
    private javax.swing.JMenuBar jMenuBar1;
    private javax.swing.JMenuBar jMenuBar2;
    private javax.swing.JPanel jPanel1;
    private javax.swing.JPanel jPanel2;
    private javax.swing.JPanel jPanel3;
    private javax.swing.JScrollPane jScrollPane1;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JPopupMenu.Separator jSeparator3;
    private javax.swing.JPopupMenu.Separator jSeparator4;
    private javax.swing.JMenuItem menuAbout;
    private javax.swing.JMenuItem menuClear;
    private javax.swing.JMenuItem menuExit;
    private javax.swing.JMenu menuFile;
    private javax.swing.JMenu menuHelp;
    private javax.swing.JMenuItem menuHideConfig;
    private javax.swing.JMenuItem menuLoad;
    private javax.swing.JMenuItem menuSave;
    private javax.swing.JMenuItem menuSaveAs;
    private javax.swing.JMenuItem menuShowGraph;
    private javax.swing.JMenu menuWindow;
    private javax.swing.JPanel panelColorBackground;
    private javax.swing.JPanel panelColorForeground;
    private javax.swing.JPanel panelColorGrid;
    private javax.swing.JPanel panelSeries;
    private javax.swing.JSpinner spinnerBufferSize;
    private javax.swing.JSpinner spinnerHour;
    private javax.swing.JSpinner spinnerLineWidth;
    private javax.swing.JSpinner spinnerMin;
    private javax.swing.JSpinner spinnerRedrawInterval;
    private javax.swing.JSpinner spinnerSampleInterval;
    private javax.swing.JSpinner spinnerSec;
    private javax.swing.JTable tableSeries;
    // End of variables declaration//GEN-END:variables
}
