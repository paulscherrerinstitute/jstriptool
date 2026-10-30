package ch.psi.jstriptool;

import ch.psi.jstriptool.App.LookAndFeel;
import ch.psi.jstriptool.App.Protocol;
import ch.psi.jstriptool.App.TimeMode;
import java.awt.Component;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;
import java.util.logging.Level;
import javax.swing.JFileChooser;
import javax.swing.JMenuItem;
import javax.swing.JRadioButtonMenuItem;
import javax.swing.filechooser.FileNameExtensionFilter;

/**
 *
 */
public class ConfigFrame extends javax.swing.JFrame {
    /**
     * Creates new form ConfigFrame
     */
    public ConfigFrame() {
        initComponents();        
        for (Protocol p: Protocol.values()){
            var menu = new JRadioButtonMenuItem(p.toString().toLowerCase());
            menu.addActionListener((e)->{
                try{
                    var protocol = Protocol.valueOf(((JMenuItem)e.getSource()).getText().toUpperCase());
                    if (protocol !=  App.defaultProtocol){
                        App.defaultProtocol = protocol;
                        if (App.plotFrame.isStarted()){
                            App.plotFrame.start();
                        }
                        
                    }
                } catch (Exception ex){
                    Logger.getLogger(ConfigFrame.class.getName()).log(Level.SEVERE, null, ex);
                }
             });            
            menuDefaultProtocol.add(menu);
        }                        
        
        for (TimeMode tm: TimeMode.values()){
            var menu = new JRadioButtonMenuItem(tm.toString().toLowerCase());
            menu.addActionListener((e)->{
                try{
                    var mode = TimeMode.valueOf(((JMenuItem)e.getSource()).getText().toUpperCase());
                    if (mode !=  App.timeMode){
                        App.timeMode = mode;
                        if (App.plotFrame.isStarted()){
                            App.plotFrame.start();
                        }
                    }
                } catch (Exception ex){
                    Logger.getLogger(ConfigFrame.class.getName()).log(Level.SEVERE, null, ex);
                }
             });            
            menuTimeMode.add(menu);
        }          
        
        for (Level level: new Level[]{Level.OFF, Level.SEVERE, Level.WARNING, Level.INFO, Level.FINE}){
            var menu = new JRadioButtonMenuItem(level.toString().toLowerCase());
            menu.addActionListener((e)->{
                try{
                    var logLevel = Level.parse(((JMenuItem)e.getSource()).getText().toUpperCase());
                    if (logLevel !=  App.logLevel){
                        App.setLogLevel(logLevel);
                    }
                } catch (Exception ex){
                    Logger.getLogger(ConfigFrame.class.getName()).log(Level.SEVERE, null, ex);
                }
             });            
            menuLogLevel.add(menu);
        }            
        
        for (LookAndFeel laf : LookAndFeel.values()) {
            var menu = new JRadioButtonMenuItem(laf.toString().toLowerCase());
            menu.addActionListener((e)->{
                try{
                    var lookAndFeel = ((JMenuItem)e.getSource()).getText();
                    App.setLookAndFeel(lookAndFeel);
                    App.plotFrame.getPlotPanel().updateLafColors();
                    App.plotFrame.updateAxisColor();
                    SwingUtils.updateAllFrames();
                } catch (Exception ex){
                    Logger.getLogger(ConfigFrame.class.getName()).log(Level.SEVERE, null, ex);
                }
             });            
            menuLookAndFeel.add(menu);
        }        
                        
        menuAntiAliasing.addActionListener((e)->{
            App.plotFrame.getPlotPanel().setAntiAliasing(menuAntiAliasing.isSelected());
        });
        
        menuMarkers.addActionListener((e)->{
            App.plotFrame.getPlotPanel().setMarkersVisible(menuMarkers.isSelected());
        });
        
        menuHideInvalid.addActionListener((e)->{
            ChannelValue.hideInvalid = menuHideInvalid.isSelected();
        });
        
        menuProperties.addChangeListener((e)->{
            if (menuProperties.isSelected()) {        
                menuAntiAliasing.setSelected(App.plotFrame.getPlotPanel().isAntiAlias());
                menuMarkers.setSelected(App.plotFrame.getPlotPanel().isMarkersVisible());
                menuHideInvalid.setSelected(ChannelValue.hideInvalid);
                for (var item : menuDefaultProtocol.getMenuComponents()){                
                    if (item instanceof JMenuItem menuItem){
                      menuItem.setSelected(menuItem.getText().equalsIgnoreCase(App.defaultProtocol.toString()));
                    }
                }                
                for (var item : menuTimeMode.getMenuComponents()){                
                    if (item instanceof JMenuItem menuItem){
                      menuItem.setSelected(menuItem.getText().equalsIgnoreCase(App.timeMode.toString()));
                    }
                }
                for (var item : menuLogLevel.getMenuComponents()){                
                    if (item instanceof JMenuItem menuItem){
                      menuItem.setSelected(menuItem.getText().equalsIgnoreCase(App.logLevel.toString()));
                    }
                }      
                
                for (Component item : menuLookAndFeel.getMenuComponents()) {            
                    if (item instanceof JRadioButtonMenuItem radioButtonMenuItem){
                        radioButtonMenuItem.setSelected(radioButtonMenuItem.getText().equalsIgnoreCase(App.lookAndFeel));
                    }
                }
                
            }
        });        
        
        clear();

    }

    void clear() {
        setTitle("JStripTool");
        configPanel.clear();
    }    

    void open(File file) throws IOException {
        configPanel.open(file);
        this.setTitle(file.getName());
        showPlotFrame();
    }

    void save(File file) throws IOException {
        configPanel.save(file);
        this.setTitle(file.getName());
        App.plotFrame.setTitle(getTitle());
    }

    void load(String[] channels) throws Exception {
        configPanel.load(channels);
        this.setTitle("Channels");
        showPlotFrame();
    }

    void showPlotFrame() {
        App.plotFrame.setVisible(true);
        App.plotFrame.setTitle(getTitle());
        App.plotFrame.setVisible(true);
        App.plotFrame.start();
    }
    
    public Config getConfig(){
        return configPanel.getConfig();
    }

    public ConfigPanel getConfigPanel(){
        return configPanel;
    }    
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
        configPanel = new ch.psi.jstriptool.ConfigPanel();
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
        menuProperties = new javax.swing.JMenu();
        menuDefaultProtocol = new javax.swing.JMenu();
        menuTimeMode = new javax.swing.JMenu();
        menuLogLevel = new javax.swing.JMenu();
        menuLookAndFeel = new javax.swing.JMenu();
        menuAntiAliasing = new javax.swing.JCheckBoxMenuItem();
        menuMarkers = new javax.swing.JCheckBoxMenuItem();
        menuHideInvalid = new javax.swing.JCheckBoxMenuItem();
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

        menuProperties.setText("Properties");

        menuDefaultProtocol.setText("Default Protocol");
        menuProperties.add(menuDefaultProtocol);

        menuTimeMode.setText("Time Mode");
        menuProperties.add(menuTimeMode);

        menuLogLevel.setText("Log Level");
        menuProperties.add(menuLogLevel);

        menuLookAndFeel.setText("Look & Feel");
        menuProperties.add(menuLookAndFeel);

        menuAntiAliasing.setSelected(true);
        menuAntiAliasing.setText("Anti-aliasing");
        menuProperties.add(menuAntiAliasing);

        menuMarkers.setSelected(true);
        menuMarkers.setText("Show Markers");
        menuProperties.add(menuMarkers);

        menuHideInvalid.setSelected(true);
        menuHideInvalid.setText("Hide Invalid Values");
        menuProperties.add(menuHideInvalid);

        jMenuBar2.add(menuProperties);

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
            .addComponent(configPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 767, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(configPanel, javax.swing.GroupLayout.DEFAULT_SIZE, 442, Short.MAX_VALUE)
        );

        pack();
    }// </editor-fold>//GEN-END:initComponents

    private void menuSaveActionPerformed(java.awt.event.ActionEvent evt) {//GEN-FIRST:event_menuSaveActionPerformed
        File file = configPanel.getFile();
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
        /* Create and display the form */
        java.awt.EventQueue.invokeLater(new Runnable() {
            public void run() {
                new ConfigFrame().setVisible(true);
            }
        });
    }

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private ch.psi.jstriptool.ConfigPanel configPanel;
    private javax.swing.JMenu jMenu1;
    private javax.swing.JMenu jMenu2;
    private javax.swing.JMenuBar jMenuBar1;
    private javax.swing.JMenuBar jMenuBar2;
    private javax.swing.JPopupMenu.Separator jSeparator1;
    private javax.swing.JPopupMenu.Separator jSeparator2;
    private javax.swing.JPopupMenu.Separator jSeparator3;
    private javax.swing.JPopupMenu.Separator jSeparator4;
    private javax.swing.JMenuItem menuAbout;
    private javax.swing.JCheckBoxMenuItem menuAntiAliasing;
    private javax.swing.JMenuItem menuClear;
    private javax.swing.JMenu menuDefaultProtocol;
    private javax.swing.JMenuItem menuExit;
    private javax.swing.JMenu menuFile;
    private javax.swing.JMenu menuHelp;
    private javax.swing.JMenuItem menuHideConfig;
    private javax.swing.JCheckBoxMenuItem menuHideInvalid;
    private javax.swing.JMenuItem menuLoad;
    private javax.swing.JMenu menuLogLevel;
    private javax.swing.JMenu menuLookAndFeel;
    private javax.swing.JCheckBoxMenuItem menuMarkers;
    private javax.swing.JMenu menuProperties;
    private javax.swing.JMenuItem menuSave;
    private javax.swing.JMenuItem menuSaveAs;
    private javax.swing.JMenuItem menuShowGraph;
    private javax.swing.JMenu menuTimeMode;
    private javax.swing.JMenu menuWindow;
    // End of variables declaration//GEN-END:variables
}
