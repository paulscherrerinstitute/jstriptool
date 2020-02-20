package ch.psi.jstriptool;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Insets;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.event.ComponentListener;
import java.util.Arrays;
import javax.swing.JComponent;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.Border;

/**
 *
 */
public class SeriesSetPanel extends javax.swing.JPanel {
    static SeriesPanel active;
    JScrollPane scrollPane;
    JPanel scrollPanel;
    
    public SeriesSetPanel() {
        initComponents();
        this.addComponentListener(new ComponentAdapter() {
            @Override
            public void componentResized(ComponentEvent e) {
                verifySeriesPanelsVisibleRows();
            }
        });        
    }
    
   
    public void addSeries(PlotSeries series){
        if (getComponents().length == 0){
            scrollPane = new javax.swing.JScrollPane(JScrollPane.VERTICAL_SCROLLBAR_AS_NEEDED,JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);
            scrollPanel = new JPanel();
            scrollPane.setBorder(null);
            scrollPane.setViewportView(scrollPanel);
            setLayout(new BorderLayout());
            add(scrollPane);
            scrollPanel.setLayout(new javax.swing.BoxLayout(scrollPanel, javax.swing.BoxLayout.PAGE_AXIS));            
        }
        scrollPanel.add(new SeriesPanel(series));
        updateUI();
        verifySeriesPanelsVisibleRows();
    }
    
    public void removeSeries(int index){
        scrollPanel.remove(index);
        updateUI();
    }    
    
    
    public void updateSeries(int index){
        getSeriesPanels()[index].initialize();        
    }
    
    public void setSeriesActive(PlotSeries series){          
        SeriesPanel panel = SeriesSetPanel.this.getSeriesPanel(series);
        if (panel != null) {
            panel.setActive();
        }    
        repaint();
    }
    
    public PlotSeries getActiveSeries() {
        return (active != null) ? active.getSeries() : null;
    }        
        
    void verifySeriesPanelsVisibleRows(){        
        int panels = scrollPanel.getComponentCount();
        if (panels>0){
            int lineHeight = 20;
            int borderHeight = 18;            
            int seriesAvailableHeight = getHeight()/panels - borderHeight;   
            setSeriesPanelsVisibleRows(seriesAvailableHeight/lineHeight);
        }
    }
    
    int visibleRows=3;
    public void setSeriesPanelsVisibleRows(int rows) {
        rows = Math.max(Math.min(rows, 3), 0);
        if ((visibleRows!=rows) && (visibleRows>=0) && (visibleRows<=3)){
            visibleRows = rows;
            for (SeriesPanel sp : getSeriesPanels()) {
                try {
                    sp.setVisibleRows(rows);
                } catch (Exception ex) {
                }                    
            }
        }
    }
    
    public void update() {
        if (isShowing()) {
            for (SeriesPanel sp : getSeriesPanels()) {
                try {
                    sp.update();
                } catch (Exception ex) {
                }                    
            }
        }
    }
    
    public void clear(){
        removeAll();
        active = null;
    }
    
    SeriesPanel[] getSeriesPanels() {
        return Arrays.asList(scrollPanel.getComponents()).toArray(new SeriesPanel[0]);
    }

    SeriesPanel getSeriesPanel(PlotSeries series) {
        for (SeriesPanel panel : getSeriesPanels()) {
            if (panel.getSeries() == series) {
                return panel;
            }
        }
        return null;
    }    

    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 269, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addGap(0, 353, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents


    // Variables declaration - do not modify//GEN-BEGIN:variables
    // End of variables declaration//GEN-END:variables
}
