package ch.psi.jstriptool;

import java.awt.BorderLayout;
import java.awt.Component;
import java.util.Arrays;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 *
 */
public class SeriesSetPanel extends javax.swing.JPanel {
    static SeriesPanel active;
    JScrollPane scrollPane;
    JPanel scrollPanel;
    
    public SeriesSetPanel() {
        initComponents();
        
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
        
    
    public void update() {

        if (isShowing()) {
            for (Component c : getSeriesPanels()) {
                try {
                    ((SeriesPanel) c).update();
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
