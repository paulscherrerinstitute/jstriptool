package ch.psi.jstriptool;

import java.awt.Component;
import java.util.Arrays;

/**
 *
 */
public class SeriesSetPanel extends javax.swing.JPanel {
    static SeriesPanel active;
    
    public SeriesSetPanel() {
        initComponents();
        
    }

    public void addSeries(PlotSeries series){
        if (getComponents().length == 0){
            setLayout(new javax.swing.BoxLayout(this, javax.swing.BoxLayout.PAGE_AXIS));
        }
        add(new SeriesPanel(series));
        updateUI();
    }
    
    public void removeSeries(int index){
        remove(index);
        updateUI();
    }    
    
    
    public void updateSeries(int index){
        getSeriesPanels()[index].initialize();        
    }
    
    public void setSeriesActive(PlotSeries series){          
        SeriesPanel panel = getSeriesPanels(series);
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
            try {
                for (Component c : getComponents()) {
                    ((SeriesPanel) c).update();
                }
            } catch (Exception ex) {
            }
        }
    }
    
    public void clear(){
        removeAll();
        active = null;
    }
    
    SeriesPanel[] getSeriesPanels() {
        return Arrays.asList(getComponents()).toArray(new SeriesPanel[0]);
    }

    SeriesPanel getSeriesPanels(PlotSeries series) {
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
