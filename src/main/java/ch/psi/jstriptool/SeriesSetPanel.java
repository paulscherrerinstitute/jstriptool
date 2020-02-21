package ch.psi.jstriptool;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.FontMetrics;
import java.awt.event.ComponentAdapter;
import java.awt.event.ComponentEvent;
import java.awt.font.LineMetrics;
import java.util.ArrayList;
import java.util.Arrays;
import javax.swing.JPanel;
import javax.swing.JScrollPane;

/**
 *
 */
public class SeriesSetPanel extends javax.swing.JPanel {
    final boolean ADAPT_SERIES_PANEL_LINES = true;
    
    static SeriesPanel active;
    JScrollPane scrollPane;
    JPanel scrollPanel;
    int visiblePanels;
    int lineHeight;
    int borderHeight;
    
    public SeriesSetPanel() {
        initComponents();
        if (ADAPT_SERIES_PANEL_LINES){
            this.addComponentListener(new ComponentAdapter() {
                @Override
                public void componentResized(ComponentEvent e) {
                    verifySeriesPanelsVisibleRows();
                }
            });        
        }
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
        
        if (borderHeight==0){
            SeriesPanel sp = (SeriesPanel) scrollPanel.getComponents()[0];
            borderHeight = sp.getInsets().bottom + sp.getInsets().top;            
            Font f = sp.getFont();
            FontMetrics fm = sp .getGraphics().getFontMetrics(f);
            LineMetrics km =f.getLineMetrics("<html><div style='text-align: center;'>1.0</div></html>", fm.getFontRenderContext());
            float height = km.getHeight();                
            lineHeight = Math.round(height) +1;                                
        }
        
        visiblePanels = getVisiblePanels().length;
        if (ADAPT_SERIES_PANEL_LINES){
            verifySeriesPanelsVisibleRows();
        }
    }
    
    public void removeSeries(int index){
        scrollPanel.remove(index);
        updateUI();
        visiblePanels = getVisiblePanels().length;
        if (ADAPT_SERIES_PANEL_LINES){
            verifySeriesPanelsVisibleRows();
        }        
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
        if (visiblePanels>0){
            int seriesAvailableHeight = getHeight()/visiblePanels - borderHeight;   
            int rows = seriesAvailableHeight/lineHeight;
            setSeriesPanelsVisibleRows(rows);                                
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

    SeriesPanel[] getVisiblePanels() {
        ArrayList<SeriesPanel> ret = new ArrayList<>();
        for (SeriesPanel pn : getSeriesPanels()){
            if (pn.isVisible()){
                ret.add(pn);
            }
        }
        return ret.toArray(new SeriesPanel[0]);
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
