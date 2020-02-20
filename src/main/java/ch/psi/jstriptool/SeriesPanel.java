package ch.psi.jstriptool;

import java.awt.Color;
import java.awt.Font;
import javax.swing.BorderFactory;
import static javax.swing.border.TitledBorder.DEFAULT_POSITION;
import static javax.swing.border.TitledBorder.LEADING;

/**
 *
 */
public class SeriesPanel extends javax.swing.JPanel {

    final PlotSeries series;
    final Color textBackground;
    final Color panelBackground;
    String currentDisplayValue;

    public SeriesPanel(PlotSeries series) {
        initComponents();
        this.series = series;
        textBackground = textInfo.getBackground();
        panelBackground = getBackground();
        initialize();
        Font f = textInfo.getFont().deriveFont(11.0f);
        textInfo.setFont(f);
    }
    String range;
    String desc;
    String name;

    public PlotSeries getSeries(){
        return series;
    }
    
    public void updateBorder() {
        if (this == SeriesSetPanel.active) {
            setBorder(BorderFactory.createTitledBorder(BorderFactory.createLineBorder(series.getColor(), 2), name, LEADING, DEFAULT_POSITION, null, series.getColor()));
        } else {
            setBorder(BorderFactory.createTitledBorder(null, name, LEADING, DEFAULT_POSITION, null, series.getColor()));
        }
    }

    public void initialize() {
        name = series.getName();
        if ((name == null) || (name.trim().isEmpty())){
            setVisible(false);
        } else  {
            setVisible(true);
            if (series.isFixedRange()) {
                range = String.format(("(%s, %s)"),
                        getDisplayRange(series.getRangeMin()),
                        getDisplayRange(series.getRangeMax()));
            } else {
                range = "";
            }
            desc = series.desc == null ? "" : series.desc;
            if (desc.isEmpty()){
                 desc = Config.getChannelDesc(App.plotFrame.context, name);
            }
            textInfo.setForeground(series.getColor());
            setToolTipText(name);
            updateBorder();   
            update();
        }
    }

    public void setActive() {
        if (!isActive()) {
            SeriesPanel former = SeriesSetPanel.active;
            SeriesSetPanel.active = this;
            if ((former != null) && former.isShowing()) {
                former.updateBorder();
            }
            updateBorder();
        }
    }
    
    
    public boolean isActive(){
        return (this == SeriesSetPanel.active);
    }    

    public void update() {
        try {
            String displayValue;
            if (!series.isConnected()){
                displayValue = "Not connected";
            } else {
                TimestampedValue tval = series.getLastValue();
                if ((tval == null) || (tval.getValue() == null)) {
                    displayValue = "";
                } else {
                    displayValue = getDisplayValue(tval.getValue().doubleValue());
                }
            }
            if (!displayValue.equals(currentDisplayValue)) {
                String text = "";
                switch(visibleRows){
                    case 1:
                        text= String.format("<html><div style='text-align: center;'>%s</div></html>", displayValue );
                        break;
                    case 2:
                        text= String.format("<html><div style='text-align: center;'>%s<br>%s</div></html>", displayValue, range );
                        break;
                    case 3:
                        text= String.format("<html><div style='text-align: center;'>%s<br>%s<br>%s</div></html>", displayValue, range, desc );
                        break;
                }                
                textInfo.setText(text);
                currentDisplayValue = displayValue;
            }
        } catch (Exception ex) {

        }
    }
    
    int visibleRows = 3;
    void setVisibleRows(int rows){
        if (visibleRows != rows){
            visibleRows = rows;
            currentDisplayValue = null;
            update();
        }
    }

    String getDisplayRange(Double value) {
        if ((value == null) || Double.isNaN(value)) {
            return "";
        }
        return series.toString(value, true, false, true);
    }

    String getDisplayValue(double value) {
        if (Double.isNaN(value)) {
            return "";
        }
        return series.toString(value, true, true, false);
    }

    /**
     * This method is called from within the constructor to initialize the form.
     * WARNING: Do NOT modify this code. The content of this method is always
     * regenerated by the Form Editor.
     */
    @SuppressWarnings("unchecked")
    // <editor-fold defaultstate="collapsed" desc="Generated Code">//GEN-BEGIN:initComponents
    private void initComponents() {

        textInfo = new javax.swing.JLabel();

        addMouseListener(new java.awt.event.MouseAdapter() {
            public void mousePressed(java.awt.event.MouseEvent evt) {
                formMousePressed(evt);
            }
        });

        textInfo.setBackground(new java.awt.Color(255, 255, 255));
        textInfo.setHorizontalAlignment(javax.swing.SwingConstants.CENTER);
        textInfo.setHorizontalTextPosition(javax.swing.SwingConstants.CENTER);
        textInfo.addMouseListener(new java.awt.event.MouseAdapter() {
            public void mousePressed(java.awt.event.MouseEvent evt) {
                formMousePressed(evt);
            }
        });

        javax.swing.GroupLayout layout = new javax.swing.GroupLayout(this);
        this.setLayout(layout);
        layout.setHorizontalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(textInfo, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, 219, Short.MAX_VALUE)
        );
        layout.setVerticalGroup(
            layout.createParallelGroup(javax.swing.GroupLayout.Alignment.LEADING)
            .addComponent(textInfo, javax.swing.GroupLayout.Alignment.TRAILING, javax.swing.GroupLayout.DEFAULT_SIZE, javax.swing.GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE)
        );
    }// </editor-fold>//GEN-END:initComponents

    private void formMousePressed(java.awt.event.MouseEvent evt) {//GEN-FIRST:event_formMousePressed
        setActive();
        PlotFrame.instance.setActive(series);
    }//GEN-LAST:event_formMousePressed

    // Variables declaration - do not modify//GEN-BEGIN:variables
    private javax.swing.JLabel textInfo;
    // End of variables declaration//GEN-END:variables
}
