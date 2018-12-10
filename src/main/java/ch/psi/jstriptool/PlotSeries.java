package ch.psi.jstriptool;

import ch.psi.jstriptool.Config.Curve;
import ch.psi.jstriptool.PlotPanel.SeriesInfo;
import java.awt.Color;

/**
 *
 */
public final class PlotSeries {

    public Color color = null;
    String name;
    double rangeMin = Double.NaN;
    double rangeMax = Double.NaN;
    String units = "";
    String desc = "";
    String format = "";
    String expFormat = "";

    int precision;
    boolean logarithmic;

    public PlotSeries(String name) {
        setName((name == null) ? "" : name);
    }

    public PlotSeries(String name, Color color) {
        this(name);
        this.color = color;
    }

    public PlotSeries(String name, Color color, int precision, double min, double max) {
        this(name, color);
        setPrecision(precision);
        setRange(min, max);
    }

    public PlotSeries(String name, Color color, int precision, double min, double max, String units) {
        this(name, color, precision, min, max);
        this.units = units;
    }

    public PlotSeries(String name, Color color, int precision, double min, double max, String units, String desc) {
        this(name, color, precision, min, max, units);
        this.desc = desc;
    }

    public PlotSeries(String name, Color color, int precision, double min, double max, String units, String desc, boolean logarithmic) {
        this(name, color, precision, min, max, units, desc);
        this.logarithmic = logarithmic;
    }

    public static PlotSeries create(Curve c, Color color) {
        return new PlotSeries(c.name, color,
                c.precision, c.min, c.max, c.units, c.comment, c.scale == Config.Scale.logarithmic);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Color getColor() {
        if ((color == null) && (getPlot() != null)) {
            return getPlot().getSeriesColor(this);
        }
        return color;
    }

    public void setColor(Color color) {
        this.color = color;
        if (getPlot() != null) {
            getPlot().setSeriesColor(this, color);
        }
    }

    public int getIndex() {
        SeriesInfo info = getPlot().getSeriesInfo(this);
        if (info == null) {
            return -1;
        }
        return info.index;
    }

    public void appendTerminator() {
        getPlot().addTerminator(getIndex());
    }

    public void appendData(double value) {
        getPlot().add(getIndex(), value);
    }

    public void appendData(long time, double value) {
        getPlot().add(getIndex(), time, value);
    }

    public double getRangeMin() {
        return rangeMin;
    }

    public double getRangeMax() {
        return rangeMax;
    }

    public void setRangeMin(double value) {
        setRange(value, rangeMax);
    }

    public void setRangeMax(double value) {
        setRange(rangeMin, value);
    }

    public void setRange(double min, double max) {
        boolean fixed = isFixedRange(min, max);
        rangeMin = fixed ? Math.min(min, max) : min;
        rangeMax = fixed ? Math.max(min, max) : max;
        if (getIndex() >= 0) {
            if (isFixedRange()){
                getPlot().setAxisScale(getIndex(), getRangeMin(), getRangeMax());
            } else {
                getPlot().setAxisAutoScale(getIndex());
            }
        } 
    }

    public boolean isFixedRange() {
        return isFixedRange(rangeMin, rangeMax);
    }
    
    public static boolean isFixedRange(double min, double max) {
        return (!Double.isNaN(min) && !Double.isNaN(max));
    }    

    public void setUnits(String value) {
        units = value;
    }

    public String getUnits() {
        return units;
    }

    public void setDesc(String value) {
        desc = value;
    }

    public String getDesc() {
        return desc;
    }

    public void setPrecision(int value) {
        precision = value;
        format = "%1." + precision + "f";
        expFormat = "%." + Math.min(8, precision) + "G";
    }

    public int getPrecision() {
        return precision;
    }

    public void setLogaritimic(boolean value) {
        logarithmic = value;
        if (getIndex() >= 0) {
            getPlot().setLogarithmic(getIndex(), value);
        }
    }

    public boolean isLogaritimic() {
        return logarithmic;
    }

    public TimestampedValue getLastValue() {
        return getPlot().getLastItem(getIndex());
    }

    public void setActive() {
        if (getIndex() >= 0) {
            if (getPlot().getActiveSeriesIndex() != getIndex()) {
                getPlot().setActiveSeries(getIndex());
                getPlot().notifyUpdate();
            }
        }
    }

    public boolean isActive() {
        if (getIndex() >= 0) {
            return getPlot().getActiveSeriesIndex() == getIndex();
        }
        return false;
    }

    PlotPanel getPlot() {
        //TODO
        return PlotFrame.instance.getPlot();
    }

    String toString(double value, boolean allowExponential, boolean showUnits) {
        String ret = String.format(format, value);
        if (allowExponential && (ret.length() > 10)) {
            ret = String.format(expFormat, value);
        }
        if (showUnits) {
            if ((units != null) && (!units.isEmpty())) {
                ret += " " + units;
            }
        }
        return ret;
    }

}
