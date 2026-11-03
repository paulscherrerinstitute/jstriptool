package ch.psi.jstriptool;

import org.epics.ca.data.Graphic;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVADisplay;

/**
 *
 */
public record ChannelMetadata(String units, int precision, double low, double high, String description) {

    static ChannelMetadata fromCa(Graphic g, String desc) {
        String units = g.getUnits() == null ? "" : g.getUnits();
        int precision = ((Number) g.getPrecision()).intValue();
        double low = ((Number) g.getLowerDisplay()).doubleValue();
        double high = ((Number) g.getUpperDisplay()).doubleValue();
        if (high <= low) {
            high = low + 1;
        }
        return new ChannelMetadata(units, precision, low, high, desc);
    }

    static ChannelMetadata fromPva(PVAStructure value) {        
        PVADisplay display = PVADisplay.getDisplay(value);
        if (display==null) {
            return null;
        }
        String units = display.get("units").toString();
        //Sometimes pva returns this prefix in front of the units...
        String bug_prefix = "string units ";
        if (units.startsWith(bug_prefix)){
            units = units.substring(bug_prefix.length());
        }
        int precision = ((PVAInt) display.get("precision")).get();
        double low = ((PVADouble) display.get("limitLow")).get();
        double high = ((PVADouble) display.get("limitHigh")).get();
        String description = ((PVAString) display.get("description")).get();
        if (high <= low) {
            high = low + 1;
        }
        return new ChannelMetadata(units, precision, low, high, description);
    }

    static ChannelMetadata empty() {
        return new ChannelMetadata("", 0, 0, 0, "");
    }
};
