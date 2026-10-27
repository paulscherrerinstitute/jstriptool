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
public record ChannelMetadata(String units, int precision, double low, double high, String description){

    static ChannelMetadata  fromCa(Graphic g, String desc){       
        String units = g.getUnits() == null ? "" : g.getUnits();
        int precision = ((Number) g.getPrecision()).intValue();
        double low = ((Number) g.getLowerDisplay()).doubleValue();
        double high = ((Number) g.getUpperDisplay()).doubleValue();   
        if (high <= low) {
            high = low + 1;
        }                                          
        return new ChannelMetadata(units, precision, low, high, desc);        
    }

    static ChannelMetadata fromPva(PVAStructure value){
        PVADisplay display = PVADisplay.fromStructure(value);
        String units = display.get("units").toString();
        int precision = ((PVAInt) display.get("precision")).get();
        double low = ((PVADouble) display.get("limitLow")).get();
        double high = ((PVADouble) display.get("limitHigh")).get();
        String description = ((PVAString) display.get("description")).get();        
        if (high <= low) {
            high = low + 1;
        }                                                      
        return new ChannelMetadata(units, precision, low, high, description);        
    }

    static ChannelMetadata empty(){
        return new ChannelMetadata ("", 0, 0, 0, "");
    }
};