package ch.psi.jstriptool;

import java.time.Instant;
import org.epics.ca.data.AlarmSeverity;
import org.epics.ca.data.Timestamped;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVANumber;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVAAlarm;
import org.epics.pva.data.nt.PVATimeStamp;

/**
 *
 */
public record ChannelValue(Double value, long timestamp, boolean invalid) {

    static boolean hideInvalid = App.hasArgument("hide_invalid");

    static ChannelValue fromCa(Timestamped timestamped) {
        if (timestamped != null) {
            if (timestamped.getValue() instanceof Double value) {
                return new ChannelValue(value, timestamped.getMillis(), timestamped.getAlarmSeverity() == AlarmSeverity.INVALID_ALARM);
            }
        }
        return null;
    }

    static ChannelValue fromPva(PVAStructure value) {
        if (value != null) {
            PVAData data = value.get("value");
            if (data instanceof PVANumber number) {
                double val = number.getNumber().doubleValue();
                PVATimeStamp timestamp = PVATimeStamp.getTimeStamp(value);
                Instant instant = (timestamp == null) ? Instant.now() : timestamp.instant();
                PVAAlarm alarm = PVAAlarm.getAlarm(value);
                boolean invalid = alarm != null && alarm.alarmSeverity() == PVAAlarm.AlarmSeverity.INVALID;
                return new ChannelValue(val, instant.toEpochMilli(), invalid);
            }
        }
        return null;
    }

    static ChannelValue empty() {
        return new ChannelValue(Double.NaN, System.currentTimeMillis(), true);
    }

    public double displayValue() {
        return (hideInvalid && invalid) || (value==null)? Double.NaN : value;
    }

}
