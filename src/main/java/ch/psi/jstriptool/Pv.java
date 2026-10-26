
package ch.psi.jstriptool;

import ch.psi.jstriptool.App.Protocol;
import java.io.IOException;
import java.time.Instant;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.epics.ca.Channel;
import org.epics.ca.Context;
import org.epics.ca.Monitor;
import org.epics.ca.data.AlarmSeverity;
import org.epics.ca.data.Graphic;
import org.epics.ca.data.Timestamped;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAData;
import org.epics.pva.data.PVADouble;
import org.epics.pva.data.PVAInt;
import org.epics.pva.data.PVANumber;
import org.epics.pva.data.PVAString;
import org.epics.pva.data.PVAStructure;
import org.epics.pva.data.nt.PVADisplay;
import org.epics.pva.data.nt.PVATimeStamp;

/**
 *
 */
public class Pv implements AutoCloseable {
    
    public static record Metadata(String units, int precision, double low, double high, String description){
        public static Metadata empty(){
            return new Metadata ("", 0, 0, 0, "");
        }
    };

    static PVAClient client;
    public static PVAClient getPva() throws Exception{
        if (client == null){
            client = new PVAClient();
        }
        return client;
    }    
    
    public static PVAChannel getPvaChannel(String name) throws Exception{    
        PVAClient client = getPva();
        return client.getChannel(name);
    }
    
    static Timestamped<Double>  readPvaValue(PVAChannel channel) throws InterruptedException, ExecutionException, IOException, TimeoutException{    
        return getPvaValue(channel.read("field(value,timeStamp)").get(App.timeout, TimeUnit.MILLISECONDS));
    }
    
    static Timestamped<Double>  getPvaValue(PVAStructure value){
        //PVAScalar<?> scalar = PVAScalar.fromStructure(value);
        PVAData data = value.get("value");
        if (data instanceof PVANumber number) {
            double y = number.getNumber().doubleValue();
            PVATimeStamp timestamp = PVATimeStamp.getTimeStamp(value);
            Instant instant = (timestamp==null) ? Instant.now() : timestamp.instant();
            long x = instant.toEpochMilli();                                            
            Timestamped timestampedValue = toTimestamped(y, x);
            return timestampedValue;
        }
        return null;
    }       
    
    static Metadata  readPvaMetadata(PVAChannel channel) throws InterruptedException, ExecutionException, TimeoutException{   
        PVAStructure value = channel.read("field(display)").get(App.timeout, TimeUnit.MILLISECONDS);
        return getPvaMetadata(value);
    }
    
    static Metadata  getPvaMetadata(PVAStructure value){            
        PVADisplay display = PVADisplay.fromStructure(value);
        String units = display.get("units").toString();
        int precision = ((PVAInt) display.get("precision")).get();
        double low = ((PVADouble) display.get("limitLow")).get();
        double high = ((PVADouble) display.get("limitHigh")).get();
        String description = ((PVAString) display.get("description")).get();        
        if (high <= low) {
            high = low + 1;
        }                                                      
        return new Metadata(units, precision, low, high, description);        
    }
    
    static Timestamped toTimestamped(Object obj, Long millis) {
        Timestamped tv = new Timestamped();
        //Timestamped<Timestamped<Double>[]> tv = new Timestamped<>(); //Retain IOC time}
        tv.setValue(obj);
        if (obj != null){
            tv.setAlarmSeverity(AlarmSeverity.NO_ALARM);
        }
        if (millis != null) {
            tv.setSeconds((long) (millis / 1e3));
            tv.setNanos((int) ((millis % 1e3) * 1e6));
        }
        return tv;
    }
    
        
    static Context context;
    public static Context getCa() {
        if (!App.isSimulated()) {
            Logger.getLogger(PlotFrame.class.getName()).fine("Creating EPICS CA context");

            if (context == null){
                context = context = new Context(App.getCaProperties());
            }
        }
        return context;
    }    
    
    public static  void closeCa(){
        if (context != null) {
            context.close();
            Logger.getLogger(PlotFrame.class.getName()).fine("Closed EPICS CA context");
            context=null;
        }        
    }
            
    static Metadata  readCaMetadata(Channel<Double> channel) throws InterruptedException, ExecutionException, TimeoutException{           
        CompletableFuture<Graphic> future= channel.getAsync(Graphic.class);
        Graphic g = future.get(App.timeout, TimeUnit.MILLISECONDS);
        Channel<String> channelDesc = getCa().createChannel(channel.getName() + ".DESC", String.class);
        channelDesc.connectAsync().get(App.timeout, TimeUnit.MILLISECONDS);
        String desc =  channelDesc.getAsync().get(App.timeout, TimeUnit.MILLISECONDS);
        return getCaMetadata(g, desc);
    }
    
    static Metadata  getCaMetadata(Graphic g, String desc){       
        String units = g.getUnits() == null ? "" : g.getUnits();
        int precision = ((Number) g.getPrecision()).intValue();
        double low = ((Number) g.getLowerDisplay()).doubleValue();
        double high = ((Number) g.getUpperDisplay()).doubleValue();   
        if (high <= low) {
            high = low + 1;
        }                                          
        return new Metadata(units, precision, low, high, desc);        
    }

    
    
    public static Metadata readMetadata(String channelName) {        
        try (Pv pv = new Pv(channelName)){
            pv.waitConnected();
            return pv.metadata();
        } catch (Exception ex){
            Logger.getLogger(PlotFrame.class.getName()).warning("Error reading metadata of " + channelName + ": " + ex );            
            return Metadata.empty();
        }
    }
           
    
    final String name;
    final AutoCloseable channel;
    final Protocol protocol;
    CompletableFuture connectionFuture;
    boolean connected;
    
    public Pv(String channelName) {
        name = getChannelName(channelName);
        protocol = getChannelProtocol(channelName);
        AutoCloseable ch = null;
        try {
            
            if (protocol == Protocol.PVA){
                PVAChannel channel = getPvaChannel(name);
                connectionFuture = channel.connect().handle((ret, ex)->{
                    connected(ex);
                    return ret;
                });                    
                ch = channel;
            } else {
                Channel<Double> channel = ((name!=null) && (!name.isEmpty())) ? getCa().createChannel(name, Double.class) : null;
                if (channel!=null){
                    connectionFuture = channel.connectAsync().handle((ret, ex) -> {
                        connected(ex);
                        return ret;
                    });
                }
                ch = channel;
            }            
        } catch(Exception ex){
            Logger.getLogger(PlotFrame.class.getName()).warning("Error creating channel " + name + ": " + ex );            
        }
        this.channel = ch;
    }       
    
    private void connected(Throwable ex){
        if (ex==null){                            
            Logger.getLogger(PlotFrame.class.getName()).fine("Connected to channel: " + name);        
            connected = true;
            onConnection();                            
        } else {
            Logger.getLogger(PlotFrame.class.getName()).warning("Error connecting to channel " + name + ": " + ex );
        }        
    }
            
    public void waitConnected() throws TimeoutException, InterruptedException, ExecutionException{
        if (connectionFuture!=null){
            connectionFuture.get(App.timeout, TimeUnit.MILLISECONDS);
        }
    }
    
    public boolean isConnected(){
        return connected;
    }

    public void addMonitor(Consumer<Timestamped<Double>> consumer) {
        try {
            if (channel instanceof PVAChannel c) {
                c.subscribe("field(value,timeStamp)", (PVAChannel channel1, BitSet changed, BitSet overrun, PVAStructure value) -> {
                    Timestamped<Double> timestampedValue = getPvaValue(value);
                    if (timestampedValue != null) {
                        consumer.accept(timestampedValue);
                    }
                });                
            } else if (channel instanceof Channel c){
                Monitor<Timestamped<Double>> monitor = c.addMonitor(Timestamped.class, timestampedValue -> {
                    if (timestampedValue != null) {
                        consumer.accept((Timestamped<Double>)timestampedValue);
                    }
                });
            }                                
        } catch (Exception ex) {
            System.getLogger(Pv.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
    
    static Protocol getChannelProtocol(String channel) {
        if (channel.contains("://")) {
            String protocol = channel.substring(0, channel.indexOf("://"));
            try{
                return Protocol.valueOf(protocol.toUpperCase());
            } catch (Exception ex){
                System.getLogger(Pv.class.getName()).log(System.Logger.Level.ERROR, "Invalid protocol in channel: " + channel);
            }
        }
        return App.defaultProtocol;        
    }

    static String getChannelName(String channel) {
        if (channel.contains("://")) {
            return channel.substring(channel.indexOf("://") + 3, channel.length());
        }
        return channel;        
    }    
    
    public Timestamped<Double> readValue(){
        try{        
            if (channel instanceof Channel ch){                
                double ret = ((Channel<Double>) ch).getAsync().get(App.timeout, TimeUnit.MILLISECONDS);
                return toTimestamped(ret, System.currentTimeMillis()); //TODO

            } else if (channel instanceof PVAChannel ch){                                                                        
                Timestamped<Double> timestampedValue = readPvaValue(ch);
                if (timestampedValue!=null){
                    return timestampedValue;
                }                                                       
            }   
        } catch (Exception ex){      
            Logger.getLogger(Pv.class.getName()).warning("Error reading channel " + channel + " value: " + ex );
        }
        return toTimestamped(Double.NaN, System.currentTimeMillis());
    }    
    
    public CompletableFuture<Timestamped> getAsync(){
        if (channel instanceof Channel ch){
            return ch.getAsync(Timestamped.class).thenApply(value -> {
                return value;
            });    
        } else if (channel instanceof PVAChannel ch){
            return ch.read("field(value,timeStamp)").thenApply(value -> {
                Timestamped<Double> timestampedValue = getPvaValue(value);
                return timestampedValue;
            });    
            
        }
        return CompletableFuture.failedFuture(
                new IllegalArgumentException(
                        "Unsupported channel type: " + channel.getClass().getName()));        
    }           
    
    
    public Metadata metadata(){        
        try{
            if (channel instanceof Channel ch){
                return readCaMetadata(ch);   

            } else if (channel instanceof PVAChannel ch){
                return readPvaMetadata(ch);                                
            }
        } catch (Exception ex){      
            Logger.getLogger(Pv.class.getName()).warning("Error reading channel " + name + " metadata: " + ex );
        }
        return Metadata.empty();
    }
            

    protected void onConnection(){

    }
        
    
    public void close(){
        try {
            Logger.getLogger(Pv.class.getName()).fine("Disconnected channel: " + name);
            channel.close();
        } catch (Exception ex) {
            System.getLogger(Pv.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
}
