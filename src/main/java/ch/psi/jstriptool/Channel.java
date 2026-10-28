package ch.psi.jstriptool;

import ch.psi.jstriptool.App.Protocol;
import java.util.BitSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.epics.ca.Context;
import org.epics.ca.Monitor;
import org.epics.ca.data.Graphic;
import org.epics.ca.data.Timestamped;
import org.epics.pva.client.PVAChannel;
import org.epics.pva.client.PVAClient;
import org.epics.pva.data.PVAStructure;

/**
 *
 */
public class Channel implements AutoCloseable {

    static PVAClient pva;

    public static PVAClient getPva() throws Exception {
        if (!App.isSimulated()) {
            if (pva == null) {
                Logger.getLogger(PlotFrame.class.getName()).fine("Creating EPICS PVA client");
                pva = new PVAClient();
            }
        }
        return pva;
    }

    static Context ca;

    public static Context getCa() {
        if (!App.isSimulated()) {
            Logger.getLogger(PlotFrame.class.getName()).fine("Creating EPICS CA context");
            if (ca == null) {
                ca = new Context(App.getCaProperties());
            }
        }
        return ca;
    }

    public static void dispose() {
        if (ca != null) {
            ca.close();
            Logger.getLogger(PlotFrame.class.getName()).fine("Closed EPICS CA context");
            ca = null;
        }
        if (pva != null) {
            pva.close();
            Logger.getLogger(PlotFrame.class.getName()).fine("Closed EPICS PVA client");
            pva = null;
        }
    }

    public static ChannelMetadata readMetadata(String channelName) {
        try (Channel pv = new Channel(channelName)) {
            pv.waitConnected();
            return pv.metadata();
        } catch (Exception ex) {
            Logger.getLogger(PlotFrame.class.getName()).warning("Error reading metadata of " + channelName + ": " + ex);
            return ChannelMetadata.empty();
        }
    }

    final String name;
    final AutoCloseable channel;
    final Protocol protocol;
    CompletableFuture connectionFuture;
    boolean connected;

    public Channel(String channelName) {
        name = getChannelName(channelName);
        protocol = getChannelProtocol(channelName);
        AutoCloseable ch = null;
        try {
            if ((name == null) || name.isEmpty()) {
                throw new Exception("Empty");
            }

            if (protocol == Protocol.PVA) {
                PVAChannel channel = getPva().getChannel(name);
                connectionFuture = channel.connect().handle((ret, ex) -> {
                    connected(ex);
                    return ret;
                });
                ch = channel;
            } else {
                org.epics.ca.Channel<Double> channel = getCa().createChannel(name, Double.class);
                if (channel != null) {
                    connectionFuture = channel.connectAsync().handle((ret, ex) -> {
                        connected(ex);
                        return ret;
                    });
                }
                ch = channel;
            }
        } catch (Exception ex) {
            Logger.getLogger(PlotFrame.class.getName()).warning("Error creating channel " + name + ": " + ex);
        }
        this.channel = ch;
    }

    private void connected(Throwable ex) {
        if (ex == null) {
            Logger.getLogger(PlotFrame.class.getName()).fine("Connected to channel: " + name);
            connected = true;
            onConnection();
        } else {
            Logger.getLogger(PlotFrame.class.getName()).warning("Error connecting to channel " + name + ": " + ex);
        }
    }

    public void waitConnected() throws TimeoutException, InterruptedException, ExecutionException {
        if (connectionFuture != null) {
            connectionFuture.get(App.timeout, TimeUnit.MILLISECONDS);
        }
    }

    public boolean isConnected() {
        return connected;
    }

    public void setMonitor(Consumer<ChannelValue> consumer) {
        try {
            if (channel instanceof PVAChannel ch) {
                ch.subscribe("field(value,alarm,timeStamp)", (PVAChannel channel1, BitSet changed, BitSet overrun, PVAStructure structure) -> {
                    ChannelValue value = ChannelValue.fromPva(structure);
                    if (value != null) {
                        consumer.accept(value);
                    }
                });
            } else if (channel instanceof org.epics.ca.Channel ch) {
                Monitor<Timestamped<Double>> monitor = ch.addMonitor(Timestamped.class, timestampedValue -> {
                    ChannelValue value = ChannelValue.fromCa((Timestamped) timestampedValue);
                    if (value != null) {
                        consumer.accept(value);
                    }
                });
            }
        } catch (Exception ex) {
            System.getLogger(Channel.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }

    static Protocol getChannelProtocol(String channel) {
        if (channel.contains("://")) {
            String protocol = channel.substring(0, channel.indexOf("://"));
            try {
                return Protocol.valueOf(protocol.toUpperCase());
            } catch (Exception ex) {
                System.getLogger(Channel.class.getName()).log(System.Logger.Level.ERROR, "Invalid protocol in channel: " + channel);
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

    public ChannelValue readValue() {
        try {
            if (channel instanceof org.epics.ca.Channel ch) {
                ChannelValue value = ChannelValue.fromCa((Timestamped) ch.getAsync(Timestamped.class).get(App.timeout, TimeUnit.MILLISECONDS));
                if (value != null) {
                    return value;
                }
            } else if (channel instanceof PVAChannel ch) {
                ChannelValue value = ChannelValue.fromPva(ch.read("field(value,alarm,timeStamp)").get(App.timeout, TimeUnit.MILLISECONDS));
                if (value != null) {
                    return value;
                }
            }
        } catch (Exception ex) {
            Logger.getLogger(Channel.class.getName()).warning("Error reading channel " + channel + " value: " + ex);
        }
        return ChannelValue.empty();
    }

    public CompletableFuture<ChannelValue> getAsync() {
        if (channel instanceof org.epics.ca.Channel ch) {
            return ch.getAsync(Timestamped.class).thenApply(value -> {
                return ChannelValue.fromCa((Timestamped) value);
            });
        } else if (channel instanceof PVAChannel ch) {
            return ch.read("field(value,alarm,timeStamp)").thenApply(value -> {
                return ChannelValue.fromPva(value);
            });

        }
        return CompletableFuture.failedFuture(
                new IllegalArgumentException(
                        "Unsupported channel type: " + channel.getClass().getName()));
    }

    public ChannelMetadata metadata() {
        try {
            if (channel instanceof org.epics.ca.Channel ch) {
                CompletableFuture<Graphic> future = ch.getAsync(Graphic.class);
                Graphic g = future.get(App.timeout, TimeUnit.MILLISECONDS);
                org.epics.ca.Channel<String> channelDesc = getCa().createChannel(ch.getName() + ".DESC", String.class);
                channelDesc.connectAsync().get(App.timeout, TimeUnit.MILLISECONDS);
                String desc = channelDesc.getAsync().get(App.timeout, TimeUnit.MILLISECONDS);
                return ChannelMetadata.fromCa(g, desc);
            } else if (channel instanceof PVAChannel ch) {
                return ChannelMetadata.fromPva(ch.read("field(display)").get(App.timeout, TimeUnit.MILLISECONDS));
            }
        } catch (Exception ex) {
            Logger.getLogger(Channel.class.getName()).warning("Error reading channel " + name + " metadata: " + ex);
        }
        return ChannelMetadata.empty();
    }

    protected void onConnection() {

    }

    public void close() {
        try {
            Logger.getLogger(Channel.class.getName()).fine("Disconnected channel: " + name);
            channel.close();
        } catch (Exception ex) {
            System.getLogger(Channel.class.getName()).log(System.Logger.Level.ERROR, (String) null, ex);
        }
    }
}
