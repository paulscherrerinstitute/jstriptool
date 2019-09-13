package ch.psi.bsread.message.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;

import ch.psi.bsread.ConfigIReceiver;
import ch.psi.bsread.command.Command;
import ch.psi.bsread.message.Message;

public class ReconnectCommand implements Command {
	private static final long serialVersionUID = -1681049107646307776L;
	private static final Logger LOGGER = LoggerFactory.getLogger(ReconnectCommand.class);

	public static final String HTYPE_VALUE_NO_VERSION = "bsr_reconnect";
	// update AbstractCommand when version increases to support old and new
	// Command
	public static final String DEFAULT_HTYPE = HTYPE_VALUE_NO_VERSION + "-1.0";

	@JsonInclude
	private String htype = DEFAULT_HTYPE;
	private String address;

	public ReconnectCommand() {
	}

	public ReconnectCommand(String address) {
		this.address = address;
	}

	public String getHtype() {
		return htype;
	}

	public void setHtype(String htype) {
		this.htype = htype;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	@Override
	public <V> Message<V> process(ConfigIReceiver<V> receiver) {
		LOGGER.info("Reconnect '{}' to '{}'", receiver.getReceiverConfig().getAddress(), address);

		receiver.close();
		receiver.getReceiverConfig().setAddress(this.address);
		receiver.connect();

		return null;
	}

}
