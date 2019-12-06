package ch.psi.jstriptool;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.PrintWriter;
import java.io.StringWriter;
import javax.swing.BorderFactory;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.border.Border;

public class SwingUtils {

    public static void invokeDelayed(final Runnable runnable, final int delayMillis) {
        new Thread(() -> {
            try {
                Thread.sleep(delayMillis);
                SwingUtilities.invokeLater(runnable);
            } catch (Exception ex) {
            }
        }).start();
    }
    
    public static String getStackTrace(final Throwable throwable) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw, true);
        throwable.printStackTrace(pw);
        return sw.getBuffer().toString();
    }

    public static String getMessage(final Throwable throwable) {
        String message = throwable.getClass().getSimpleName();
        final String msg = throwable.getMessage();
        if ((msg != null) && (msg.trim().length() > 0)) {
            message += (": " + msg);
        }
        return message;
    }

    public static String getString(final Component parent, final String msg, final Object current) {
        return JOptionPane.showInputDialog(parent, msg, (current == null) ? "" : String.valueOf(current));
    }

    public static String getString(final Component parent, final String msg, final String[] options, final String current) {
        JComboBox comboBox = new JComboBox();
        DefaultComboBoxModel model = new DefaultComboBoxModel();
        for (String option : options) {
            if (option != null) {
                model.addElement(option);
            }
        }
        if (current != null) {
            model.setSelectedItem(current);
        }
        comboBox.setModel(model);
        JPanel panel = new JPanel();
        panel.setLayout(new BorderLayout(0, 4));
        panel.add(new JLabel(msg), BorderLayout.NORTH);
        panel.add(comboBox, BorderLayout.CENTER);
        int ret = JOptionPane.showOptionDialog(parent, panel, "Input", JOptionPane.OK_CANCEL_OPTION, JOptionPane.QUESTION_MESSAGE, null, null, null);
        if (ret == JOptionPane.OK_OPTION) {
            if (comboBox.getItemCount() > 0) {
                return (String) comboBox.getSelectedItem();
            }
        }
        return null;
    }

    public static void showMessageBlocking(final Component parent, final String title, final String msg) {
        showMessageBlocking(parent, title, msg, -1);
    }

    public static void showMessageBlocking(final Component parent, final String title, final String msg, int autoCloseTimer) {
        showMessageBlocking(parent, title, msg, autoCloseTimer, Integer.MIN_VALUE);
    }

    public static void showMessageBlocking(final Component parent, final String title, final String msg, int autoCloseTimer, int messageType) {
        if (autoCloseTimer > 0) {
            final JOptionPane pane = new JOptionPane(msg, JOptionPane.INFORMATION_MESSAGE);
            final JDialog dialog = pane.createDialog(parent, (title == null) ? "Message" : title);
            dialog.setDefaultCloseOperation(JDialog.DISPOSE_ON_CLOSE);
            Timer timer = new Timer(autoCloseTimer, (ActionEvent e) -> {
                dialog.setVisible(false);
                ((Timer) e.getSource()).stop();
            });
            timer.start();
            dialog.setVisible(true);
        } else {
            if (messageType == Integer.MIN_VALUE) {
                messageType = JOptionPane.INFORMATION_MESSAGE;
            }
            JOptionPane.showMessageDialog(parent, msg, (title == null) ? "Message" : title, messageType, null);
        }
    }

    /**
     * Safe to call from any thread
     */
    public static void showMessage(final Component parent, final String title, final String msg) {
        showMessage(parent, title, msg, -1);
    }

    public static void showMessage(final Component parent, final String title, final String msg, final int autoCloseTimer) {
        showMessage(parent, title, msg, autoCloseTimer, Integer.MIN_VALUE);
    }

    public static void showMessage(final Component parent, final String title, final String msg, final int autoCloseTimer, int messageType) {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> {
                showMessageBlocking(parent, title, msg, autoCloseTimer, messageType);
            });
            return;
        }
        showMessageBlocking(parent, title, msg, autoCloseTimer, messageType);
    }

    public static void showExceptionBlocking(final Component parent, final Exception ex) {
        showExceptionBlocking(parent, ex, null);
    }

    public static void showExceptionBlocking(final Component parent, final Exception ex, String title) {

        int max_width = 1000;
        int max_msg_height = 300;
        int min_width = 400;
        String message = getMessage(ex);

        BorderLayout layout = new BorderLayout();
        layout.setVgap(6);
        JPanel panel = new JPanel(layout);
        Border padding = BorderFactory.createEmptyBorder(8, 0, 2, 0);
        panel.setBorder(padding);
        JTextArea textMessage = new JTextArea(message);
        textMessage.setEnabled(false);
        textMessage.setFont(new JLabel().getFont().deriveFont(Font.BOLD));
        textMessage.setLineWrap(false);
        textMessage.setBorder(null);
        textMessage.setBackground(null);
        textMessage.setDisabledTextColor(textMessage.getForeground());
        textMessage.setAutoscrolls(true);

        if (textMessage.getPreferredSize().width > max_width) {
            textMessage.setPreferredSize(new Dimension(max_width, textMessage.getPreferredSize().height));
        } else if (textMessage.getPreferredSize().width < min_width) {
            textMessage.setPreferredSize(new Dimension(min_width, textMessage.getPreferredSize().height));
        }
        if (textMessage.getPreferredSize().height > max_msg_height) {
            textMessage.setPreferredSize(new Dimension(textMessage.getPreferredSize().width, max_msg_height));
        }
        panel.add(textMessage, BorderLayout.NORTH);

        JScrollPane scrollDetails = new javax.swing.JScrollPane();
        JButton button = new JButton("Details");
        button.setFont(button.getFont().deriveFont(Font.PLAIN));
        JPanel buttonPanel = new JPanel(new BorderLayout());
        buttonPanel.add(button, BorderLayout.WEST);
        panel.add(buttonPanel, BorderLayout.SOUTH);
        button.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                panel.remove(buttonPanel);
                panel.add(scrollDetails, BorderLayout.SOUTH);
                //scrollDetails.setSize(250, scrollDetails.getHeight());
                if (scrollDetails.getPreferredSize().width > max_width) {
                    scrollDetails.setPreferredSize(new Dimension(max_width, scrollDetails.getPreferredSize().height));
                }
                ((JDialog) panel.getTopLevelAncestor()).pack();
            }
        });

        JTextArea textDetails = new JTextArea(getStackTrace(ex));
        textDetails.setEditable(false);
        textDetails.setLineWrap(false);
        textDetails.setRows(12);
        scrollDetails.setViewportView(textDetails);
        scrollDetails.setAutoscrolls(true);

        JOptionPane.showMessageDialog(parent, panel, (title == null) ? "Exception" : title, JOptionPane.WARNING_MESSAGE, null);
    }

    /**
     * Safe to call from any thread
     */
    public static void showException(final Component parent, final Exception ex) {
        showException(parent, ex, null);
    }

    public static void showException(final Component parent, final Exception ex, String title) {
        if (!SwingUtilities.isEventDispatchThread()) {
            SwingUtilities.invokeLater(() -> {
                showExceptionBlocking(parent, ex, title);
            });
            return;
        }
        showExceptionBlocking(parent, ex, title);
    }

    public enum OptionType {

        Default,
        YesNo,
        YesNoCancel,
        OkCancel;

        int toJOptionPaneType() {
            switch (this) {
                case YesNo:
                    return JOptionPane.YES_NO_OPTION;
                case YesNoCancel:
                    return JOptionPane.YES_NO_CANCEL_OPTION;
                case OkCancel:
                    return JOptionPane.OK_CANCEL_OPTION;
                default:
                    return JOptionPane.DEFAULT_OPTION;
            }
        }
    }

    public enum OptionResult {

        Yes,
        No,
        Cancel,
        Closed;

        static OptionResult fromJOptionPaneResult(int result) {
            switch (result) {
                case JOptionPane.YES_OPTION:
                    return OptionResult.Yes;   //Equals OK
                case JOptionPane.NO_OPTION:
                    return OptionResult.No;
                case JOptionPane.CANCEL_OPTION:
                    return OptionResult.Cancel;
                default:
                    return OptionResult.Closed;
            }
        }
    }

    public static OptionResult showOption(final Component parent, final String title, final String msg, OptionType type) {
        int ret = JOptionPane.showOptionDialog(parent, msg, (title == null) ? "Input" : title, type.toJOptionPaneType(), JOptionPane.QUESTION_MESSAGE, null, null, null);
        return OptionResult.fromJOptionPaneResult(ret);
    }

    public static OptionResult showOption(final Component parent, final String title, final Component component, OptionType type) {
        int ret = JOptionPane.showOptionDialog(parent, component, (title == null) ? "Input" : title, type.toJOptionPaneType(), JOptionPane.QUESTION_MESSAGE, null, null, null);
        return OptionResult.fromJOptionPaneResult(ret);
    }
}
