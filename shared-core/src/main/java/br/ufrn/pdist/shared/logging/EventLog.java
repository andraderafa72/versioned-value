package br.ufrn.pdist.shared.logging;

import java.io.PrintStream;

public final class EventLog {

    private EventLog() {
    }

    /**
     * Prints one line (the structured event) then the full stack trace on stdout,
     * so logs stay correlated when redirected to a file.
     */
    public static void printlnWithStackTrace(String eventLine, Throwable throwable) {
        printlnWithStackTrace(System.out, eventLine, throwable);
    }

    /**
     * Same as {@link #printlnWithStackTrace(String, Throwable)} but on stderr for components that already log there.
     */
    public static void printlnWithStackTraceStderr(String eventLine, Throwable throwable) {
        printlnWithStackTrace(System.err, eventLine, throwable);
    }

    private static void printlnWithStackTrace(PrintStream stream, String eventLine, Throwable throwable) {
        stream.println(eventLine);
        if (throwable != null) {
            throwable.printStackTrace(stream);
        }
    }
}
