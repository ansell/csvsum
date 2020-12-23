/**
 *
 */
package com.github.ansell.csv.util;

/**
 * An exception which signals that a line needs to be filtered out.
 *
 * @author Peter Ansell p_ansell@yahoo.com
 */
public class LineFilteredException extends RuntimeException {
    private static final long serialVersionUID = -2321787151217662059L;

    public LineFilteredException() {
        super();
    }

    public LineFilteredException(String message) {
        super(message);
    }

    public LineFilteredException(Throwable cause) {
        super(cause);
    }

    public LineFilteredException(String message, Throwable cause) {
        super(message, cause);
    }

    public LineFilteredException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}
