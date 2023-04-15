package io.strimzi.markdown;

public class Header {

    public static String firstLevelHeader(String text) {
        return "# " + text;
    }

    public static String secondLevelHeader(String text) {
        return "## " + text;
    }

    public static String thirdLevelHeader(String text) {
        return "### " + text;
    }
}
