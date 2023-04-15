package io.strimzi.markdown;

public class TextStyle {

    public static String boldText(String text) {
        return "**" + text + "**";
    }

    public static String italicText(String text) {
        return "_" + text + "_";
    }
}
