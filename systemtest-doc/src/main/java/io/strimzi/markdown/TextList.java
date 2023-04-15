package io.strimzi.markdown;

import java.util.List;

public class TextList {

    public static String createOrderedList(List<String> objects) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < objects.size(); i++) {
            builder.append(i + 1 + ". " + objects.get(i) + "\n");
        }

        return builder.toString();
    }

    public static String createUnorderedList(List<String> objects) {
        StringBuilder builder = new StringBuilder();
        objects.forEach( object ->
            builder.append("* " + object + "\n")
        );

        return builder.toString();
    }
}
