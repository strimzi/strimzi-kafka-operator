/*
 * Copyright 2018, Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.docgen.renderer;

import io.strimzi.docgen.model.ColumnModel;
import io.strimzi.docgen.model.TableInstanceModel;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class AsciidocTableRenderer implements Renderer {

    @Override
    public String fileFxtension() {
        return "adoc";
    }

    @Override
    public void render(TableInstanceModel instance, Appendable writer) throws IOException {
        int tableWidth = -1;
        int[] columnWidth = new int[instance.rows().iterator().next().size() + 1];
        Arrays.fill(columnWidth, -1);

        int i = 0;
        int rowWidth = 0;
        for (ColumnModel column : instance.kindModel().columns()) {
            rowWidth += (column.heading().length() + 2);
            columnWidth[i] = Math.max(columnWidth[i], column.heading().length());
            i++;
        }
        tableWidth = Math.max(tableWidth, rowWidth - 2);

        for (List<String> row : instance.rows()) {
            rowWidth = 0;
            i = 0;
            for (String value : row) {
                rowWidth += (value.length() + 2);
                columnWidth[i] = Math.max(columnWidth[i], value.length());
                i++;
            }
            tableWidth = Math.max(tableWidth, rowWidth - 2);
        }

        writer.append("[options=\"header\"]").append(System.lineSeparator());

        tableDelimiter(writer, tableWidth);

        writeRow(writer, columnWidth, instance.kindModel().columnHeadings());

        for (List<String> row : instance.rows()) {
            writeRow(writer, columnWidth, row);
        }

        tableDelimiter(writer, tableWidth);
    }

    private void writeRow(Appendable writer, int[] columnWidth, List<String> row) throws IOException {
        int j = 0;
        for (String value : row) {
            writeValue(writer, value, columnWidth[j], j == row.size()-1);
            j++;
        }
        writer.append(System.lineSeparator());
    }

    private void writeValue(Appendable writer, String value, int colWidth, boolean last) throws IOException {
        writer.append("|");
        writer.append(value);
        spaces(writer, " ", colWidth - value.length() + (last ? 0 : 1));
    }

    private void spaces(Appendable writer, String s, int times) throws IOException {
        for (int k = 0; k < times; k++) {
            writer.append(s);
        }
    }

    private void tableDelimiter(Appendable writer, int tableWidth) throws IOException {
        writer.append("|");
        for (int i = 0; i < tableWidth; i++) {
            writer.append("=");
        }
        writer.append(System.lineSeparator());
    }
}
