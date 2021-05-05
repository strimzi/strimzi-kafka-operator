/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.test.generators;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Class, which providing set of generated numbers and each number can be used only once
 */
public class RndWithoutRepetition {

    private final List<Integer> integerList = new ArrayList<>();

    /**
     * Constructor of the RndWithoutRepetition
     * @param startingInterval starting interval of numbers, which should be generated
     * @param endingInterval ending interval of numbers, which should be generated
     */
    public RndWithoutRepetition(Integer startingInterval, Integer endingInterval) {
        int limit = endingInterval - startingInterval;

        if (limit <= 0) throw new RuntimeException("Ending interval must be bigger than starting interval.");

        ThreadLocalRandom.current()
            .ints(startingInterval, endingInterval)
            .distinct()
            .limit(limit)
            .forEach(number -> integerList.add(number));

        System.out.println("All generated numbers are:\n" + integerList.toString());
    }

    /**
     * Picking specific element from list
     * @return return unique number from list
     */
    public synchronized int getUniqueNumber() {
        if (integerList.size() <= 0) throw new RuntimeException("There is no more unique numbers!");

        int pickedNumber = integerList.get(new Random().nextInt(integerList.size()));
        integerList.remove((Integer) pickedNumber);
        return pickedNumber;
    }
}
