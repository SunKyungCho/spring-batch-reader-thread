package me.study.springbatchreaderthread.test;

import java.util.LinkedList;
import java.util.Queue;

import org.junit.jupiter.api.Test;


class TestCaseExample {

    @Test
    void exampleTest() {

        Queue<String> test = new LinkedList<>();
        test.add("test");
        test.add("king");

        for (int i = 0; i < test.size(); i++) {
            String peek = test.peek();
            System.out.println();

        }
    }

}
