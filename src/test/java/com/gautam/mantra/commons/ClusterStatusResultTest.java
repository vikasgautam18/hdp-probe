package com.gautam.mantra.commons;

import org.junit.jupiter.api.Test;

class ClusterStatusResultTest {

    @Test
    void testToString() {
        ClusterStatusResult result = new ClusterStatusResult(true, false,
                true, false,
                true, false,
                true, false );

        System.out.println(result.toString());
        assert !result.toString().isEmpty();
    }
}