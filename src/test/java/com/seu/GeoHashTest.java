package com.seu;

import org.junit.Test;
import org.locationtech.geomesa.utils.geohash.GeoHash;
import org.locationtech.geomesa.utils.geohash.GeohashUtils;

public class GeoHashTest {
    @Test
    public void test() {
        GeoHash apply = GeoHash.apply(35.0, -80.0, 9);
        System.out.println(apply);

    }
}
