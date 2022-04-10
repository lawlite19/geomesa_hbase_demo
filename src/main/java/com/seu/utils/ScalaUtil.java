
package com.seu.utils;

import java.util.List;
import java.util.stream.Collectors;

import scala.Tuple2;
import scala.collection.Seq;

public class ScalaUtil {
    public static <K, V> scala.collection.immutable.Map<K, V> toScalaImmutableMap(java.util.Map<K, V> jmap) {
        List<Tuple2<K, V>> tuples =
            jmap.entrySet().stream().map(e -> Tuple2.apply(e.getKey(), e.getValue())).collect(Collectors.toList());

        Seq<Tuple2<K, V>> scalaSeq = scala.collection.JavaConverters.asScalaBufferConverter(tuples).asScala().toSeq();

        return (scala.collection.immutable.Map<K, V>) scala.collection.immutable.Map$.MODULE$.apply(scalaSeq);
    }
}
