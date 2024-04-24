/*
 * Copyright 2018 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.titus.common.util.rx;

import com.netflix.titus.testkit.rx.ExtTestSubscriber;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import rx.Observable;
import rx.schedulers.Schedulers;
import rx.schedulers.TestScheduler;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;

public class ObservableExtTest {

    private static final int DELAY_MS = 100;

    private final TestScheduler testScheduler = Schedulers.test();

    private final ExtTestSubscriber<Object> testSubscriber = new ExtTestSubscriber<>();

    private final Object a = new Object();
    private final Object b = new Object();
    private final Object c = new Object();
    private final Object d = new Object();
    private final Object e = new Object();
    private final Object f = new Object();

    @Test
    public void testFromWithDelayEmpty() {
        final List<Observable<Object>> chunks = Collections.emptyList();
        Observable<Object> observable = ObservableExt.fromWithDelay(chunks, DELAY_MS, TimeUnit.MILLISECONDS, testScheduler);
        observable = observable.defaultIfEmpty(b);

        observable.subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isSameAs(b);
        assertThat(testSubscriber.takeNext()).isNull();
    }

    @Test
    public void testFromWithDelaySingle() {
        final List<Observable<Object>> chunks = Collections.singletonList(Observable.just(a));
        Observable<Object> observable = ObservableExt.fromWithDelay(chunks, DELAY_MS, TimeUnit.MILLISECONDS, testScheduler);
        observable = observable.defaultIfEmpty(b);

        observable.subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isSameAs(a);
        assertThat(testSubscriber.takeNext()).isNull();
    }

    @Test
    public void testFromWithDelayMultiple() {
        final List<Observable<Object>> chunks = Arrays.asList(
                Observable.just(a).delay(1, TimeUnit.MILLISECONDS, testScheduler),
                Observable.just(b, c, d),
                Observable.just(e));
        Observable<Object> observable = ObservableExt.fromWithDelay(chunks, DELAY_MS, TimeUnit.MILLISECONDS, testScheduler);
        observable = observable.defaultIfEmpty(b);

        observable.subscribe(testSubscriber);

        // Nothing is available yet. The first element arrives after 1 ms because
        // of the delay on the just(A).
        assertThat(testSubscriber.takeNext()).isNull();

        assertThat(1).isLessThan(DELAY_MS);
        testScheduler.advanceTimeBy(1, TimeUnit.MILLISECONDS);
        // Now that we've waited the 1 ms delay, A should be available. Note
        // that fromWithDelay() should not delay the first element by DELAY_MS.
        assertThat(testSubscriber.takeNext()).isSameAs(a);
        assertThat(testSubscriber.takeNext()).isNull();

        testScheduler.advanceTimeBy(DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isSameAs(b);
        assertThat(testSubscriber.takeNext()).isSameAs(c);
        assertThat(testSubscriber.takeNext()).isSameAs(d);
        assertThat(testSubscriber.takeNext()).isNull();

        testScheduler.advanceTimeBy(DELAY_MS, TimeUnit.MILLISECONDS);
        assertThat(testSubscriber.takeNext()).isSameAs(e);
        assertThat(testSubscriber.takeNext()).isNull();
    }

    @Test
    public void testFromWithDelayHundreds() {
        final int numChunksToTest = 300;
        final List<Observable<Integer>> chunks = new ArrayList<>(numChunksToTest);
        for (int i = 0; i < numChunksToTest; ++i) {
            chunks.add(Observable.just(i).delay(1000, TimeUnit.MILLISECONDS, testScheduler));
        }
        Observable<Integer> observable = ObservableExt.fromWithDelay(chunks, DELAY_MS, TimeUnit.MILLISECONDS, testScheduler);
        observable = observable.defaultIfEmpty(-1);

        observable.subscribe(testSubscriber);

        assertThat(testSubscriber.takeNext()).isNull();

        for (int i = 0; i < numChunksToTest; ++i) {
            if (i > 0) {
                testScheduler.advanceTimeBy(DELAY_MS, TimeUnit.MILLISECONDS);
            }
            testScheduler.advanceTimeBy(1000, TimeUnit.MILLISECONDS);
            assertThat(testSubscriber.takeNext()).isEqualTo(i);
            assertThat(testSubscriber.takeNext()).isNull();
        }
    }
}
