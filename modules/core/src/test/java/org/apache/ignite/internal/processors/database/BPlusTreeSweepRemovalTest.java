package org.apache.ignite.internal.processors.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.cache.persistence.tree.BPlusTree;
import org.apache.ignite.internal.util.lang.GridCursor;
import org.junit.Test;
import org.knowm.xchart.BitmapEncoder;
import org.knowm.xchart.XYChart;
import org.knowm.xchart.XYChartBuilder;

import static org.apache.ignite.internal.processors.cache.persistence.DataStructure.rnd;

/** Sweep removal tests. */
public class BPlusTreeSweepRemovalTest extends BPlusTreeReuseSelfTest {

    private static final String CHART_PATH = "D:\\Temp\\";

    private static int NUM_KEYS = 100_000;

    @Override protected long getTestTimeout() {
        return 2_400_000L;
    }

    @Test
    public void testSweep() throws IgniteCheckedException {
        MAX_PER_PAGE = 2;

        TestTree t = createTestTree(true);

        t.put(1L);
        t.put(2L);
        t.put(3L);
        t.put(4L);
        t.put(5L);
        t.put(6L);
        t.put(7L);
        t.put(8L);
        log.info(t.printTree());

        GridCursor<Void> cursor = t.sweep((tree, io, pageAddr, idx) -> io.getLookupRow(tree, pageAddr, idx) % 2 == 0);

        while (cursor.next())
            cursor.get();

        log.info(t.printTree());

        assertFalse(t.removex(2L));
        assertFalse(t.removex(4L));
        assertFalse(t.removex(6L));
        assertFalse(t.removex(8L));
        assertEquals(1L, t.findOne(1L).longValue());
        assertEquals(3L, t.findOne(3L).longValue());
        assertEquals(5L, t.findOne(5L).longValue());
        assertEquals(7L, t.findOne(7L).longValue());

        cursor = t.sweep((tree, io, pageAddr, idx) -> io.getLookupRow(tree, pageAddr, idx) % 2 == 1);

        while (cursor.next())
            cursor.get();

        log.info(t.printTree());

        assertTrue(t.isEmpty());
    }

    @Test
    public void testSweepLarge() throws IgniteCheckedException {
        TestTree t = createTestTree(true);

        BPlusTree.TreeRowClosure<Long, Long> clo = (tree, io, pageAddr, idx) ->
            io.getLookupRow(tree, pageAddr, idx) % 2 == 0;

        for (long k = 0L; k < NUM_KEYS; k++)
            t.putx(k);

        log.info("height=" + t.rootLevel());

        GridCursor<Void> cursor = t.sweep(clo);
        while (cursor.next())
            cursor.get();

        for (long k = 0L; k < NUM_KEYS; k++) {
            Long res = t.findOne(k);
            if (k%2 == 0)
                assertNull(res);
            else
                assertEquals(k, res.longValue());
        }

        t.destroy();
    }

    @Test
    public void testSweepConcurrentDeletes() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(0);
    }

    @Test
    public void testSweepConcurrentDeletesBackwards() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(1);
    }

    @Test
    public void testSweepConcurrentDeletesAll() throws IgniteCheckedException {
        doTestSweepConcurrentDeletes(2);
    }

    private void doTestSweepConcurrentDeletes(int mode) throws IgniteCheckedException {
        final TestTree t = createTestTree(true);

        BPlusTree.TreeRowClosure<Long, Long> clo = (tree, io, pageAddr, idx) ->
            io.getLookupRow(tree, pageAddr, idx) % 2 == 0;

        for (long k = 0L; k < NUM_KEYS; k++)
            t.putx(k);

        Thread thread = new Thread(new Runnable() {
            @Override public void run() {
                try {
                    switch (mode) {
                        case 0:
                            for (long k = 0L; k < NUM_KEYS; k++) {
                                if (k%2==1)
                                    t.removex(k);
                            }
                            break;
                        case 1:
                            for (long k = NUM_KEYS; k > 0; k--) {
                                if (k%2==1)
                                    t.removex(k);
                            }
                            break;
                        case 2:
                            for (long k = 0L; k < NUM_KEYS; k++)
                                t.removex(k);

                            break;
                    }

                }
                catch (IgniteCheckedException e) {
                    throw new IgniteException(e);
                }
            }
        });

        thread.start();

        GridCursor<Void> cursor = t.sweep(clo);
        while (cursor.next())
            cursor.get();

        try {
            thread.join();
        }
        catch (InterruptedException e) {
            fail(e.getMessage());
        }

        assertTrue(t.isEmpty());
        t.destroy();
    }

    @Test
    public void testSweepBenchmark() throws IgniteCheckedException, IOException {
        int numKeys = NUM_KEYS;
        List<Long> keys = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++)
            keys.add((long)i);

        Collections.shuffle(keys, rnd);

        List<Double> xs = new ArrayList<>();
        List<Double> ySweep = new ArrayList<>();
        List<Double> errSweep = new ArrayList<>();
        List<Double> yRemove = new ArrayList<>();
        List<Double> errRemove = new ArrayList<>();

        XYChartBuilder chartBuilder = new XYChartBuilder();
        chartBuilder.xAxisTitle("ratio of dropped keys");
        chartBuilder.yAxisTitle("duration, ms");
        XYChart chart1 = chartBuilder.build();
        XYChart chart2 = chartBuilder.build();
        chart1.setTitle("Sweep removals");
        chart2.setTitle("Sweep vs series of standard remove");

        for (int kpp = 40; kpp <= 100; kpp+=20) {
            xs.clear();
            ySweep.clear();
            errSweep.clear();
            yRemove.clear();
            errRemove.clear();

            for (int ratio = 1; ratio < 10; ratio++) {
                Stats stats = doSweepSeries(keys, kpp, 0.1f * ratio, true);
                xs.add((double)ratio*0.1d);
                ySweep.add(stats.average());
                errSweep.add(stats.error());

                stats = doSweepSeries(keys, kpp, 0.1f * ratio, false);
                yRemove.add(stats.average());
                errRemove.add(stats.error());
            }
            chart1.addSeries("sweep [" + kpp + " rows/page]", xs, ySweep, errSweep);
            chart2.addSeries("sweep [" + kpp + " rows/page]", xs, ySweep, errSweep);
            chart2.addSeries("remove [" + kpp + " rows/page]", xs, yRemove, errRemove);
        }
        // Save chart as image.
        BitmapEncoder.saveBitmapWithDPI(chart1, CHART_PATH + "Chart1", BitmapEncoder.BitmapFormat.PNG, 75);
        BitmapEncoder.saveBitmapWithDPI(chart2, CHART_PATH + "Chart2", BitmapEncoder.BitmapFormat.PNG, 75);
    }

    private Stats doSweepSeries(List<Long> keys, int maxPerPage, float ratio, boolean sweep) throws IgniteCheckedException {
        List<Long> meas = new ArrayList<>(50);

        for (int i = 0; i < 50; i++) {
            Long res = doSweepTest(keys, maxPerPage, ratio, sweep);

            if (i >= 10)
                meas.add(res);
        }

        Collections.sort(meas);
        //Long mid = meas.get(meas.size()/2);
        //Long p95 = meas.get((int)(meas.size()*.95f));
        //System.out.println("maxPerPage=" + maxPerPage + ", ratio=" + ratio + ", sweep=" + sweep + ", mid=" + mid + ", p95=" + p95);
        Stats stats = calculateStats(meas);
        log.info("maxPerPage=" + maxPerPage + ", ratio=" + ratio + ", sweep=" + sweep + ", avg=" + stats.average() + ", err=" + stats.error());
        return stats;
    }

    private Long doSweepTest(List<Long> keys, int maxPerPage, float ratio, boolean sweep) throws IgniteCheckedException {
        int numBad = (int)(keys.size() * ratio);

        if (maxPerPage > 0)
            MAX_PER_PAGE = maxPerPage;

        TestTree tree = createTestTree(true);

        for (int i = 0; i < keys.size(); i++)
            tree.putx((long)i);

        long now;

        if (sweep) {
            final Set<Long> badSet = new HashSet<>(numBad);
            for (int i = 0; i < numBad; i++)
                badSet.add(keys.get(i));

            BPlusTree.TreeRowClosure<Long, Long> clo = (t, io, pageAddr, idx) ->
                badSet.contains(io.getLookupRow(t, pageAddr, idx));

            now = System.currentTimeMillis();

            GridCursor<Void> cursor = tree.sweep(clo);
            while (cursor.next())
                cursor.get();
        }
        else {
            now = System.currentTimeMillis();

            for (int i = 0; i < numBad; i++)
                tree.removex(keys.get(i));
        }

        long then = System.currentTimeMillis();

        tree.destroy();

        return then - now;
    }

    private static Stats calculateStats(List<Long> meas) {
        double avg = 0.0d;
        for (Long m : meas)
            avg += m;

        avg /= meas.size();

        double err = 0.0d;

        for (Long m : meas)
            err += Math.pow(m - avg, 2);

        err = Math.sqrt(err / meas.size());

        return new Stats(Math.round(avg*100)/100.0d, Math.round(err*100)/100.0d);
    }

    static class Stats {
        private final double avg;
        private final double err;

        public Stats(double avg, double err) {
            this.avg = avg;
            this.err = err;
        }

        public double average() {
            return avg;
        }

        public double error() {
            return err;
        }
    }
}
