package com.sziov.gacnev.orderstats.common;

import java.util.ArrayList;
import java.util.List;

public class BatchResult {

    private int success;
    private final List<String> failed = new ArrayList<>();

    public void incrementSuccess() { success++; }
    public void addFailed(String dt) { failed.add(dt); }
    public int getSuccess() { return success; }
    public int getFailedCount() { return failed.size(); }
    public List<String> getFailedDates() { return failed; }
    public boolean hasFailures() { return !failed.isEmpty(); }
}
