package io.woolford.kstreams.h2o.example;

public class IrisClassifiedWindowRecord extends IrisClassifiedRecord {

    Long startMs;
    Long endMs;
    Long count;

    public Long getStartMs() {
        return startMs;
    }

    public void setStartMs(Long startMs) {
        this.startMs = startMs;
    }

    public Long getEndMs() {
        return endMs;
    }

    public void setEndMs(Long endMs) {
        this.endMs = endMs;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "IrisClassifiedWindowRecord{" +
                "startMs=" + startMs +
                ", endMs=" + endMs +
                ", count=" + count +
                '}';
    }

}
