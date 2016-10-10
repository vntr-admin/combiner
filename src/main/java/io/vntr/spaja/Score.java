package io.vntr.spaja;

/**
 * Created by robertlindquist on 10/10/16.
 */
public class Score implements Comparable<Score> {
    public final int userId;
    public final int partitionId;
    public final float score;

    public Score(Integer userId, Integer partitionId, float score) {
        this.userId = userId;
        this.partitionId = partitionId;
        this.score = score;
    }

    public int compareTo(Score o) {
        if (this.score > o.score) {
            return 1;
        } else if (this.score < o.score) {
            return -1;
        } else {
            if (this.partitionId > o.partitionId) {
                return 1;
            } else if (this.partitionId > o.partitionId) {
                return -1;
            } else {
                if (this.userId > o.userId) {
                    return 1;
                } else if (this.userId < o.userId) {
                    return -1;
                }

                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Score score1 = (Score) o;

        if (userId != score1.userId) return false;
        if (partitionId != score1.partitionId) return false;
        return Float.compare(score1.score, score) == 0;

    }

    @Override
    public int hashCode() {
        int result = userId;
        result = 31 * result + partitionId;
        result = 31 * result + (score != +0.0f ? Float.floatToIntBits(score) : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("%3d: --(%.2f)--> %3d", userId, score, partitionId);//uid + ": --(" + score + ")-->" + newPid;
    }
}
