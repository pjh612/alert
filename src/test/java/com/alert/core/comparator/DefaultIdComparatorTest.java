package com.alert.core.comparator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultIdComparatorTest {

    DefaultIdComparator comparator;

    @BeforeEach
    void setUp() {
        comparator = new DefaultIdComparator();
    }

    @Test
    @DisplayName("둘 다 null이면 0을 반환한다")
    void bothNull_returnsZero() {
        assertThat(comparator.compare(null, null)).isZero();
    }

    @Test
    @DisplayName("첫 번째가 null이면 음수를 반환한다")
    void firstNull_returnsNegative() {
        assertThat(comparator.compare(null, "100")).isNegative();
    }

    @Test
    @DisplayName("두 번째가 null이면 양수를 반환한다")
    void secondNull_returnsPositive() {
        assertThat(comparator.compare("100", null)).isPositive();
    }

    @Test
    @DisplayName("길이가 다르면 짧은 쪽이 작다")
    void differentLength_shorterIsSmaller() {
        assertThat(comparator.compare("99", "100")).isNegative();
        assertThat(comparator.compare("1000", "99")).isPositive();
    }

    @Test
    @DisplayName("길이가 같으면 사전순으로 비교한다")
    void sameLength_comparesLexicographically() {
        assertThat(comparator.compare("100", "200")).isNegative();
        assertThat(comparator.compare("200", "100")).isPositive();
        assertThat(comparator.compare("100", "100")).isZero();
    }

    @Test
    @DisplayName("Snowflake ID 크기 비교가 정확하다")
    void snowflakeIds_compareCorrectly() {
        String smaller = "1700000000001";
        String larger = "1700000000002";

        assertThat(comparator.compare(smaller, larger)).isNegative();
        assertThat(comparator.compare(larger, smaller)).isPositive();
        assertThat(comparator.compare(smaller, smaller)).isZero();
    }
}
