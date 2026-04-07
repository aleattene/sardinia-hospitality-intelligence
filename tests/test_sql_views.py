"""SQL view and materialized query tests against data_sample via in-memory DuckDB.

Data_sample summary (used to derive expected values):
  Provinces: Cagliari, Sassari, Nuoro
  Years:     2023, 2024
  Movements: 3 provinces × 12 months × 2 acc_types × 2 origins = 144 rows/year

  2024 values per (province, month, acc_type, origin):
    Cagliari non-summer (months 1-6, 10-12): Italia 1000/3000, Germania 500/2000
    Cagliari summer     (months 7-9):        Italia 3000/12000, Germania 1500/6000
    Sassari  all months:                     Italia 500/2000,   Germania 200/800
    Nuoro    all months:                     Italia 200/600,    Germania 50/200

  2023 values: same structure, lower numbers (Cagliari ~18% less, Sassari ~11%, Nuoro ~6%)

  Capacity (annual, no month): 3 provinces × 2 acc_types per year
    Cagliari Alberghiero: beds=8000 | Cagliari Extralberghiero: beds=5000
    Sassari  Alberghiero: beds=5000 | Sassari  Extralberghiero: beds=3000
    Nuoro    Alberghiero: beds=1500 | Nuoro    Extralberghiero: beds=800

Key derived values for 2024:
  Cagliari total_arrivals = 54 000  total_nights = 198 000  beds = 13 000
  Sassari  total_arrivals = 16 800  total_nights =  67 200  beds =  8 000
  Nuoro    total_arrivals =  6 000  total_nights =  19 200  beds =  2 300
"""

# ===========================================================================
# v_demand_by_province
# ===========================================================================


class TestVDemandByProvince:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM v_demand_by_province LIMIT 0").df()
        assert set(df.columns) == {
            "year",
            "province",
            "month",
            "total_arrivals",
            "total_nights",
        }

    def test_no_null_province(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_demand_by_province WHERE province IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_expected_provinces(self, transformed_conn):
        provinces = {
            r[0]
            for r in transformed_conn.execute(
                "SELECT DISTINCT province FROM v_demand_by_province"
            ).fetchall()
        }
        assert provinces == {"Cagliari", "Sassari", "Nuoro"}

    def test_both_years_present(self, transformed_conn):
        years = {
            r[0]
            for r in transformed_conn.execute(
                "SELECT DISTINCT year FROM v_demand_by_province"
            ).fetchall()
        }
        assert years == {2023, 2024}

    def test_row_count(self, transformed_conn):
        """3 provinces × 12 months × 2 years = 72 rows."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_demand_by_province"
        ).fetchone()[0]
        assert count == 72

    def test_cagliari_non_summer_month_aggregation(self, transformed_conn):
        """Month 1 aggregates all acc_types and origins: 3000 arrivals, 10 000 nights."""
        row = transformed_conn.execute(
            "SELECT total_arrivals, total_nights FROM v_demand_by_province "
            "WHERE year=2024 AND province='Cagliari' AND month=1"
        ).fetchone()
        assert row == (3000, 10000)

    def test_cagliari_summer_month_aggregation(self, transformed_conn):
        """Month 7 (summer): 9000 arrivals, 36 000 nights."""
        row = transformed_conn.execute(
            "SELECT total_arrivals, total_nights FROM v_demand_by_province "
            "WHERE year=2024 AND province='Cagliari' AND month=7"
        ).fetchone()
        assert row == (9000, 36000)

    def test_cagliari_annual_total(self, transformed_conn):
        """Sum over all months: 54 000 arrivals, 198 000 nights."""
        row = transformed_conn.execute(
            "SELECT SUM(total_arrivals), SUM(total_nights) FROM v_demand_by_province "
            "WHERE year=2024 AND province='Cagliari'"
        ).fetchone()
        assert row == (54000, 198000)

    def test_all_twelve_months_present_per_province(self, transformed_conn):
        for province in ("Cagliari", "Sassari", "Nuoro"):
            months = {
                r[0]
                for r in transformed_conn.execute(
                    f"SELECT DISTINCT month FROM v_demand_by_province "
                    f"WHERE year=2024 AND province='{province}'"
                ).fetchall()
            }
            assert months == set(range(1, 13)), f"{province} missing months"

    def test_summer_higher_than_winter_for_cagliari(self, transformed_conn):
        jan = transformed_conn.execute(
            "SELECT total_nights FROM v_demand_by_province "
            "WHERE year=2024 AND province='Cagliari' AND month=1"
        ).fetchone()[0]
        aug = transformed_conn.execute(
            "SELECT total_nights FROM v_demand_by_province "
            "WHERE year=2024 AND province='Cagliari' AND month=8"
        ).fetchone()[0]
        assert aug > jan


# ===========================================================================
# v_supply_by_province
# ===========================================================================


class TestVSupplyByProvince:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM v_supply_by_province LIMIT 0").df()
        assert set(df.columns) == {
            "year",
            "province",
            "accommodation_type",
            "total_facilities",
            "total_beds",
            "total_rooms",
        }

    def test_no_null_province(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_by_province WHERE province IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_row_count(self, transformed_conn):
        """3 provinces × 2 acc_types × 2 years = 12 rows."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_by_province"
        ).fetchone()[0]
        assert count == 12

    def test_cagliari_alberghiero_beds(self, transformed_conn):
        row = transformed_conn.execute(
            "SELECT total_beds FROM v_supply_by_province "
            "WHERE year=2024 AND province='Cagliari' AND accommodation_type='Alberghiero'"
        ).fetchone()
        assert row[0] == 8000

    def test_cagliari_extralberghiero_beds(self, transformed_conn):
        row = transformed_conn.execute(
            "SELECT total_beds FROM v_supply_by_province "
            "WHERE year=2024 AND province='Cagliari' AND accommodation_type='Extralberghiero'"
        ).fetchone()
        assert row[0] == 5000

    def test_beds_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_by_province WHERE total_beds <= 0"
        ).fetchone()[0]
        assert count == 0


# ===========================================================================
# v_supply_demand_gap
# ===========================================================================


class TestVSupplyDemandGap:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM v_supply_demand_gap LIMIT 0").df()
        assert set(df.columns) == {
            "year",
            "province",
            "total_arrivals",
            "total_nights",
            "total_facilities",
            "total_beds",
            "total_rooms",
            "occupancy_proxy",
        }

    def test_occupancy_proxy_not_null(self, transformed_conn):
        """All provinces have beds > 0 in data_sample → no NULL occupancy."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_demand_gap WHERE occupancy_proxy IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_occupancy_proxy_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_demand_gap WHERE occupancy_proxy <= 0"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_occupancy_proxy_value(self, transformed_conn):
        """Cagliari 2024: ROUND(198000 / (13000 × 365) × 100, 2)."""
        row = transformed_conn.execute(
            "SELECT occupancy_proxy FROM v_supply_demand_gap "
            "WHERE year=2024 AND province='Cagliari'"
        ).fetchone()
        expected = round(198000 / (13000 * 365) * 100, 2)
        assert abs(row[0] - expected) < 0.01

    def test_left_join_keeps_all_demand_provinces(self, transformed_conn):
        """LEFT JOIN guarantees all demand-side provinces appear in the gap view."""
        demand = {
            r[0]
            for r in transformed_conn.execute(
                "SELECT DISTINCT province FROM v_demand_by_province WHERE year=2024"
            ).fetchall()
        }
        gap = {
            r[0]
            for r in transformed_conn.execute(
                "SELECT DISTINCT province FROM v_supply_demand_gap WHERE year=2024"
            ).fetchall()
        }
        assert demand == gap

    def test_cagliari_higher_occupancy_than_nuoro(self, transformed_conn):
        rows = {
            r[0]: r[1]
            for r in transformed_conn.execute(
                "SELECT province, occupancy_proxy FROM v_supply_demand_gap WHERE year=2024"
            ).fetchall()
        }
        assert rows["Cagliari"] > rows["Nuoro"]

    def test_six_rows_total(self, transformed_conn):
        """3 provinces × 2 years = 6 rows."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_supply_demand_gap"
        ).fetchone()[0]
        assert count == 6


# ===========================================================================
# v_seasonality_profile
# ===========================================================================


class TestVSeasonalityProfile:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute(
            "SELECT * FROM v_seasonality_profile LIMIT 0"
        ).df()
        assert set(df.columns) == {
            "year",
            "province",
            "month",
            "accommodation_type",
            "monthly_nights",
            "annual_nights",
            "month_share",
        }

    def test_month_share_between_0_and_1(self, transformed_conn):
        invalid = transformed_conn.execute(
            "SELECT count(*) FROM v_seasonality_profile "
            "WHERE month_share < 0 OR month_share > 1"
        ).fetchone()[0]
        assert invalid == 0

    def test_month_share_sums_to_1_per_group(self, transformed_conn):
        """SUM(month_share) per (year, province, acc_type) must equal 1.0."""
        deviations = transformed_conn.execute("""
            SELECT year, province, accommodation_type, ROUND(SUM(month_share), 4) AS total
            FROM v_seasonality_profile
            GROUP BY year, province, accommodation_type
            HAVING ABS(SUM(month_share) - 1.0) > 0.001
            """).fetchall()
        assert len(deviations) == 0, f"Non-unit sums: {deviations}"

    def test_cagliari_summer_month_share_higher_than_winter(self, transformed_conn):
        rows = {
            r[0]: r[1]
            for r in transformed_conn.execute(
                "SELECT month, month_share FROM v_seasonality_profile "
                "WHERE year=2024 AND province='Cagliari' AND accommodation_type='Alberghiero' "
                "AND month IN (1, 7)"
            ).fetchall()
        }
        assert rows[7] > rows[1]

    def test_sassari_flat_seasonality(self, transformed_conn):
        """Sassari has equal values every month → all month_share ≈ 1/12."""
        shares = [
            r[0]
            for r in transformed_conn.execute(
                "SELECT month_share FROM v_seasonality_profile "
                "WHERE year=2024 AND province='Sassari' AND accommodation_type='Alberghiero'"
            ).fetchall()
        ]
        assert all(abs(s - 1 / 12) < 0.001 for s in shares)

    def test_monthly_nights_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_seasonality_profile WHERE monthly_nights <= 0"
        ).fetchone()[0]
        assert count == 0


# ===========================================================================
# v_segment_origin
# ===========================================================================


class TestVSegmentOrigin:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM v_segment_origin LIMIT 0").df()
        assert set(df.columns) == {
            "year",
            "province",
            "month",
            "origin",
            "origin_group",
            "total_arrivals",
            "total_nights",
        }

    def test_only_valid_origin_groups(self, transformed_conn):
        groups = {
            r[0]
            for r in transformed_conn.execute(
                "SELECT DISTINCT origin_group FROM v_segment_origin"
            ).fetchall()
        }
        assert groups == {"Domestico", "Internazionale"}

    def test_italia_classified_as_domestico(self, transformed_conn):
        row = transformed_conn.execute(
            "SELECT origin_group FROM v_segment_origin WHERE origin='Italia' LIMIT 1"
        ).fetchone()
        assert row[0] == "Domestico"

    def test_germania_classified_as_internazionale(self, transformed_conn):
        row = transformed_conn.execute(
            "SELECT origin_group FROM v_segment_origin WHERE origin='Germania' LIMIT 1"
        ).fetchone()
        assert row[0] == "Internazionale"

    def test_no_double_counting_vs_raw(self, transformed_conn):
        """SUM(total_arrivals) in v_segment_origin must equal raw SUM(arrivals)."""
        seg_total = transformed_conn.execute(
            "SELECT SUM(total_arrivals) FROM v_segment_origin WHERE year=2024"
        ).fetchone()[0]
        raw_total = transformed_conn.execute(
            "SELECT SUM(arrivals) FROM stg_tourism_flows "
            "WHERE year=2024 AND province IS NOT NULL"
        ).fetchone()[0]
        assert seg_total == raw_total

    def test_cagliari_intl_share_one_third(self, transformed_conn):
        """Germania represents exactly 1/3 of Cagliari 2024 arrivals."""
        rows = transformed_conn.execute("""
            SELECT origin_group, SUM(total_arrivals) AS tot
            FROM v_segment_origin
            WHERE year=2024 AND province='Cagliari'
            GROUP BY origin_group
            """).fetchall()
        totals = {r[0]: r[1] for r in rows}
        total = sum(totals.values())
        intl_share = totals["Internazionale"] / total
        assert abs(intl_share - 1 / 3) < 0.001

    def test_no_null_origin_group(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_segment_origin WHERE origin_group IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_total_arrivals_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_segment_origin WHERE total_arrivals <= 0"
        ).fetchone()[0]
        assert count == 0


# ===========================================================================
# v_segment_accommodation
# ===========================================================================


class TestVSegmentAccommodation:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute(
            "SELECT * FROM v_segment_accommodation LIMIT 0"
        ).df()
        assert set(df.columns) == {
            "year",
            "province",
            "accommodation_type",
            "total_arrivals",
            "total_nights",
            "avg_stay_length",
        }

    def test_avg_stay_length_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_segment_accommodation WHERE avg_stay_length <= 0"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_alberghiero_avg_stay_length(self, transformed_conn):
        """Cagliari Alberghiero 2024: nights=99000, arrivals=27000 → avg=3.67."""
        row = transformed_conn.execute(
            "SELECT avg_stay_length FROM v_segment_accommodation "
            "WHERE year=2024 AND province='Cagliari' AND accommodation_type='Alberghiero'"
        ).fetchone()
        expected = round(99000 / 27000, 2)
        assert abs(row[0] - expected) < 0.01

    def test_two_acc_types_per_province_per_year(self, transformed_conn):
        for province in ("Cagliari", "Sassari", "Nuoro"):
            count = transformed_conn.execute(
                f"SELECT count(DISTINCT accommodation_type) FROM v_segment_accommodation "
                f"WHERE year=2024 AND province='{province}'"
            ).fetchone()[0]
            assert count == 2, f"{province} should have 2 acc types"

    def test_row_count(self, transformed_conn):
        """3 provinces × 2 acc_types × 2 years = 12 rows."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_segment_accommodation"
        ).fetchone()[0]
        assert count == 12

    def test_avg_stay_length_consistent_with_totals(self, transformed_conn):
        """avg_stay_length = ROUND(nights/arrivals, 2) → error ≤ 0.005 per arrival."""
        rows = transformed_conn.execute(
            "SELECT total_arrivals, total_nights, avg_stay_length FROM v_segment_accommodation"
        ).fetchall()
        for arrivals, nights, avg in rows:
            true_avg = nights / arrivals
            assert abs(true_avg - avg) < 0.006  # ROUND(..., 2) max error = 0.005


# ===========================================================================
# v_trend_yoy
# ===========================================================================


class TestVTrendYoy:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM v_trend_yoy LIMIT 0").df()
        assert set(df.columns) == {
            "year",
            "province",
            "total_arrivals",
            "total_nights",
            "prev_arrivals",
            "prev_nights",
            "yoy_arrivals_pct",
            "yoy_nights_pct",
            "growth_rank",
        }

    def test_first_year_has_null_yoy(self, transformed_conn):
        """2023 has no prior year data → yoy_arrivals_pct must be NULL."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_trend_yoy WHERE year=2023 AND yoy_arrivals_pct IS NOT NULL"
        ).fetchone()[0]
        assert count == 0

    def test_second_year_has_non_null_yoy(self, transformed_conn):
        """2024 has 2023 as prior year → all yoy values must be non-NULL."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM v_trend_yoy WHERE year=2024 AND yoy_arrivals_pct IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_yoy_arrivals_pct(self, transformed_conn):
        """Cagliari 2024: (54000 − 45600) / 45600 × 100 = 18.42%."""
        row = transformed_conn.execute(
            "SELECT yoy_arrivals_pct FROM v_trend_yoy "
            "WHERE year=2024 AND province='Cagliari'"
        ).fetchone()
        expected = round((54000 - 45600) / 45600 * 100, 2)
        assert abs(row[0] - expected) < 0.01

    def test_cagliari_highest_growth_rank(self, transformed_conn):
        """Cagliari has the highest YoY arrivals growth → rank 1."""
        row = transformed_conn.execute(
            "SELECT growth_rank FROM v_trend_yoy WHERE year=2024 AND province='Cagliari'"
        ).fetchone()
        assert row[0] == 1

    def test_nuoro_lowest_growth_rank(self, transformed_conn):
        """Nuoro has the lowest YoY arrivals growth → rank 3."""
        row = transformed_conn.execute(
            "SELECT growth_rank FROM v_trend_yoy WHERE year=2024 AND province='Nuoro'"
        ).fetchone()
        assert row[0] == 3

    def test_prev_arrivals_matches_prior_year(self, transformed_conn):
        """prev_arrivals for 2024 Cagliari must equal 2023 Cagliari total_arrivals."""
        prev = transformed_conn.execute(
            "SELECT prev_arrivals FROM v_trend_yoy WHERE year=2024 AND province='Cagliari'"
        ).fetchone()[0]
        actual_2023 = transformed_conn.execute(
            "SELECT total_arrivals FROM v_trend_yoy WHERE year=2023 AND province='Cagliari'"
        ).fetchone()[0]
        assert prev == actual_2023

    def test_all_provinces_have_positive_yoy_2024(self, transformed_conn):
        """All provinces grew in 2024 vs 2023 in data_sample."""
        negatives = transformed_conn.execute(
            "SELECT count(*) FROM v_trend_yoy WHERE year=2024 AND yoy_arrivals_pct <= 0"
        ).fetchone()[0]
        assert negatives == 0


# ===========================================================================
# q_priority_score
# ===========================================================================


class TestQPriorityScore:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute("SELECT * FROM q_priority_score LIMIT 0").df()
        assert set(df.columns) == {
            "province",
            "occupancy_proxy",
            "yoy_arrivals_pct",
            "intl_share_pct",
            "occupancy_proxy_norm",
            "yoy_growth_norm",
            "intl_share_norm",
            "priority_score",
        }

    def test_three_provinces_scored(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_priority_score"
        ).fetchone()[0]
        assert count == 3

    def test_priority_score_between_0_and_1(self, transformed_conn):
        invalid = transformed_conn.execute(
            "SELECT count(*) FROM q_priority_score "
            "WHERE priority_score < 0 OR priority_score > 1"
        ).fetchone()[0]
        assert invalid == 0

    def test_norm_components_between_0_and_1(self, transformed_conn):
        invalid = transformed_conn.execute("""
            SELECT count(*) FROM q_priority_score
            WHERE occupancy_proxy_norm < 0 OR occupancy_proxy_norm > 1
               OR yoy_growth_norm < 0 OR yoy_growth_norm > 1
               OR intl_share_norm < 0 OR intl_share_norm > 1
            """).fetchone()[0]
        assert invalid == 0

    def test_cagliari_priority_score_is_maximum(self, transformed_conn):
        """Cagliari dominates all three components → score=1.0."""
        row = transformed_conn.execute(
            "SELECT priority_score FROM q_priority_score WHERE province='Cagliari'"
        ).fetchone()
        assert abs(row[0] - 1.0) < 0.001

    def test_nuoro_priority_score_is_minimum(self, transformed_conn):
        """Nuoro is last in all three components → score=0.0."""
        row = transformed_conn.execute(
            "SELECT priority_score FROM q_priority_score WHERE province='Nuoro'"
        ).fetchone()
        assert abs(row[0] - 0.0) < 0.001

    def test_ordered_descending(self, transformed_conn):
        scores = [
            r[0]
            for r in transformed_conn.execute(
                "SELECT priority_score FROM q_priority_score ORDER BY priority_score DESC"
            ).fetchall()
        ]
        assert scores == sorted(scores, reverse=True)

    def test_no_null_scores(self, transformed_conn):
        nulls = transformed_conn.execute(
            "SELECT count(*) FROM q_priority_score WHERE priority_score IS NULL"
        ).fetchone()[0]
        assert nulls == 0

    def test_intl_share_pct_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_priority_score WHERE intl_share_pct <= 0"
        ).fetchone()[0]
        assert count == 0


# ===========================================================================
# q_seasonality_extremes
# ===========================================================================


class TestQSeasonalityExtremes:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute(
            "SELECT * FROM q_seasonality_extremes LIMIT 0"
        ).df()
        assert set(df.columns) == {
            "province",
            "peak_month_share_pct",
            "top3_month_share_pct",
            "seasonality_index",
            "annual_nights",
        }

    def test_three_provinces_present(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_seasonality_extremes"
        ).fetchone()[0]
        assert count == 3

    def test_peak_share_between_0_and_100(self, transformed_conn):
        invalid = transformed_conn.execute(
            "SELECT count(*) FROM q_seasonality_extremes "
            "WHERE peak_month_share_pct < 0 OR peak_month_share_pct > 100"
        ).fetchone()[0]
        assert invalid == 0

    def test_top3_share_ge_peak_share(self, transformed_conn):
        """Top-3 share ≥ peak share by definition."""
        invalid = transformed_conn.execute(
            "SELECT count(*) FROM q_seasonality_extremes "
            "WHERE top3_month_share_pct < peak_month_share_pct"
        ).fetchone()[0]
        assert invalid == 0

    def test_seasonality_index_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_seasonality_extremes WHERE seasonality_index <= 0"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_more_seasonal_than_nuoro(self, transformed_conn):
        """Cagliari has summer peaks; Nuoro is flat throughout the year."""
        rows = {
            r[0]: r[1]
            for r in transformed_conn.execute(
                "SELECT province, seasonality_index FROM q_seasonality_extremes"
            ).fetchall()
        }
        assert rows["Cagliari"] > rows["Nuoro"]

    def test_sassari_flat_seasonality_index(self, transformed_conn):
        """Sassari is perfectly flat → seasonality_index = 1/12 ≈ 0.0833."""
        row = transformed_conn.execute(
            "SELECT seasonality_index FROM q_seasonality_extremes WHERE province='Sassari'"
        ).fetchone()
        assert abs(row[0] - 1 / 12) < 0.001

    def test_nuoro_flat_seasonality_index(self, transformed_conn):
        """Nuoro is also flat → seasonality_index ≈ 1/12."""
        row = transformed_conn.execute(
            "SELECT seasonality_index FROM q_seasonality_extremes WHERE province='Nuoro'"
        ).fetchone()
        assert abs(row[0] - 1 / 12) < 0.001

    def test_annual_nights_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_seasonality_extremes WHERE annual_nights <= 0"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_annual_nights(self, transformed_conn):
        """Cagliari 2024 (latest year): 198 000 total nights."""
        row = transformed_conn.execute(
            "SELECT annual_nights FROM q_seasonality_extremes WHERE province='Cagliari'"
        ).fetchone()
        assert row[0] == 198000


# ===========================================================================
# q_top_growth_segments
# ===========================================================================


class TestQTopGrowthSegments:
    def test_columns(self, transformed_conn):
        df = transformed_conn.execute(
            "SELECT * FROM q_top_growth_segments LIMIT 0"
        ).df()
        assert set(df.columns) == {
            "province",
            "accommodation_type",
            "total_arrivals",
            "total_nights",
            "avg_stay_length",
            "prev_arrivals",
            "prev_nights",
            "yoy_arrivals_pct",
            "yoy_nights_pct",
            "growth_rank",
        }

    def test_row_count(self, transformed_conn):
        """3 provinces × 2 acc_types = 6 segments (both years present)."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_top_growth_segments"
        ).fetchone()[0]
        assert count == 6

    def test_growth_rank_starts_at_1(self, transformed_conn):
        min_rank = transformed_conn.execute(
            "SELECT MIN(growth_rank) FROM q_top_growth_segments"
        ).fetchone()[0]
        assert min_rank == 1

    def test_no_null_yoy(self, transformed_conn):
        """All segments have prior year data → no NULL yoy_arrivals_pct."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_top_growth_segments WHERE yoy_arrivals_pct IS NULL"
        ).fetchone()[0]
        assert count == 0

    def test_all_segments_show_positive_growth(self, transformed_conn):
        """All 2024 segments grew vs 2023 in data_sample."""
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_top_growth_segments WHERE yoy_arrivals_pct <= 0"
        ).fetchone()[0]
        assert count == 0

    def test_cagliari_ranks_above_nuoro(self, transformed_conn):
        """Cagliari (18.42% growth) outranks Nuoro (6.38%)."""
        ranks = {
            r[0]: r[1]
            for r in transformed_conn.execute(
                "SELECT province, growth_rank FROM q_top_growth_segments "
                "WHERE accommodation_type='Alberghiero'"
            ).fetchall()
        }
        assert ranks["Cagliari"] < ranks["Nuoro"]

    def test_prev_arrivals_matches_prior_year(self, transformed_conn):
        """prev_arrivals for Cagliari Alberghiero = 2023 arrivals."""
        prev = transformed_conn.execute(
            "SELECT prev_arrivals FROM q_top_growth_segments "
            "WHERE province='Cagliari' AND accommodation_type='Alberghiero'"
        ).fetchone()[0]
        prior = transformed_conn.execute(
            "SELECT total_arrivals FROM v_segment_accommodation "
            "WHERE year=2023 AND province='Cagliari' AND accommodation_type='Alberghiero'"
        ).fetchone()[0]
        assert prev == prior

    def test_avg_stay_length_positive(self, transformed_conn):
        count = transformed_conn.execute(
            "SELECT count(*) FROM q_top_growth_segments WHERE avg_stay_length <= 0"
        ).fetchone()[0]
        assert count == 0
