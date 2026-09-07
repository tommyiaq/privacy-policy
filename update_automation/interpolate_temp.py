"""Fill in missing temperatures from neighbouring stations.

Runs after update_temp.py. Two kinds of gap get filled:

  * stations with no thermometer at all (their whole row is zeros)
  * individual missing days at stations that do have one (SIR returns nothing
    for the odd day, and update_temp.py's fillna turns that into a 0)

Both matter, because a 0 is indistinguishable from a real 0 C reading and the
app averages it in as one -- an 8-day window containing three zeros reads about
9 C too cold.

    T_est = sum_i w_i * (T_i - LAPSE_C_PER_M * (Quota_target - Quota_i)) / sum_i w_i
    w_i   = 1 / distance_km

Parameters were fitted by leave-one-out cross-validation over the 245 stations
that have a sensor:

    lapse 0.40 C/100m   -> RMSE 1.04 C   (0.60 gives 1.28, 0.00 gives 1.56)
    k = 5 donors        -> k=3 gives 1.36, k=8 adds nothing
    cap 15 km           -> covers every target; 20/30 km add nothing

That fit used late-summer data only. Autumn lapse rates differ (inversions),
so re-run the sweep once a few autumn weeks are archived.

A day is filled only from donors that actually reported it. When the near
donors are all missing the same day, a wider fallback pass is tried; anything
still unresolved is left EMPTY rather than 0, so the app skips it instead of
reading it as freezing.

TempStimata (0 = own sensor, 1 = no sensor, fully estimated) is appended LAST
on purpose: the app builds a contiguous index range between two date columns,
so a new column must never land between them.
"""
import re

import numpy as np
import pandas as pd

CSV_PATH = "assets/temp_completi.csv"
RAIN_PATH = "assets/pluvio_completi.csv"

LAPSE_C_PER_M = 0.004   # 0.40 C per 100 m
K_DONORS = 5
MAX_DIST_KM = 15.0
# Fallback for days the near donors did not report (e.g. Punta Ala, whose only
# donor inside 15 km shares its outages).
K_DONORS_FALLBACK = 12
MAX_DIST_KM_FALLBACK = 50.0
EARTH_RADIUS_KM = 6371.0

DATE_RE = re.compile(r"\d{2}/\d{2}/\d{4}$")


def haversine_matrix(lat, lon):
    """Pairwise great-circle distances in km, diagonal set to infinity."""
    la = np.radians(lat)[:, None]
    lb = np.radians(lat)[None, :]
    dlon = np.radians(lon)[None, :] - np.radians(lon)[:, None]
    h = np.sin((lb - la) / 2) ** 2 + np.cos(la) * np.cos(lb) * np.sin(dlon / 2) ** 2
    dist = 2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(np.clip(h, 0, 1)))
    np.fill_diagonal(dist, np.inf)
    return dist


def estimate(target, donors, dist_row, values, quota, k, max_km):
    """Altitude-corrected inverse-distance estimate, per day. NaN where no donor reported."""
    near = donors[dist_row[donors] <= max_km]
    if near.size == 0:
        return None
    near = near[np.argsort(dist_row[near])][:k]

    weights = (1.0 / dist_row[near])[:, None]
    adjusted = values[near, :] - LAPSE_C_PER_M * (quota[target] - quota[near])[:, None]

    weight_sum = np.where(np.isnan(adjusted), 0.0, weights).sum(axis=0)
    total = np.nansum(adjusted * weights, axis=0)
    return np.divide(total, weight_sum,
                     out=np.full(values.shape[1], np.nan),
                     where=weight_sum > 0)


def _check_aligned_with_rain(temp_last_day):
    """Warn if temperature and rainfall stop on different days.

    They should agree: update_pluvio.py drops its in-progress day, and
    termo_men only publishes days that have closed. If SIR ever changes either
    latency the two would drift apart, which shifts the app's temperature
    window relative to its rain window. The app degrades gracefully (it anchors
    on the newest days it can see) so this is a warning, not a failure.
    """
    try:
        rain_cols = pd.read_csv(RAIN_PATH, nrows=1).columns
    except OSError:
        return
    rain_days = [c for c in rain_cols if DATE_RE.match(str(c))]
    if not rain_days:
        return
    if rain_days[-1] != temp_last_day:
        print(f"⚠️  temperature ends {temp_last_day} but rainfall ends "
              f"{rain_days[-1]} -- the two series have drifted apart")
    else:
        print(f"series aligned      : both end {temp_last_day}")


def main():
    df = pd.read_csv(CSV_PATH)

    if "TempStimata" in df.columns:
        # Already filled: the gaps are gone, so has_sensor below would come out
        # all-True and quietly reset every flag to 0. Re-run update_temp.py first.
        raise SystemExit(
            f"{CSV_PATH} already has a TempStimata column -- interpolation has "
            "run against this file. Regenerate it with update_temp.py first."
        )

    date_cols = [c for c in df.columns if DATE_RE.match(str(c))]
    if not date_cols:
        raise SystemExit(f"no date columns found in {CSV_PATH}")
    # Don't depend on the degree sign surviving an editor round-trip.
    lat_col = next(c for c in df.columns if str(c).startswith("LAT "))
    lon_col = next(c for c in df.columns if str(c).startswith("LON "))

    values = df[date_cols].to_numpy(dtype=float)
    values[values == 0] = np.nan          # 0 means "no reading"
    original = values.copy()

    has_sensor = ~np.isnan(values).all(axis=1)
    lat = df[lat_col].to_numpy(dtype=float)
    lon = df[lon_col].to_numpy(dtype=float)
    quota = df["Quota"].to_numpy(dtype=float)

    print(f"stations              : {len(df)}")
    print(f"  with own sensor     : {int(has_sensor.sum())}")
    print(f"  no sensor at all    : {int((~has_sensor).sum())}")
    print(f"  missing single days : {int(np.isnan(values[has_sensor]).sum())} cells "
          f"at {int((np.isnan(values[has_sensor]).any(axis=1)).sum())} stations")

    dist = haversine_matrix(lat, lon)
    donors = np.flatnonzero(has_sensor)

    filled_cells = 0
    unresolved = 0
    for i in range(len(df)):
        gaps = np.isnan(original[i])
        if not gaps.any():
            continue
        # A station never donates to itself; donors is index-based so drop self.
        pool = donors[donors != i]

        est = estimate(i, pool, dist[i], original, quota, K_DONORS, MAX_DIST_KM)
        if est is None:
            est = np.full(len(date_cols), np.nan)

        still = gaps & np.isnan(est)
        if still.any():
            wide = estimate(i, pool, dist[i], original, quota,
                            K_DONORS_FALLBACK, MAX_DIST_KM_FALLBACK)
            if wide is not None:
                est = np.where(np.isnan(est), wide, est)

        take = gaps & ~np.isnan(est)
        values[i, take] = np.round(est[take], 1)
        filled_cells += int(take.sum())
        unresolved += int((gaps & np.isnan(est)).sum())

    # Write NaN (empty cell) for anything still unknown -- never 0.
    df[date_cols] = values
    df["TempStimata"] = (~has_sensor).astype(int)
    df.to_csv(CSV_PATH, encoding="utf8", index=False)

    _check_aligned_with_rain(date_cols[-1])

    covered = int((~np.isnan(values)).any(axis=1).sum())
    print(f"cells filled          : {filled_cells}")
    print(f"cells left empty      : {unresolved}")
    print(f"stations with data    : {covered}/{len(df)}")
    print(f"✅ File updated: {CSV_PATH}")


if __name__ == "__main__":
    main()
