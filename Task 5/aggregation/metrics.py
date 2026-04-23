"""
Бизнес-метрики, которые считает сервис агрегации.

Каждая метрика = функция (ch_client, target_date) -> данные для записи в PG.
Все запросы используют агрегатные/оконные функции ClickHouse,
а не тупой count(*).
"""
import logging
from dataclasses import dataclass
from datetime import date, timedelta
from typing import List

log = logging.getLogger("aggregation.metrics")


@dataclass
class Metric:
    date: date
    name: str
    value: float
    dimensions: dict = None

    def __post_init__(self):
        if self.dimensions is None:
            self.dimensions = {}


# ------------------- DAU -------------------
def compute_dau(client, target_date: date) -> Metric:
    """Уникальные пользователи за день (используем uniqMerge по агрегату)."""
    q = """
    SELECT uniqMerge(uniq_users) AS dau
    FROM dau_agg
    WHERE event_date = %(d)s
    """
    result = client.query(q, parameters={"d": target_date})
    dau = int(result.result_rows[0][0]) if result.result_rows else 0
    return Metric(date=target_date, name="dau", value=float(dau))


# ------------------- Avg watch time -------------------
def compute_avg_watch_time(client, target_date: date) -> Metric:
    """Средний progress_seconds среди VIEW_FINISHED."""
    q = """
    SELECT avgMerge(avg_progress) AS avg_sec
    FROM avg_progress_agg
    WHERE event_date = %(d)s
    """
    result = client.query(q, parameters={"d": target_date})
    val = result.result_rows[0][0] if result.result_rows else None
    avg = float(val) if val is not None else 0.0
    return Metric(date=target_date, name="avg_watch_time_sec", value=avg)


# ------------------- Conversion (VIEW_FINISHED / VIEW_STARTED) -------------------
def compute_conversion(client, target_date: date) -> Metric:
    """Доля VIEW_FINISHED от VIEW_STARTED за день."""
    q = """
    SELECT
        countIf(event_type = 'VIEW_STARTED')  AS started,
        countIf(event_type = 'VIEW_FINISHED') AS finished
    FROM movie_events_raw
    WHERE event_date = %(d)s
    """
    result = client.query(q, parameters={"d": target_date})
    started, finished = result.result_rows[0] if result.result_rows else (0, 0)
    conv = (finished / started) if started > 0 else 0.0
    return Metric(
        date=target_date,
        name="view_conversion",
        value=float(conv),
        dimensions={"started": int(started), "finished": int(finished)},
    )


# ------------------- Top movies -------------------
def compute_top_movies(client, target_date: date, limit: int = 10) -> List[dict]:
    """Топ фильмов по количеству просмотров за день."""
    q = """
    SELECT
        movie_id,
        countMerge(views) AS v
    FROM movie_views_agg
    WHERE event_date = %(d)s
    GROUP BY movie_id
    ORDER BY v DESC
    LIMIT %(lim)s
    """
    result = client.query(q, parameters={"d": target_date, "lim": limit})
    rows = []
    for rank, (movie_id, views) in enumerate(result.result_rows, start=1):
        rows.append({"movie_id": movie_id, "rank": rank, "views": int(views)})
    return rows


# ------------------- Retention D1, D7 -------------------
def compute_retention_cohort(client, cohort_date: date,
                             max_day_offset: int = 7) -> List[dict]:
    """Когортный retention на max_day_offset дней после первой активности.

    Алгоритм:
      1. Определяем пользователей, у которых дата первого события = cohort_date.
      2. Для каждого day_offset смотрим, сколько из них вернулись
         (хоть какое-то событие) в cohort_date + day_offset.
      3. retention_pct = returned / cohort_size.
    """
    q = """
    WITH cohort AS (
        SELECT user_id
        FROM movie_events_raw
        GROUP BY user_id
        HAVING toDate(min(event_time)) = %(d)s
    ),
    activity AS (
        SELECT
            e.user_id AS user_id,
            dateDiff('day', toDate(%(d)s), e.event_date) AS day_offset
        FROM movie_events_raw AS e
        INNER JOIN cohort USING (user_id)
        WHERE e.event_date BETWEEN %(d)s AND addDays(%(d)s, %(max_off)s)
        GROUP BY e.user_id, day_offset
    )
    SELECT
        day_offset,
        (SELECT count() FROM cohort) AS cohort_size,
        count(DISTINCT user_id) AS returned
    FROM activity
    GROUP BY day_offset
    ORDER BY day_offset
    """
    result = client.query(q, parameters={"d": cohort_date, "max_off": max_day_offset})
    rows = []
    for day_offset, cohort_size, returned in result.result_rows:
        pct = (returned / cohort_size) if cohort_size > 0 else 0.0
        rows.append({
            "cohort_date": cohort_date,
            "day_offset": int(day_offset),
            "cohort_size": int(cohort_size),
            "returned": int(returned),
            "retention_pct": float(pct),
        })
    return rows


# ------------------- Главный запуск расчёта за дату -------------------
def compute_all_for_date(ch_client, target_date: date) -> dict:
    """Считает все метрики за день. Возвращает словарь с результатами."""
    log.info("computing metrics for %s", target_date)

    metrics = [
        compute_dau(ch_client, target_date),
        compute_avg_watch_time(ch_client, target_date),
        compute_conversion(ch_client, target_date),
    ]

    top = compute_top_movies(ch_client, target_date, limit=10)

    # Retention считаем по тем когортам, которым сегодня может "исполниться"
    # 7 дней: то есть за cohort_date = target_date - 7, также 1 день назад.
    retention_rows = []
    for offset in [1, 7]:
        cohort_date = target_date - timedelta(days=offset)
        retention_rows.extend(
            compute_retention_cohort(ch_client, cohort_date, max_day_offset=7)
        )

    return {
        "metrics": metrics,
        "top_movies": top,
        "retention": retention_rows,
    }
