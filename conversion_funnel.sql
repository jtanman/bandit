SELECT
  cohortday,
  SUM(installs) AS installs,
  SUM(dau) AS dau,
  SUM(subtrials) AS subtrials,
  SUM(purchases) AS purchases,
  SUM(renewals) AS renewals,
  SUM(dau)/SUM(installs) AS retention,
  SUM(subtrials)/SUM(installs) AS subtrial_conversion,
  SUM(purchases)/SUM(installs) AS purchase_conversion,
  SUM(renewals)/SUM(installs) AS renewal_conversion
FROM (
  SELECT
    cohortdays.installDate,
    installs,
    date,
    cohortdays.cohortday,
    dau,
    subtrials,
    purchases,
    renewals
  FROM (
    SELECT
      installDate,
      installs,
      date,
      DATE_DIFF(date, installDate, day) AS cohortDay
    FROM (
      SELECT
        DATE(TIMESTAMP_MICROS(user_dim.first_open_timestamp_micros)) AS installDate,
        COUNT(DISTINCT user_dim.app_info.app_instance_id) AS installs
      FROM (
        SELECT
          *
        FROM
          `thriller-79838.com_hypebits_thriller_IOS.app_events_*`
        WHERE
          _TABLE_SUFFIX BETWEEN '20170813'
          AND (
          SELECT
            FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)))),
        UNNEST(event_dim) AS events
      WHERE
        DATE(TIMESTAMP_MICROS(user_dim.first_open_timestamp_micros)) BETWEEN '2017-08-13'
        AND DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)
      GROUP BY
        installDate
      ORDER BY
        installDate) installs_table
    JOIN (
      WITH
        dates_array AS (
        SELECT
          GENERATE_DATE_ARRAY('2017-8-13', DATE_ADD(CURRENT_DATE(), INTERVAL -1 day), INTERVAL 1 DAY) AS single_date)
      SELECT
        date
      FROM
        dates_array
      CROSS JOIN
        UNNEST(dates_array.single_date) AS date) dates
    ON
      dates.date >= installs_table.installDate
    ORDER BY
      installDate,
      cohortday) cohortdays
  LEFT JOIN (
    SELECT
      DATE(TIMESTAMP_MICROS(user_dim.first_open_timestamp_micros)) AS installDate,
      DATE_DIFF(DATE(TIMESTAMP_MICROS(events.timestamp_micros)), DATE(TIMESTAMP_MICROS(user_dim.first_open_timestamp_micros)), day) AS cohortDay,
      COUNT(DISTINCT user_dim.app_info.app_instance_id) AS dau,
      COUNT(DISTINCT (CASE
            WHEN events.name = 'subscription_trial2' THEN user_dim.app_info.app_instance_id
            ELSE NULL END)) AS subtrials,
      COUNT(DISTINCT (CASE
            WHEN events.name = 'subscription_purchase2' THEN user_dim.app_info.app_instance_id
            ELSE NULL END)) AS purchases,
      COUNT(DISTINCT (CASE
            WHEN events.name = 'subscription_renew2' THEN user_dim.app_info.app_instance_id
            ELSE NULL END)) AS renewals
    FROM (
      SELECT
        *
      FROM
        `thriller-79838.com_hypebits_thriller_IOS.app_events_*`
      WHERE
        _TABLE_SUFFIX BETWEEN '20170813'
        AND (
        SELECT
          FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY)))),
      UNNEST(event_dim) AS events
    WHERE
      DATE(TIMESTAMP_MICROS(user_dim.first_open_timestamp_micros)) >= '2017-08-13'
    GROUP BY
      installDate,
      cohortDay
    ORDER BY
      installDate,
      cohortDay) AS events_table
  ON
    cohortdays.installDate = events_table.installDate
    AND cohortdays.cohortday = events_table.cohortday
  ORDER BY
    cohortdays.installDate,
    cohortdays.cohortday)
GROUP BY
  cohortday