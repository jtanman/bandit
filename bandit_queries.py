# By: James Tan

# Date: 5/16/2017

"""Fetch data needed to run bandit reports"""

from analytics.tasking.command.ltv_helpers.ltv_fetch import return_query_as_df
import logging


PUBLISHER_QUERY = """
    SELECT DISTINCT coalesce(NULLIF(split_part(tags, '_', 1), ''), split_part(tags, '_', 2)) AS publisher
    FROM physical.channelclaims a
    INNER JOIN dragonsongall.users b
    ON a.udid = b.udid
    WHERE a.date BETWEEN '{start_date}' AND '{run_date}'
      AND b.install_date between ('{start_date}' - {day}) and ('{run_date}' - {day} - 1)
      AND channel = '{channel}'
      AND (a.game = 'dragonsong' OR a.game = 'dragonsongdroid')
"""

PUBLISHER_RETENTION_QUERY = """
    SELECT a.udid,
           a.install_date + {day} AS date,
           a.publisher,
           CASE
               WHEN count(b.ts_start) = 0 THEN 0
               ELSE 1
           END AS value
    FROM
      (
        SELECT a.udid,
                 b.install_date,
                 coalesce(NULLIF(split_part(tags, '_', 1), ''), split_part(tags, '_', 2)) AS publisher
         FROM physical.channelclaims a
         INNER JOIN dragonsongall.users b
         ON a.udid = b.udid
         WHERE a.channel = '{channel}'
           AND b.install_date BETWEEN ('{start_date}' - {day}) AND ('{run_date}' - {day} - 1)
           AND (a.game = 'dragonsong' OR a.game = 'dragonsongdroid')
      ) a
    LEFT JOIN dragonsongall.sessions b
    ON a.udid = b.udid
    AND b.date = a.install_date + {day}
    GROUP BY a.udid, a.install_date, a.publisher
"""

UDID_QUERY = """
    {set_param}
    (
      SELECT distinct udid, date, sub_group as shard
      FROM physical.crmplayerclusterchange
      WHERE group_name = '{name}'
        AND addition = True
        AND date between '{start_date}' and ('{run_date}' - {day} - 1)
        AND game = '{game}'
    )
"""

RETENTION_QUERY = """
    SELECT a.udid,
           a.shard,
           a.date as date_joined,
           a.date + {day} as date,
           CASE
               WHEN count(b.ts_start) = 0 THEN 0
               ELSE 1
           END AS value
    FROM udid_table a
    LEFT JOIN {game}.sessions b
    ON a.udid = b.udid
    AND b.date = a.date + {day}
    GROUP BY a.udid, a.shard, a.date
"""

CONVERSION_QUERY = """
    SELECT a.udid,
           a.shard,
           a.date as date_joined,
           a.date + {day} as date,
           CASE
               WHEN count(b.ts) = 0 THEN 0
               ELSE 1
           END AS value
    FROM udid_table a
    LEFT JOIN {game}.iaps b
    ON a.udid = b.udid
    AND b.date between a.date and a.date + {day}
    GROUP BY a.udid, a.shard, a.date
"""

CUMARPU_QUERY = """
    SELECT a.udid,
           a.shard,
           a.date as date_joined,
           a.date + {day} as date,
           sum(coalesce(b.rev, 0)) as value
    FROM udid_table a
    LEFT JOIN {game}.iaps b
    ON a.udid = b.udid
    AND b.date between a.date and a.date + {day}
    GROUP BY a.udid, a.shard, a.date
"""


def publisher_query(db, channel, start_date, run_date, day=1):
    """"Get latest session data per user in the past week"""

    query = PUBLISHER_QUERY.format(
        channel=channel,
        start_date=start_date,
        run_date=run_date,
        day=day,
    )

    logging.info('Fetching publisher list')
    df = return_query_as_df(db, query)

    return df


def publisher_retention_query(db, channel, start_date, run_date, day=1):
    """"Get latest session data per user in the past week"""

    query = PUBLISHER_RETENTION_QUERY.format(
        channel=channel,
        start_date=start_date,
        run_date=run_date,
        day=day,
    )

    logging.info('Fetching retention data for {} through {}'.format(start_date, run_date))
    df = return_query_as_df(db, query)

    return df


def get_udid_table(db, game, name, start_date, run_date, day, ret=False):
    """Get udid table for bandit crm"""

    if ret:
        set_param = ''
    else:
        set_param = 'CREATE temp table udid_table as'

    query = UDID_QUERY.format(
        game=game,
        name=name,
        start_date=start_date,
        run_date=run_date,
        day=day,
        set_param=set_param,
    )

    if ret:
        logging.info('Setting udid table for {} through {}'.format(start_date, run_date))
        df = return_query_as_df(db, query)
        return df
    else:
        logging.info('Returning udid table for {} through {}'.format(start_date, run_date))
        db.cur.execute(query)
        return


def crm_retention_query(db, game, day):
    """Get retention data for bandit crm"""

    query = RETENTION_QUERY.format(
        game=game,
        day=day,
    )

    logging.info('Fetching crm retention data')
    df = return_query_as_df(db, query)

    return df


def crm_conversion_query(db, game, day):
    """Get retention data for bandit crm"""

    query = CONVERSION_QUERY.format(
        game=game,
        day=day,
    )

    logging.info('Fetching crm conversion data')
    df = return_query_as_df(db, query)

    return df


def crm_cumarpu_query(db, game, day):
    """Get retention data for bandit crm"""

    query = CUMARPU_QUERY.format(
        game=game,
        day=day,
    )

    logging.info('Fetching crm retention data')
    df = return_query_as_df(db, query)

    return df
