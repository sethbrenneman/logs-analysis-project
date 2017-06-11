#!/usr/bin/env python3

import psycopg2
from datetime import datetime

conn = psycopg2.connect('dbname=news')
cur = conn.cursor()


def top_articles(cur):
    cur.execute("""
    SELECT title, count(*) AS views
    FROM articles, log
    WHERE substring(path from 10) = slug
    GROUP BY title
    ORDER BY views DESC
    LIMIT 3;""")

    results = cur.fetchall()
    print("\nTOP ARTICLES")
    for r in results:
        print("%s -- %s views" % (r[0], r[1]))


def most_popular_authors(cur):
    cur.execute("""
    SELECT authors.name, count(*) AS views
    FROM authors, articles, log
    WHERE substring(path from 10) = slug
    AND articles.author = authors.id
    GROUP BY authors.id
    ORDER BY views DESC;""")
    results = cur.fetchall()
    print("\nMOST POPULAR AUTHORS")
    for r in results:
        print("%s -- %s views" % (r[0], r[1]))


def high_error_days(cur):
    cur.execute("""
        SELECT t.* FROM(WITH total AS (
        SELECT date_trunc('day', time) AS time, count(*) AS views
        FROM log
        GROUP BY date_trunc('day', time)
        ), errors AS (
        SELECT date_trunc('day', time) AS time, count(*) AS views
        FROM log
        WHERE status != '200 OK'
        GROUP BY date_trunc('day', time)
        )
        SELECT (errors.views::float / total.views) * 100 AS perc, total.time
        FROM total, errors
        WHERE total.time = errors.time) t WHERE perc > 1;""")

    results = cur.fetchall()
    print("\nDAYS WITH MANY REQUEST ERRORS")
    for r in results:
        print("%s -- %.1f%%" % (r[1].strftime("%b, %d, %Y"), r[0]))


top_articles(cur)
most_popular_authors(cur)
high_error_days(cur)
conn.close()
