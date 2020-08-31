mau_percentage="""
WITH
  monthly_activity AS (
  SELECT
    DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    CAST(COUNT(DISTINCT `by` ) AS FLOAT64) AS mau
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    month ),
  total_users AS (
  SELECT
    DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    COUNT(`by`) AS total
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    1 )
SELECT
  monthly_activity.month,
  ROUND((monthly_activity.mau/total_users.total)*100,2) AS percentage_of_active_users
FROM
  monthly_activity
JOIN
  total_users
ON
  monthly_activity.month = total_users.month
WHERE
  monthly_activity.month >= DATE("2007-02-01")
ORDER BY 
  monthly_activity.month DESC
"""



mom_mau = """
WITH
  monthly_activity AS (
  SELECT
    DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    CAST(COUNT(DISTINCT `by` ) AS FLOAT64) AS mau
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    month )
SELECT
  this_month.month,
  ROUND(((this_month.mau - last_month.mau)/last_month.mau)*100,2) AS MOM_MAU
FROM
  monthly_activity this_month
JOIN
  monthly_activity last_month
ON
  this_month.month = DATE_ADD(last_month.month, INTERVAL 1 MONTH)
WHERE
  this_month.month < ( SELECT MAX(month) FROM monthly_activity) 
  AND this_month.month > DATE("2007-06-01")
ORDER BY this_month.month DESC

      """

churn = """
 WITH
    monthly_activity AS (
    SELECT
      DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
      `by` AS userid
    FROM
      bigquery-public-data.hacker_news.full
    ),
    all_users_running_sum AS (
    SELECT
      month,
      SUM(all_users) OVER (ORDER BY month) AS running_sum
    FROM (
      SELECT
        DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
        COUNT(`by`) AS all_users
      FROM
        bigquery-public-data.hacker_news.full
      GROUP BY 1
        ) user_cnt )
SELECT
  churn.month,
  ROUND((churn.churned_users/ (all_users_running_sum.running_sum))*100.00000,5) AS churn_percentage
FROM (
  SELECT
    DATE_ADD(last_month.month, INTERVAL 1 MONTH) AS month,
    COUNT( DISTINCT last_month.userid) AS churned_users
  FROM
    monthly_activity last_month
  LEFT JOIN monthly_activity this_month
  ON
    this_month.userid = last_month.userid
    AND this_month.month = DATE_ADD(last_month.month, INTERVAL 1 MONTH)
  WHERE
    this_month.userid IS NULL
  GROUP BY
    last_month.month
  ORDER BY month DESC ) churn
JOIN
  all_users_running_sum
ON
  churn.month = all_users_running_sum.month
WHERE
  churn.month > DATE("2010-01-01")
ORDER BY month DESC

          """



new_users = """
WITH
  new_users AS (
  SELECT
    `by` AS userid,
    MIN(DATE_TRUNC(DATE(timestamp),MONTH)) AS first_month,
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    userid )
SELECT
  new_users.first_month AS month,
  COUNT(new_users.userid) AS new_users_cnt
FROM
  new_users
WHERE
  new_users.first_month < ( SELECT MAX(first_month) FROM new_users) 
GROUP BY
  new_users.first_month
ORDER BY
  new_users.first_month DESC
              """




reactivation = """
WITH
  monthly_activity AS (
  SELECT
    DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    `by` AS userid
  FROM
    bigquery-public-data.hacker_news.full ),
  first_activity AS (
  SELECT
    `by` AS userid,
    MIN(DATE_TRUNC(DATE(timestamp),MONTH)) AS month,
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    userid ),
  total_users AS (
  SELECT
    DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    COUNT(DISTINCT `by`) AS total
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    1 )
SELECT
  reactivated.month,
  ROUND((reactivated.reactivated_users / total_users.total)*100,2) AS reactivation_percentage
FROM (
  SELECT
    this_month.month AS month,
    COUNT(DISTINCT this_month.userid) AS reactivated_users,
  FROM
    monthly_activity this_month
  LEFT JOIN
    monthly_activity last_month
  ON
    this_month.userid = last_month.userid
    AND this_month.month = DATE_ADD(last_month.month, INTERVAL 1 MONTH)
  LEFT JOIN
    first_activity
  ON
    this_month.userid = first_activity.userid
    AND first_activity.month != this_month.month
  WHERE
    last_month.userid IS NULL
    AND this_month.month < (
    SELECT
      MAX(month)
    FROM
      monthly_activity)
  GROUP BY
    this_month.month ) reactivated
LEFT JOIN
  total_users
ON
  reactivated.month = total_users.month
WHERE
  DATE_TRUNC(reactivated.month,MONTH) >= DATE("2007-05-01")
ORDER BY
  reactivated.month DESC
              """



retained_users ="""
WITH
January2012Pool AS (
    SELECT
      DISTINCT `by` AS userid
    FROM
      bigquery-public-data.hacker_news.full
    WHERE
      DATE_TRUNC(DATE(timestamp),MONTH) = "2012-01-01" ),
monthly_activity AS (
    SELECT
      DATE_TRUNC(DATE(timestamp),MONTH) AS month,
      `by` AS userid
    FROM
      bigquery-public-data.hacker_news.full )


SELECT
  monthly_activity.month,
  COUNT(DISTINCT monthly_activity.userid) AS still_active_users
FROM
  monthly_activity
WHERE
  DATE_TRUNC(monthly_activity.month,YEAR) >= DATE("2012-01-01")
  AND userid IN ( SELECT * FROM January2012Pool)
GROUP BY 1
ORDER BY
  monthly_activity.month ASC
          """




retention_percentage = """
WITH
  monthly_activity AS (
  SELECT
    DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    `by` AS userid
  FROM
    bigquery-public-data.hacker_news.full ),
  total_users AS (
  SELECT
    DISTINCT DATE_TRUNC(DATE(timestamp),MONTH) AS month,
    COUNT(DISTINCT  `by`) AS total
  FROM
    bigquery-public-data.hacker_news.full
  GROUP BY
    1 )
SELECT
  retained.month,
  ROUND(((retained.retained_users / total_users.total))*100,2) AS retained_percentage
FROM (
  SELECT
    this_month.month,
    COUNT(DISTINCT this_month.userid) AS retained_users
  FROM
    monthly_activity this_month
  JOIN
    monthly_activity last_month
  ON
    this_month.month = DATE_ADD(last_month.month, INTERVAL 1 MONTH)
    AND this_month.userid = last_month.userid
  GROUP BY
    this_month.month) retained
LEFT JOIN
  total_users
ON
  retained.month = total_users.month
WHERE
  DATE_TRUNC(retained.month,MONTH) >= DATE("2007-05-01")

ORDER BY
  month DESC
"""




queries = {
  "mau_percentage":mau_percentage,
  "mom_mau": mom_mau,
  "churn": churn,
  "retained_users":retained_users,
  "retention_percentage":retention_percentage,
  "new_users":new_users,
  "reactivation":reactivation

}

















