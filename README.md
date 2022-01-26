# sqlgen

Generate SQL using a nicer language. Just a prototype but I've been
using it daily since summer 2021.

No docs yet but see examples.clj for how queries look and feel.
Hopefully the language is fairly intuitive. If you're familiar with
dplyr, it will probably make sense quite quickly.

If you want to play, something like the following should work.
```
$ lein uberjar # to build a jar
$ cat query.clj
(-> (table foo)
    (where (= ds "2021-01-01")
           (or flag-col
               (= (json-extract-scalar data "$.topic")
                  "some_topic"))
           (not= (json-extract-scalar data "$.status") nil))
    (select data (id :as new-name-id))
    (where (= id "1234"))
    (where flag-col)
    (mutate topic (json-extract-scalar data "$.topic")
            status (json-extract-scalar data "$.status")
            is-closed (= status "closed"))
    (mutate not-is-closed (not is-closed))
    (order-by is-closed
              (desc status)
              (rand)
              (desc (json-extract-scalar data "$.topic")))
    (limit 100)
    (sql/format :inline true))
$ cat query.clj | java -jar path/to/uberjar.jar
WITH q1 AS (
  SELECT
    jq1.user_id AS user_id,
    jq1.avg_load_time AS avg_load_time,
    jq1.n_events AS n_events,
    jq2.region AS region
  FROM (
    WITH q1 AS (
      SELECT
        *
      FROM site_events
    ),
    q2 AS (
      SELECT
        *
      FROM q1
      WHERE
        (ts = '2021-01-01')
        AND (event = 'land_page')
    ),
    q3 AS (
      SELECT
        user_id,
        load_time
      FROM q2
    )
    SELECT
      user_id,
      AVG(load_time) AS avg_load_time,
      COUNT(*) AS n_events
    FROM q3
    GROUP BY
      user_id
  ) AS jq1
  LEFT JOIN (
    WITH q1 AS (
      SELECT
        *
      FROM dim_users
    )
    SELECT
      user_id,
      region
    FROM q1
  ) AS jq2
    ON (jq1.user_id = jq2.user_id)
)
SELECT
  user_id,
  avg_load_time,
  n_events,
  region,
  COUNT(*) OVER (
    PARTITION BY
      region
  ) AS n_in_region
FROM q1
ORDER BY
  RAND() ASC
LIMIT
  5000
```
