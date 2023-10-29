EXPLAIN SELECT COUNT(blocked_did) from blocklists;
Finalize Aggregate  (cost=97913.48..97913.49 rows=1 width=8)
  ->  Gather  (cost=97913.27..97913.48 rows=2 width=8)
        Workers Planned: 2
        ->  Partial Aggregate  (cost=96913.27..96913.28 rows=1 width=8)
              ->  Parallel Index Only Scan using blocklist_blocked_did on blocklists  (cost=0.56..93320.69 rows=1437031 width=33)

EXPLAIN select count(distinct blocked_did) from blocklists;
Aggregate  (cost=122061.31..122061.32 rows=1 width=8)
  ->  Index Only Scan using blocklist_blocked_did on blocklists  (cost=0.56..113439.12 rows=3448874 width=33)


EXPLAIN select count(distinct user_did) from blocklists;
Aggregate  (cost=113842.91..113842.92 rows=1 width=8)
  ->  Index Only Scan using blocklist_user_did on blocklists  (cost=0.43..105220.72 rows=3448874 width=33)


EXPLAIN SELECT COUNT(*) AS user_count
                                FROM (
                                SELECT user_did
                                FROM blocklists
                                GROUP BY user_did
                                HAVING COUNT(DISTINCT blocked_did) = 1
                            ) AS subquery;
Aggregate  (cost=167625.87..167625.88 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..167625.06 rows=65 width=33)
        Group Key: blocklists.user_did
        Filter: (count(DISTINCT blocklists.blocked_did) = 1)
        ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..150217.45 rows=3448874 width=66)


EXPLAIN SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 2 AND 100
                                ) AS subquery;
Aggregate  (cost=167662.92..167662.93 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..167662.10 rows=65 width=33)
        Group Key: blocklists.user_did
        Filter: ((count(DISTINCT blocklists.blocked_did) >= 2) AND (count(DISTINCT blocklists.blocked_did) <= 100))
        ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..150221.85 rows=3448874 width=66)

EXPLAIN SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 101 AND 1000
                                ) AS subquery;
Aggregate  (cost=167665.12..167665.13 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..167664.30 rows=65 width=33)
        Group Key: blocklists.user_did
        Filter: ((count(DISTINCT blocklists.blocked_did) >= 101) AND (count(DISTINCT blocklists.blocked_did) <= 1000))
        ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..150224.05 rows=3448874 width=66)

EXPLAIN SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) > 1000
                                ) AS subquery;
Aggregate  (cost=167686.07..167686.08 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..167631.66 rows=4353 width=33)
        Group Key: blocklists.user_did
        Filter: (count(DISTINCT blocklists.blocked_did) > 1000)
        ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..150224.05 rows=3448874 width=66)

EXPLAIN SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT user_did, COUNT(DISTINCT blocked_did) AS block_count
                                    FROM blocklists
                                    GROUP BY user_did
                                ) AS subquery;
Aggregate  (cost=167762.25..167762.26 rows=1 width=32)
  ->  GroupAggregate  (cost=0.56..167599.01 rows=13059 width=41)
        Group Key: blocklists.user_did
        ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..150224.05 rows=3448874 width=66)

EXPLAIN SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) = 1
                                ) AS subquery;
Aggregate  (cost=285950.08..285950.09 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..285946.90 rows=254 width=33)
        Group Key: blocklists.blocked_did
        Filter: (count(DISTINCT blocklists.user_did) = 1)
        ->  Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..268067.78 rows=3448874 width=66)

EXPLAIN SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 2 AND 100
                                ) AS subquery;
Aggregate  (cost=286077.03..286077.04 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..286073.85 rows=254 width=33)
        Group Key: blocklists.blocked_did
        Filter: ((count(DISTINCT blocklists.user_did) >= 2) AND (count(DISTINCT blocklists.user_did) <= 100))
        ->  Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..268067.78 rows=3448874 width=66)

EXPLAIN SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 101 AND 1000
                                ) AS subquery;
Aggregate  (cost=286077.03..286077.04 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..286073.85 rows=254 width=33)
        Group Key: blocklists.blocked_did
        Filter: ((count(DISTINCT blocklists.user_did) >= 101) AND (count(DISTINCT blocklists.user_did) <= 1000))
        ->  Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..268067.78 rows=3448874 width=66)

EXPLAIN SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) > 1000
                                ) AS subquery;
Aggregate  (cost=286158.49..286158.50 rows=1 width=8)
  ->  GroupAggregate  (cost=0.56..285946.90 rows=16927 width=33)
        Group Key: blocklists.blocked_did
        Filter: (count(DISTINCT blocklists.user_did) > 1000)
        ->  Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..268067.78 rows=3448874 width=66)

EXPLAIN SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT blocked_did, COUNT(DISTINCT user_did) AS block_count
                                    FROM blocklists
                                    GROUP BY blocked_did
                                ) AS subquery;
Aggregate  (cost=286454.71..286454.72 rows=1 width=32)
  ->  GroupAggregate  (cost=0.56..285819.95 rows=50780 width=41)
        Group Key: blocklists.blocked_did
        ->  Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..268067.78 rows=3448874 width=66)

EXPLAIN SELECT b.blocked_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    JOIN users AS u ON b.blocked_did = u.did AND u.status = TRUE
                                    WHERE b.block_date::date >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY b.blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25;
Limit  (cost=259784.81..259784.88 rows=25 width=41)
  ->  Sort  (cost=259784.81..259911.76 rows=50780 width=41)
        Sort Key: (count(*)) DESC
        ->  Finalize HashAggregate  (cost=257844.04..258351.84 rows=50780 width=41)
              Group Key: b.blocked_did
              ->  Gather  (cost=246672.44..257336.24 rows=101560 width=41)
                    Workers Planned: 2
                    ->  Partial HashAggregate  (cost=245672.44..246180.24 rows=50780 width=41)
                          Group Key: b.blocked_did
                          ->  Nested Loop  (cost=0.44..243288.72 rows=476743 width=33)
                                ->  Parallel Seq Scan on blocklists b  (cost=0.00..207082.62 rows=479010 width=33)
                                      Filter: ((block_date)::date >= (CURRENT_DATE - '1 day'::interval))
                                ->  Memoize  (cost=0.44..0.49 rows=1 width=33)
                                      Cache Key: b.blocked_did
                                      Cache Mode: logical
                                      ->  Index Scan using users_pkey on users u  (cost=0.43..0.48 rows=1 width=33)
                                            Index Cond: (did = b.blocked_did)
                                            Filter: status

EXPLAIN SELECT b.user_did, COUNT(*) AS block_count
                                    FROM blocklists as b
                                    JOIN users AS u ON b.user_did = u.did AND u.status = TRUE
                                    WHERE b.block_date::date >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25;
Limit  (cost=233373.72..233373.78 rows=25 width=41)
  ->  Sort  (cost=233373.72..233406.37 rows=13059 width=41)
        Sort Key: (count(*)) DESC
        ->  Finalize GroupAggregate  (cost=229696.71..233005.21 rows=13059 width=41)
              Group Key: b.user_did
              ->  Gather Merge  (cost=229696.71..232744.03 rows=26118 width=41)
                    Workers Planned: 2
                    ->  Sort  (cost=228696.69..228729.34 rows=13059 width=41)
                          Sort Key: b.user_did
                          ->  Partial HashAggregate  (cost=227673.34..227803.93 rows=13059 width=41)
                                Group Key: b.user_did
                                ->  Nested Loop  (cost=0.44..225289.62 rows=476743 width=33)
                                      ->  Parallel Seq Scan on blocklists b  (cost=0.00..207082.62 rows=479010 width=33)
                                            Filter: ((block_date)::date >= (CURRENT_DATE - '1 day'::interval))
                                      ->  Memoize  (cost=0.44..0.49 rows=1 width=33)
                                            Cache Key: b.user_did
                                            Cache Mode: logical
                                            ->  Index Scan using users_pkey on users u  (cost=0.43..0.48 rows=1 width=33)
                                                  Index Cond: (did = b.user_did)
                                                  Filter: status

EXPLAIN SELECT user_did FROM blocklists WHERE blocked_did = $1;
Index Scan using blocklist_blocked_did on blocklists  (cost=0.56..69.82 rows=61 width=33)
  Index Cond: (blocked_did = 'did:plc:w4xbfzo7kqfes5zb7r6qv3rw'::text)


EXPLAIN SELECT json_agg(jsonb_build_object(
                            'url', ml.url,
                            'creator', u.handle,
                            'status', u.status,
                            'list_name', ml.name,
                            'description', ml.description,
                            'list_created_date', ml.created_date,
                            'date_user_added', mu.date_added
                        )) AS mutelists
                        FROM mutelists AS ml
                        INNER JOIN mutelists_users AS mu ON ml.uri = mu.list
                        INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle
                        WHERE mu.did = $1;
Aggregate  (cost=22993.60..22993.61 rows=1 width=32)
  ->  Gather  (cost=1815.17..22993.58 rows=5 width=175)
        Workers Planned: 2
        ->  Nested Loop  (cost=815.17..21993.08 rows=2 width=175)
              ->  Hash Join  (cost=814.74..21988.91 rows=2 width=185)
                    Hash Cond: (mu.list = ml.uri)
                    ->  Parallel Seq Scan on mutelists_users mu  (cost=0.00..21174.14 rows=2 width=80)
                          Filter: (did = 'did:plc:w4xbfzo7kqfes5zb7r6qv3rw'::text)
                    ->  Hash  (cost=674.33..674.33 rows=11233 width=245)
                          ->  Seq Scan on mutelists ml  (cost=0.00..674.33 rows=11233 width=245)
              ->  Index Scan using users_pkey on users u  (cost=0.43..2.08 rows=1 width=56)
                    Index Cond: (did = ml.did)

EXPLAIN SELECT b.blocked_did, b.block_date, u.handle, u.status FROM blocklists AS b JOIN users AS u ON b.blocked_did = u.did WHERE b.user_did = $1 ORDER BY block_date DESC LIMIT $2 OFFSET $3;
Limit  (cost=566.19..566.44 rows=100 width=64)
  ->  Sort  (cost=566.19..566.57 rows=151 width=64)
        Sort Key: b.block_date DESC
        ->  Nested Loop  (cost=0.86..560.73 rows=151 width=64)
              ->  Index Scan using blocklist_user_did on blocklists b  (cost=0.43..161.33 rows=151 width=41)
                    Index Cond: (user_did = 'did:plc:w4xbfzo7kqfes5zb7r6qv3rw'::text)
              ->  Index Scan using users_pkey on users u  (cost=0.43..2.65 rows=1 width=56)
                    Index Cond: (did = b.blocked_did)

EXPLAIN SELECT COUNT(blocked_did) FROM blocklists WHERE user_did = $1;
Aggregate  (cost=41.02..41.03 rows=1 width=8)
  ->  Index Only Scan using unique_blocklist_entry on blocklists  (cost=0.56..40.64 rows=151 width=33)
        Index Cond: (user_did = 'did:plc:w4xbfzo7kqfes5zb7r6qv3rw'::text)

EXPLAIN SELECT b.user_did, b.block_date, u.handle, u.status FROM blocklists AS b JOIN users as u ON b.user_did = u.did WHERE b.blocked_did = $1 ORDER BY block_date DESC LIMIT $2 OFFSET $3;
Limit  (cost=232.97..233.13 rows=61 width=64)
  ->  Sort  (cost=232.97..233.13 rows=61 width=64)
        Sort Key: b.block_date DESC
        ->  Nested Loop  (cost=0.98..231.17 rows=61 width=64)
              ->  Index Scan using blocklist_blocked_did on blocklists b  (cost=0.56..69.82 rows=61 width=41)
                    Index Cond: (blocked_did = 'did:plc:w4xbfzo7kqfes5zb7r6qv3rw'::text)
              ->  Index Scan using users_pkey on users u  (cost=0.43..2.65 rows=1 width=56)
                    Index Cond: (did = b.user_did)
