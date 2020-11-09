# CREATE SNOWBALL SCHEMA
snowball_schema_create = "CREATE SCHEMA IF NOT EXISTS snowball;"

# DROP TABLE
comments_table_drop = "DROP TABLE IF EXISTS %s;"

# CREATE TABLE
stock_table_create = (""" CREATE TABLE IF NOT EXISTS {} (timestamp TIMESTAMPTZ UNIQUE, date TEXT, comment TEXT, likes TEXT, positive_prob TEXT, negative_prob TEXT, confidence TEXT, sentiment TEXT) ;
""")

# DELETE OLD ROWS
delete_old_comments = (""" DELETE FROM %s WHERE timestamp <= (NOW() - INTERVAL '%s') ;
""")

# INSERT INTO TABLE
insert_new_comments = (""" INSERT INTO {} (timestamp, date, comment, likes, \
                        positive_prob, negative_prob, confidence, sentiment) \
                    VALUES (TO_TIMESTAMP(%s::double precision / 1000), %s, %s, %s, %s, %s, %s, %s) \
                    ON CONFLICT (timestamp) DO NOTHING ;
""")

# FIND COMMENTS
select_comments_by_period = (""" SELECT t.timestamp AS timestamp, t.date AS date, t.comment AS comment, t.likes AS like, t.positive_prob as positive, t.negative_prob as negative, t.confidence as confidence, \
t.sentiment as sentiment \
                    FROM {} AS t WHERE t.timestamp >= (NOW() - INTERVAL %s) \
                    ORDER BY t.timestamp DESC ;
""")

select_comments_by_count = (""" SELECT t.timestamp AS timestamp, t.date AS date, t.comment AS comment, t.likes AS like, t.positive_prob as positive, t.negative_prob as negative, t.confidence as confidence, \
t.sentiment as sentiment \
                            FROM {} AS t ORDER BY t.timestamp DESC LIMIT %s ;
""")

# GET NEWEST OCMMENT TIME IN MILLISECONDS
find_last_comment_time = (""" SELECT EXTRACT(EPOCH FROM t.timestamp) * 1000.0 AS milliseconds FROM {} AS t ORDER BY t.timestamp DESC LIMIT 1 ;
""")

# GET NUMBER OF ROWS IN THE TABLE
find_total_number_records = (""" SELECT COUNT(*) FROM {} """)
