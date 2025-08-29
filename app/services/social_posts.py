from app.infra import database, fanpagekarma

def save_posts(network: str, handle: str, start_date: str, end_date: str):
    posts = fanpagekarma.get_posts(handle, network, start_date, end_date)
    if posts.empty:
        return {"inserted": 0}

    rows = posts.to_dict(orient="records")

    tmp_table = "_tmp_social_post"
    database.load_json(tmp_table, rows)

    query = f"""
    MERGE `{database.DATASET}.social_post` T
    USING `{database.DATASET}.{tmp_table}` S
    ON T.id = S.id AND T.network = S.network
    WHEN MATCHED THEN
      UPDATE SET
        type = S.type,
        date = S.date,
        message = S.message,
        link = S.link,
        likes = S.likes,
        comments = S.comments,
        shares = S.shares,
        views = S.views,
        reactions = S.reactions,
        engagement = S.engagement
    WHEN NOT MATCHED THEN
      INSERT ROW
    """
    database.exec(query)
    database.delete_table(tmp_table)

    return {"inserted": len(rows)}


def list_posts_by_handle(network: str, handle: str):
    sql = f"""
    SELECT * FROM `{database.DATASET}.social_post`
    WHERE network = @network AND id LIKE CONCAT('%', @handle, '%')
    """
    job = database.bq_client.query(sql, job_config=database.bigquery.QueryJobConfig(
        query_parameters=[
            database.bigquery.ScalarQueryParameter("network", "STRING", network),
            database.bigquery.ScalarQueryParameter("handle", "STRING", handle)
        ]
    ))
    return [dict(r) for r in job.result()]
