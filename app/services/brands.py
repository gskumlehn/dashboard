from app.infra import database
import uuid

def save_brand(data: dict):
    brand_id = data.get("brand_id") or str(uuid.uuid4())

    row = {
        "brand_id": brand_id,
        "name": data["name"],
        "instagram_handle": data.get("instagram_handle"),
        "tiktok_handle": data.get("tiktok_handle"),
        "twitter_handle": data.get("twitter_handle"),
        "facebook_handle": data.get("facebook_handle"),
        "youtube_handle": data.get("youtube_handle"),
    }

    # staging
    tmp_table = "_tmp_brand"
    database.load_json(tmp_table, [row])

    # merge garantindo unicidade
    query = f"""
    MERGE `{database.DATASET}.brand` T
    USING `{database.DATASET}.{tmp_table}` S
    ON T.brand_id = S.brand_id
       OR (T.instagram_handle IS NOT NULL AND T.instagram_handle = S.instagram_handle)
       OR (T.tiktok_handle IS NOT NULL AND T.tiktok_handle = S.tiktok_handle)
       OR (T.twitter_handle IS NOT NULL AND T.twitter_handle = S.twitter_handle)
       OR (T.facebook_handle IS NOT NULL AND T.facebook_handle = S.facebook_handle)
       OR (T.youtube_handle IS NOT NULL AND T.youtube_handle = S.youtube_handle)
    WHEN MATCHED THEN
      UPDATE SET
        name = S.name,
        instagram_handle = COALESCE(S.instagram_handle, T.instagram_handle),
        tiktok_handle = COALESCE(S.tiktok_handle, T.tiktok_handle),
        twitter_handle = COALESCE(S.twitter_handle, T.twitter_handle),
        facebook_handle = COALESCE(S.facebook_handle, T.facebook_handle),
        youtube_handle = COALESCE(S.youtube_handle, T.youtube_handle)
    WHEN NOT MATCHED THEN
      INSERT (brand_id, name, instagram_handle, tiktok_handle, twitter_handle, facebook_handle, youtube_handle)
      VALUES (S.brand_id, S.name, S.instagram_handle, S.tiktok_handle, S.twitter_handle, S.facebook_handle, S.youtube_handle)
    """
    database.exec(query)

    # limpar staging
    database.delete_table(tmp_table)

    return {"brand_id": brand_id}


def list_brands():
    sql = f"SELECT * FROM `{database.DATASET}.brand`"
    return [dict(r) for r in database.query(sql)]
