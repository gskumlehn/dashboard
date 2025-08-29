from flask import Flask, request, jsonify
from app.services import brands, social_posts
from app.infra import database
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "https://kxkplnwemiprukblxuul.lovableproject.com"}})

# ---------------- DASHBOARD CONTROLLER ---------------- #
@app.route("/dashboard-group", methods=["POST"])
def create_dashboard_group():
    from uuid import uuid4
    data = request.json
    row = {
        "group_id": data.get("group_id") or str(uuid4()),
        "name": data["name"],
        "description": data.get("description"),
        "created_at": data.get("created_at")
    }
    database.load_json("dashboard_group", [row])
    return jsonify({"status": "created", "group_id": row["group_id"]}), 201


@app.route("/dashboard-group", methods=["GET"])
def list_dashboard_groups():
    sql = f"SELECT * FROM `{database.DATASET}.dashboard_group`"
    rows = database.query(sql)
    return jsonify([dict(r) for r in rows])\

@app.route("/dashboard-group/<group_id>/brand", methods=["POST"])
def add_brand_to_group(group_id):
    data = request.json
    row = {
        "group_id": group_id,
        "brand_id": data["brand_id"],
        "added_at": data.get("added_at")
    }
    database.load_json("dashboard_group_brand", [row])
    return jsonify({"status": "brand added"}), 201


@app.route("/dashboard-group/<group_id>/brands", methods=["GET"])
def list_brands_in_group(group_id):
    query = f"""
        SELECT b.*
        FROM `{database.DATASET}.brand` b
        JOIN `{database.DATASET}.dashboard_group_brand` gb
          ON b.brand_id = gb.brand_id
        WHERE gb.group_id = '{group_id}'
    """
    rows = database.query(query)

    return jsonify(rows)


# ---------------- BRAND CONTROLLER ---------------- #
@app.route("/brand", methods=["POST"])
def create_brand():
    data = request.json
    result = brands.save_brand(data)
    return jsonify(result)


@app.route("/brands", methods=["GET"])
def list_brands():
    rows = database.query("SELECT * FROM `dashboard.brand`")
    return jsonify(rows)


# ---------------- SOCIAL POST CONTROLLER ---------------- #
@app.route("/posts/<network>/<handle>", methods=["GET"])
def list_posts_by_handle(network, handle):
    query = """
        SELECT * 
        FROM `dashboard.social_post`
        WHERE network = @network AND handle = @handle
    """
    from google.cloud import bigquery
    params = [
        bigquery.ScalarQueryParameter("network", "STRING", network),
        bigquery.ScalarQueryParameter("handle", "STRING", handle)
    ]
    rows = database.query(query, params)
    return jsonify(rows)


@app.route("/posts/group/<group_id>", methods=["GET"])
def list_posts_by_group(group_id):
    query = """
        SELECT sp.* 
        FROM `dashboard.dashboard_group_brand` gb
        JOIN `dashboard.brand` b ON gb.brand_id = b.brand_id
        JOIN `dashboard.social_post` sp 
          ON ( (sp.network = 'instagram' AND sp.handle = b.instagram_handle) OR
               (sp.network = 'tiktok' AND sp.handle = b.tiktok_handle) OR
               (sp.network = 'twitter' AND sp.handle = b.twitter_handle) OR
               (sp.network = 'facebook' AND sp.handle = b.facebook_handle) OR
               (sp.network = 'youtube' AND sp.handle = b.youtube_handle) )
        WHERE gb.group_id = @group_id
    """
    from google.cloud import bigquery
    params = [bigquery.ScalarQueryParameter("group_id", "STRING", group_id)]
    rows = database.query(query, params)
    return jsonify(rows)


@app.route("/posts/sync/<network>/<handle>", methods=["POST"])
def sync_posts(network, handle):
    data = request.json
    start_date = data["start_date"]
    end_date = data["end_date"]
    result = social_posts.fetch_and_save_posts(handle, network, start_date, end_date)
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
