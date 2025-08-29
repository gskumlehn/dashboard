from flask import Flask, request, jsonify
from app.services import brands, social_posts
from app.infra import database
from flask_cors import CORS
from uuid import uuid4

app = Flask(__name__)
CORS(app, resources={
    r"/*": {
        "origins": [
            "https://kxkplnwemiprukblxuul.lovableproject.com",
            "https://*.sandbox.lovable.dev"
        ],        "methods": ["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization", "apikey", "x-client-info"],
        "supports_credentials": False
    }
})

# ---------------- DASHBOARD CONTROLLER ---------------- #
@app.route("/dashboard-full", methods=["POST"])
def create_dashboard_full():
    data = request.json
    group_id = str(uuid4())
    group_row = {
        "group_id": group_id,
        "name": data["name"],
        "description": data.get("description"),
    }

    database.load_json("dashboard_group", [group_row])

    brands_created = []
    for b in data.get("brands", []):
        brand_row = {
            "brand_id": str(uuid4()),
            "group_id": group_id,
            "name": b["name"],
            "instagram_handle": b.get("instagram_handle"),
            "tiktok_handle": b.get("tiktok_handle"),
            "twitter_handle": b.get("twitter_handle"),
            "facebook_handle": b.get("facebook_handle"),
            "youtube_handle": b.get("youtube_handle"),
        }

        database.load_json("brand", [brand_row])
        brands_created.append(brand_row)

    return jsonify({
        "status": "created",
        "group": {**group_row, "brands": brands_created}
    }), 201

@app.route("/dashboard-full", methods=["GET"])
def list_dashboards_full():
    page = int(request.args.get("page", 1))
    page_size = 10
    offset = (page - 1) * page_size

    # total count
    count_sql = f"SELECT COUNT(*) as total FROM `{database.DATASET}.dashboard_group`"
    total_rows = database.query(count_sql)
    total_count = list(total_rows)[0]["total"]

    # paginação
    query = f"""
        SELECT g.group_id, g.name, g.description, g.created_at,
               ARRAY_AGG(
                 STRUCT(
                   b.brand_id,
                   b.name,
                   b.instagram_handle,
                   b.tiktok_handle,
                   b.twitter_handle,
                   b.facebook_handle,
                   b.youtube_handle
                 )
               ) AS brands
        FROM `{database.DATASET}.dashboard_group` g
        LEFT JOIN `{database.DATASET}.brand` b
          ON g.group_id = b.group_id
        GROUP BY g.group_id, g.name, g.description, g.created_at
        ORDER BY g.created_at ASC
        LIMIT {page_size} OFFSET {offset}
    """
    rows = database.query(query)

    return jsonify({
        "page": page,
        "page_size": page_size,
        "total_count": total_count,
        "dashboards": [dict(r) for r in rows]
    })

@app.route("/dashboard-full/<group_id>", methods=["GET"])
def get_dashboard_full(group_id):
    group_sql = f"""
        SELECT *
        FROM `{database.DATASET}.dashboard_group`
        WHERE group_id = '{group_id}'
    """
    groups = database.query(group_sql)
    if not groups:
        return jsonify({"error": "Dashboard group não encontrado"}), 404

    group = dict(groups[0])

    brands_sql = f"""
        SELECT *
        FROM `{database.DATASET}.brand`
        WHERE group_id = '{group_id}'
    """
    brands = database.query(brands_sql)

    group["brands"] = [dict(b) for b in brands]

    return jsonify(group)

@app.route("/dashboard-full/<group_id>", methods=["DELETE"])
def delete_dashboard_full(group_id):
    delete_brands_sql = f"""
        DELETE FROM `{database.DATASET}.brand`
        WHERE group_id = '{group_id}'
    """
    database.exec(delete_brands_sql)

    delete_group_sql = f"""
        DELETE FROM `{database.DATASET}.dashboard_group`
        WHERE group_id = '{group_id}'
    """
    database.exec(delete_group_sql)

    return jsonify({"status": "deleted", "group_id": group_id}), 200

@app.route("/dashboard-full/<group_id>/description", methods=["PUT"])
def update_dashboard_description(group_id):
    data = request.json
    description = data.get("description")

    if not description:
        return jsonify({"error": "description is required"}), 400

    query = f"""
        UPDATE `{database.DATASET}.dashboard_group`
        SET description = '{description}'
        WHERE group_id = '{group_id}'
    """
    database.exec(query)

    return jsonify({"status": "updated", "group_id": group_id, "description": description})

# ---------------- BRAND CONTROLLER ---------------- #
@app.route("/brand/<brand_id>", methods=["DELETE"])
def delete_brand(brand_id):
    sql = f"DELETE FROM `{database.DATASET}.brand` WHERE brand_id = '{brand_id}'"
    database.exec(sql)
    return jsonify({"status": "brand deleted", "brand_id": brand_id}), 200

@app.route("/dashboard-full/<group_id>/brand", methods=["POST"])
def add_brand_to_group(group_id):
    data = request.json

    brand_row = {
        "brand_id": str(uuid4()),
        "group_id": group_id,
        "name": data["name"],
        "instagram_handle": data.get("instagram_handle"),
        "tiktok_handle": data.get("tiktok_handle"),
        "twitter_handle": data.get("twitter_handle"),
        "facebook_handle": data.get("facebook_handle"),
        "youtube_handle": data.get("youtube_handle"),
    }

    database.load_json("brand", [brand_row])
    return jsonify({"status": "brand created", "brand_id": brand_row["brand_id"]}), 201

@app.route("/brand/<brand_id>/handles", methods=["PUT"])
def update_brand_handles(brand_id):
    data = request.json

    set_clauses = []
    for field in ["instagram_handle", "tiktok_handle", "twitter_handle", "facebook_handle", "youtube_handle"]:
        if field in data:
            set_clauses.append(f"{field} = '{data[field]}'")

    if not set_clauses:
        return jsonify({"error": "Nenhum handle informado"}), 400

    set_sql = ", ".join(set_clauses)

    query = f"""
        UPDATE `{database.DATASET}.brand`
        SET {set_sql}
        WHERE brand_id = '{brand_id}'
    """
    database.exec(query)
    return jsonify({"status": "handles updated", "brand_id": brand_id}), 200

# ---------------- SOCIAL POST CONTROLLER ---------------- #
@app.route("/posts/<network>/<handle>", methods=["GET"])
def list_posts_by_handle(network, handle):
    query = f"""
        SELECT * 
        FROM `dashboard.social_post`
        WHERE network = '{network}' AND handle = '{handle}'
    """
    rows = database.query(query)
    return jsonify([dict(r) for r in rows])


@app.route("/posts/group/<group_id>", methods=["GET"])
def list_posts_by_group(group_id):
    query = f"""
        SELECT sp.* 
        FROM `dashboard.dashboard_group_brand` gb
        JOIN `dashboard.brand` b ON gb.brand_id = b.brand_id
        JOIN `dashboard.social_post` sp 
          ON ( (sp.network = 'instagram' AND sp.handle = b.instagram_handle) OR
               (sp.network = 'tiktok' AND sp.handle = b.tiktok_handle) OR
               (sp.network = 'twitter' AND sp.handle = b.twitter_handle) OR
               (sp.network = 'facebook' AND sp.handle = b.facebook_handle) OR
               (sp.network = 'youtube' AND sp.handle = b.youtube_handle) )
        WHERE gb.group_id = '{group_id}'
    """
    rows = database.query(query)
    return jsonify([dict(r) for r in rows])


@app.route("/posts/sync/<network>/<handle>", methods=["POST"])
def sync_posts(network, handle):
    data = request.json
    start_date = data["start_date"]
    end_date = data["end_date"]
    result = social_posts.save_posts(network, handle, start_date, end_date)
    return jsonify(result)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)
