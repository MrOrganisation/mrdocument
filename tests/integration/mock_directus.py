"""Mock Directus API backend for integration tests.

Implements the subset of the Directus API that the mrdocument-watcher uses
for user provisioning and field configuration:
    - Authentication (login)
    - Role lookup
    - User lookup and creation
    - Field metadata updates (context dropdown choices)

Endpoints:
    GET   /server/health                          - Health check
    POST  /auth/login                             - Returns a mock access token
    GET   /roles                                  - Returns roles including "MrDocument User"
    GET   /users?filter[...]                      - User lookup by external_identifier
    POST  /users                                  - User creation
    PATCH /fields/documents_v2/<field>            - Update field metadata

Usage:
    gunicorn --bind 0.0.0.0:8055 --workers 2 --timeout 30 mock_directus:app
"""

import uuid

from flask import Flask, jsonify, request

app = Flask("mock_directus")

# In-memory state
ROLE_ID = str(uuid.uuid4())
users = {}  # external_identifier -> user dict
fields = {}  # field_name -> meta dict

MOCK_TOKEN = "mock-directus-token"


def directus_response(data):
    """Wrap data in the standard Directus response envelope."""
    return jsonify({"data": data})


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.route("/server/health", methods=["GET"])
def health():
    return jsonify({"status": "ok"})


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

@app.route("/auth/login", methods=["POST"])
def login():
    return directus_response({
        "access_token": MOCK_TOKEN,
        "expires": 900000,
        "refresh_token": "mock-refresh-token",
    })


# ---------------------------------------------------------------------------
# Roles
# ---------------------------------------------------------------------------

@app.route("/roles", methods=["GET"])
def get_roles():
    return directus_response([
        {
            "id": ROLE_ID,
            "name": "MrDocument User",
            "icon": "person",
            "admin_access": False,
            "app_access": True,
        },
    ])


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

@app.route("/users", methods=["GET"])
def get_users():
    """Supports filter[external_identifier][_eq]=<value>."""
    ext_id = request.args.get("filter[external_identifier][_eq]")
    if ext_id and ext_id in users:
        return directus_response([users[ext_id]])
    if ext_id:
        return directus_response([])
    return directus_response(list(users.values()))


@app.route("/users", methods=["POST"])
def create_user():
    body = request.get_json(force=True)
    ext_id = body.get("external_identifier", "")
    user = {
        "id": str(uuid.uuid4()),
        "email": body.get("email", ""),
        "role": body.get("role", ""),
        "external_identifier": ext_id,
    }
    users[ext_id] = user
    return directus_response(user), 200


# ---------------------------------------------------------------------------
# Fields
# ---------------------------------------------------------------------------

@app.route("/fields/documents_v2/<field_name>", methods=["PATCH"])
def patch_field(field_name):
    body = request.get_json(force=True)
    fields[field_name] = body.get("meta", {})
    return directus_response({"collection": "documents_v2", "field": field_name, "meta": fields[field_name]})


@app.route("/fields/documents_v2/<field_name>", methods=["GET"])
def get_field(field_name):
    meta = fields.get(field_name, {})
    return directus_response({"collection": "documents_v2", "field": field_name, "meta": meta})
