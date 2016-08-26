from os import path
from datetime import timedelta
from random import randint
import sqlite3

from flask import Flask, request, g, jsonify
from celery import Celery
from celery.task import periodic_task
from redis import StrictRedis
from functools import wraps


app = Flask(__name__)
app.config.from_object(__name__)

REDIS_HOST = "127.0.0.1"
REDIS_PORT = "6379"
BROKER_URL = "redis://{}:{}".format(REDIS_HOST, REDIS_PORT)

celery_worker = Celery("tasks", broker=BROKER_URL)
redis = StrictRedis(host=REDIS_HOST, port=REDIS_PORT)

app.config.update(dict(
    DATABASE=path.join(app.root_path, "rest_api.db"),
    SECRET_KEY="secret",
    USERNAME="admin",
    PASSWORD="admin"
))

app.config.from_envvar("REST_API_SETTINGS", silent=True)

DATABASE = "/tmp/rest_api.db"


def check_if_all_exist(*required):
    def decor(fun):
        @wraps(fun)
        def wrapper(*args, **kwargs):
            for key in required:
                if key not in request.json:
                    return jsonify({"msg": "Request didn't contain obligatory parameters"})
            return fun(*args, **kwargs)
        return wrapper
    return decor


def connect_db():
    """
    Connects to the specific database.
    """
    rows = sqlite3.connect(app.config["DATABASE"])
    rows.row_factory = sqlite3.Row
    return rows


def get_db():
    """
    Opens a new database connection if there is none
    yet for the current application context.
    """
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
    return db


@app.teardown_appcontext
def close_db(error):
    """
    Closes the database again at the end of the request.
    """
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    with app.open_resource("schema.sql", mode="r") as f:
        db.cursor().executescript(f.read())
    db.commit()


@app.cli.command("initdb")
def initdb_command():
    """
    Initializes the database.
    """
    init_db()
    print("Initialized the database.")


@app.route("/rest_api/users", methods=["GET"])
def get_users():
    """
    Get all users.
    """
    db = get_db()
    cur = db.execute("select id, name, surname, birth, points from people")
    users = cur.fetchall()
    if not users:
        return jsonify({"msg": "Users not found"})
    return jsonify({"msg": "Users found", "users": users})


@app.route("/rest_api/user", methods=["GET"])
@check_if_all_exist("id")
def get_user():
    """
    Get user with given id.
    """
    req = request.json
    db, uid = get_db(), req["id"]
    cur = db.execute("select id, name, surname, birth, points from people where id=?", [uid])
    user = cur.fetchall()
    if not user:
        return jsonify({"msg": "User not found", "id": uid})
    return jsonify({"msg": "User found", "user": user})


@app.route("/rest_api/user", methods=["POST"])
@check_if_all_exist("name", "surname", "birth", "points")
def add_user():
    """
    Add new user to db, add new id to redis
    (id will expire after 30 minutes)
    """
    req = request.json
    expire_after = 60*30  # seconds
    db = get_db()
    data = [req["name"], req["surname"], req["birth"], req["points"]]
    cur = db.execute("insert into people (name, surname, birth, points) values (?, ?, ?, ?)", data)
    db.commit()
    new_id = cur.lastrowid
    req.update({"id": new_id})
    redis.setex("_key_" + str(new_id), expire_after, "key")
    return jsonify({"msg": "New user added", "user": req})


@app.route("/rest_api/user", methods=["DELETE"])
@check_if_all_exist("id")
def delete_user_by_id():
    """
    Delete available user from db.
    """
    req = request.json
    new_keys, uid = get_keys(redis.keys("_key_*")), req["id"]
    if uid in new_keys:
        return jsonify({"msg": "User not available, try later", "id": uid})

    db = get_db()
    cur = db.execute("select id from people where id = ?", [uid])
    users = cur.fetchall()
    if not users:
        return jsonify({"msg": "Nothing to delete"})
    db.execute("delete from people where id=?", [uid])
    db.commit()
    return jsonify({"msg": "User deleted", "id": uid})


@app.route("/rest_api/user", methods=["PUT"])
@check_if_all_exist("id", "name", "surname", "birth", "points")
def update_user():
    """
    Update user's data (if available).
    """
    r = request.json
    db, uid = get_db(), r["id"]
    cur = db.execute("select id from people where id = ?", [uid])
    users = cur.fetchall()
    if not users:
        return jsonify({"msg": "Nothing to update"})

    new_keys = get_keys(redis.keys("_key_*"))
    if uid in new_keys:
        return jsonify({"msg": "User not available, try later", "id": uid})
    to_update = [r["name"], r["surname"], r["birth"], r["points"], uid]
    db.execute("update people set name=?, surname=?, birth=?, points=? where id=?", to_update)
    db.commit()
    return jsonify({"msg": "User updated", "user": to_update})


def get_keys(redis_list):
    """
    Get keys from redis list.
    """
    return [str(key)[7:-1] for key in redis_list]


@periodic_task(run_every=timedelta(seconds=60))
def increment_points():
    """
    Increment user's points.
    """
    with app.app_context():
        new_keys = get_keys(redis.keys("_key_*"))
        if new_keys:
            db = get_db()
            for key in new_keys:
                cur = db.execute("select points from people where id=?", [key])
                points = cur.fetchall()[0][0]
                points += randint(1, 9)
                db.execute("update people set points=? where id=?", [points, key])
                db.commit()
                print({"id": key, "points after incrementation": points})
        else:
            print("There isn't new users")


if __name__ == "__main__":
    app.run(debug=True)
