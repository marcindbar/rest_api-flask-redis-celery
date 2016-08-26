# rest_api-flask-redis-celery

## Task
Create application using flask framework, that will give you access to database through rest api.
Specification:
Database - SQLite (name, surname, birthday, points)
Rest handlers:
- return all records from database
- add new database record
- modify and remove single database record.
All handlers should get and return json.

When you add record to the database, points in this record should increase by a random number from set [1,9] every minute (and it should last 30 minutes). Remember to avoid situation when rest api and increasing points mechanism want to change the same database record.
