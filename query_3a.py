from pymongo import MongoClient
from bson.tz_util import FixedOffset
import datetime
import logging
import csv
import concurrent.futures
from queue import Queue

logFormatter = '%(asctime)s - %(message)s'
logging.basicConfig(format=logFormatter, level=logging.DEBUG)
logger = logging.getLogger(__name__)
client = MongoClient("mongodb://%s:%s@mongos.perf.league.dev:27018/" % (username, password))
database = client["events"]
collection = database["events"]

database = client["events"]
collection = database["events"]
usersQueue = Queue()

def thread_function(user):
    logger.info("Starting thread %s", user)

    query = {}
    query["$and"] = [
        {
            "event_timestamp": {
                "$lt": datetime.datetime.strptime("2019-11-29 00:00:00.000000", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=FixedOffset(-300, "-0500"))
            }
        },
        {
            "info.opted_into_marketing_communications": {"$in": [True, False]}
        },
        {
            "event_type": "user_profile_set"
        },
        {
            "entity_id": user
        }
    ]
    # get the most recent opt-in flag set for the user
    sort = [("event_timestamp", 1)]
    cursor = collection.find(query, sort=sort, limit=1)

    for doc in cursor:
        opted_in = doc["info"]["opted_into_marketing_communications"]

        logger.info("User %s opt-in flag: %s", user, opted_in)

        # only include the users that opted in before nov 29
        if opted_in:
            logger.info("Adding user %s", user)
            usersQueue.put(user)

    logger.info("Thread %s finished", user)

if __name__ == "__main__":
    logger.info("Started script!!!")

    users = []
    with open('all_users_opt_in_false_after_nov_29_registered_before.csv', newline='') as inputfile:
        for row in csv.reader(inputfile):
            users.append(row[0])

    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        executor.map(thread_function, users)

    with open('result_query_3a.csv', 'w') as f:
        while usersQueue.qsize():
            f.write(usersQueue.get() + ",\n")

    client.close()
