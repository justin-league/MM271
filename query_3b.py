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

def get_set_user_profile_timestamp(user):
    logger.info("Getting set user profile timestamp for user %s", user)

    query = {}
    query["$and"] = [
        {
            "entity_id": user
        },
        {
            "event_type": "user_profile_set"
        },
        {
            "request_type": "set_user_profile"
        },
        {
            "event_timestamp": {
                "$gte": datetime.datetime.strptime("2019-11-28 19:00:00.000000", "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=FixedOffset(-300, "-0500"))
            }
        },
        {
            "info.opted_into_marketing_communications": False
        }
    ]

    sort = [("event_timestamp", 1)]

    cursor = collection.find(query, sort=sort, limit=1)

    for doc in cursor:
        timestamp = doc["event_timestamp"]

        logger.info("User %s timestamp %s", user, timestamp)

        return timestamp

    logger.error("User %s had no timestamp", user)

    return None


def thread_function(user):
    logger.info("Starting thread %s", user)

    set_user_profile_timestamp = get_set_user_profile_timestamp(user)

    if set_user_profile_timestamp is None:
        logger.error("Exiting")
        exit(1)

    query = {}
    query["$and"] = [
        {
            "event_type": "member_terms_accepted"
        },
        {
            "entity_id": user
        },
        {
            "event_timestamp": {
                "$gte": set_user_profile_timestamp - datetime.timedelta(seconds=1),
                "$lte": set_user_profile_timestamp + datetime.timedelta(seconds=1)
            }
        }
    ]
    # get the most recent opt-in flag set for the user
    sort = [("event_timestamp", 1)]
    cursor = collection.find(query, sort=sort, limit=1)

    for doc in cursor:
        logger.info("Adding user %s", user)
        usersQueue.put(user)

    logger.info("Thread %s finished", user)

if __name__ == "__main__":
    logger.info("Started script!!!")

    users = []
    with open('query_3a.csv', newline='') as inputfile:
        for row in csv.reader(inputfile):
            users.append(row[0])

    with concurrent.futures.ThreadPoolExecutor(max_workers=200) as executor:
        executor.map(thread_function, users)

    logger.info("Finished handling requests - writing to file")

    with open('result_query_3b.csv', 'w') as f:
        while usersQueue.qsize():
            f.write(usersQueue.get() + ",\n")

    client.close()
