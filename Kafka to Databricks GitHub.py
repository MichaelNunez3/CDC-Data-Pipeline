import json
import time

from kafka import KafkaConsumer
from databricks.sdk import WorkspaceClient


# ---------------------------------
# CONFIGURATION
# ============================================================

KAFKA_BOOTSTRAP = "localhost:29092"  # Kafka is available at this address.

TOPIC = "retail.MichaelN_Retail.dbo.RetailCustomer" # Kafka topic containing the Debezium changes

CONSUMER_GROUP = "retail-databricks-etl" #  Consumer group used by Kafka to track the consumer's committed offsets.

DATABRICKS_BRONZE_PATH = (
    "/Volumes/.json"  # hidden for privacy
)


BATCH_SIZE = 10
BATCH_TIMEOUT_SECONDS = 10



# ********************************************
# KAFKA Consumer


consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,

    # Persistent consumer group
    group_id=CONSUMER_GROUP,

    # If this is the first time this group runs,
    # start from the earliest available Kafka record.
    auto_offset_reset="earliest",

    # IMPORTANT:
    # We will manually commit only AFTER Databricks succeeds.
    enable_auto_commit=False,

    value_deserializer=lambda message: (
        json.loads(message.decode("utf-8"))
        if message is not None
        else None
    )

    # Decode Kafka message bytes and deserialize the JSON
    # payload into a Python dictionary.

)


#
# DATABRICKS CLIENT
# -------------------------------------------------------

w = WorkspaceClient()


print("Python CDC ETL bridge started.")
print(f"Kafka topic: {TOPIC}")
print(f"Consumer group: {CONSUMER_GROUP}")
print("Waiting for Kafka CDC events...")


# ============================================================
# Continuous ETL Loop
# ============================================================

while True:

    records = []

    batch_start_time = time.time()

    # --------------------------------------------------------
    # Collect records until:
    #
    #   1. We have BATCH_SIZE records
    #   OR
    #   2. BATCH_TIMEOUT_SECONDS has passed
    # --------------------------------------------------------

    while (
        len(records) < BATCH_SIZE
        and time.time() - batch_start_time < BATCH_TIMEOUT_SECONDS
    ):

        polled = consumer.poll(
            timeout_ms=1000
        )

        for _, messages in polled.items():

            for message in messages:

                if message.value is None:
                    print(
                        f"Skipping null Kafka message at "
                        f"partition={message.partition}, "
                        f"offset={message.offset}"
                    )
                    continue


                records.append({
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "value": message.value
                })

                if len(records) >= BATCH_SIZE:
                    break

            if len(records) >= BATCH_SIZE:
                break


    # --------------------------------------------------------
    # Nothing received
    # --------------------------------------------------------

    if not records:

        print("No new Kafka records. Waiting...")

        continue


    # --------------------------------------------------------
    # Create JSON batch
    # --------------------------------------------------------

    batch_filename = "retail_batch.json"

    batch_path = f"./{batch_filename}"

    with open(batch_path,"w",encoding="utf-8") as file:

        json.dump(records,file,indent=2)


    print(
        f"Wrote {len(records)} Kafka records "
        f"to {batch_filename}"
    )


    # --------------------------------------------------------
    # Upload to Databricks
    # --------------------------------------------------------

    try:

        with open(
            batch_path,
            "rb"
        ) as file:

            w.files.upload(DATABRICKS_BRONZE_PATH,file,overwrite=True)

        print("Uploaded batch to Databricks Bronze successfully.")


        # ----------------------------------------------------
        # ONLY commit Kafka offsets AFTER successful upload
        # ----------------------------------------------------

        consumer.commit()

        print("Kafka offsets committed successfully.")


    except Exception as error:

        print("ERROR uploading batch to Databricks:")

        print(error)

        print("Kafka offsets were NOT committed." )

        print("The records will be available again when the consumer restarts.")

        time.sleep(5)