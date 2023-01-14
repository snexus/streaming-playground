from confluent_kafka.admin import AdminClient, NewTopic
import argparse


def main(args):
    admin_client = AdminClient({"bootstrap.servers": args.bootstrap_servers})

    # This is a toy example, don't use in production
    num_partitions = 1
    replication_factor = 1

    topics = args.topics.split(",")
    topic_list = [NewTopic(t, num_partitions, replication_factor) for t in topics]
    print("Creating topics: ", topics)
    admin_client.create_topics(topic_list)

    res = admin_client.list_topics()
    print("List of topics: ")
    print(res.topics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="JSONSerailizer example")
    parser.add_argument(
        "-b",
        dest="bootstrap_servers",
        default="localhost:9092",
        help="Bootstrap broker(s) (host[:port])",
    )
    parser.add_argument("-t", dest="topics", required=True, help="Names of the topics")
    main(parser.parse_args())
