import logging

from consumer.config import load_config
from consumer.consumer import LandingConsumer

logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
    level=logging.INFO,
)


def main() -> None:
    config = load_config()
    LandingConsumer(config).run()


if __name__ == "__main__":
    main()
