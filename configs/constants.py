import os

SESSION_DIR = "sessions"
DEFAULT_SESSION_NAME = "user1"
DEFAULT_LIMIT = 20

DB_DIR = "registry"
DB_PATH = os.path.join(DB_DIR, "amharic_dataset.db")

REVISIT_INTERVAL_SECONDS = 60
BASE_PRIORITY = 0.5

DEBUG = True

SEED_FILE = "seed.txt"