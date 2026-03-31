"""SQLAlchemy engine, session, DB initialization with default admin."""
import logging
import os

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .models import Base, User
from .security import hash_password

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///app_data.db")

if DATABASE_URL.startswith("sqlite"):
    engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False}, echo=False)
else:
    engine = create_engine(DATABASE_URL, echo=False)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """Yield a DB session (for use as FastAPI dependency or manual context)."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _migrate_add_columns(db):
    """Add brand_id, access_types, allowed_category_ids, allowed_subcategory_ids to users if missing."""
    cols = [
        ("brand_id", "INTEGER"),
        ("access_types", "TEXT"),
        ("allowed_category_ids", "TEXT"),
        ("allowed_subcategory_ids", "TEXT"),
    ]
    for col, col_type in cols:
        try:
            db.execute(text(f"ALTER TABLE users ADD COLUMN {col} {col_type}"))
            db.commit()
            logger.info("Added column users.%s", col)
        except Exception as e:
            db.rollback()
            if "duplicate" not in str(e).lower() and "already exists" not in str(e).lower():
                logger.warning("Migration %s: %s", col, e)


def init_db():
    """Create tables and seed admin user if not present."""
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        _migrate_add_columns(db)
        admin = db.query(User).filter(User.username == "expert").first()
        if not admin:
            admin = User(
                username="expert",
                hashed_password=hash_password("test"),
                display_name="Admin",
                role="admin",
                is_active=True,
                allowed_filters="[]",
                allowed_tabs='[]',
            )
            db.add(admin)
            db.commit()
            logger.info("Default admin user 'expert' created.")
        else:
            logger.info("Admin user 'expert' already exists.")
    except Exception as e:
        db.rollback()
        logger.error("init_db error: %s", e)
    finally:
        db.close()
