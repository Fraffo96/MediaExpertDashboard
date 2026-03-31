"""SQLAlchemy models for users, ecosystems and permissions."""
import json
from sqlalchemy import Column, Integer, String, Boolean, Text, ForeignKey, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

ACCESS_SALES_INTELLIGENCE = "sales_intelligence"
ACCESS_MARKETING_INSIGHTS = "marketing_insights"
ALL_ACCESS_TYPES = [ACCESS_SALES_INTELLIGENCE, ACCESS_MARKETING_INSIGHTS]
# New Sales tabs
TAB_MARKET_INTELLIGENCE = "market_intelligence"
TAB_BRAND_COMPARISON = "brand_comparison"
TAB_PROMO_CREATOR = "promo_creator"
TAB_CHECK_LIVE_PROMO = "check_live_promo"
TAB_MARKETING = "marketing"
ALL_TABS = [TAB_MARKET_INTELLIGENCE, TAB_BRAND_COMPARISON, TAB_PROMO_CREATOR, TAB_CHECK_LIVE_PROMO, TAB_MARKETING]
ALL_FILTERS = ["period", "category", "segment", "brand", "gender", "promo_type"]


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String(100), unique=True, nullable=False, index=True)
    hashed_password = Column(String(255), nullable=False)
    display_name = Column(String(200), default="")
    role = Column(String(20), nullable=False, default="user")
    is_active = Column(Boolean, default=True)
    brand_id = Column(Integer, nullable=True)  # FK to dim_brand, required for non-admin users
    access_types = Column(Text, default='[]')  # ["sales_intelligence", "marketing_insights"]
    allowed_category_ids = Column(Text, default='[]')  # [1,2,3] parent categories user can see
    allowed_subcategory_ids = Column(Text, default='[]')  # [101,102,201] subcategories; empty = all in allowed categories
    allowed_filters = Column(Text, default="[]")
    allowed_tabs = Column(Text, default='["basic"]')  # legacy

    ecosystems = relationship("UserEcosystem", back_populates="user", cascade="all, delete-orphan")

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def can_recalculate(self) -> bool:
        """Può ricalcolare le tabelle precalcolate. Default: solo admin."""
        return self.is_admin

    @property
    def access_type_list(self) -> list[str]:
        try:
            return json.loads(self.access_types or "[]")
        except Exception:
            return []

    @access_type_list.setter
    def access_type_list(self, v: list[str]):
        self.access_types = json.dumps(v)

    @property
    def filter_list(self) -> list[str]:
        try:
            return json.loads(self.allowed_filters or "[]")
        except Exception:
            return []

    @filter_list.setter
    def filter_list(self, v: list[str]):
        self.allowed_filters = json.dumps(v)

    @property
    def tab_list(self) -> list[str]:
        """Deriva da access_types: sales_intelligence -> market_intelligence,brand_comparison,promo_creator; marketing_insights -> marketing."""
        acc = self.access_type_list
        if not acc:
            return []
        tabs = []
        if ACCESS_SALES_INTELLIGENCE in acc:
            tabs.extend([TAB_MARKET_INTELLIGENCE, TAB_BRAND_COMPARISON, TAB_PROMO_CREATOR, TAB_CHECK_LIVE_PROMO])
        if ACCESS_MARKETING_INSIGHTS in acc:
            tabs.append(TAB_MARKETING)
        return list(dict.fromkeys(tabs))

    @tab_list.setter
    def tab_list(self, v: list[str]):
        self.allowed_tabs = json.dumps(v or [])

    def can_access_tab(self, tab: str) -> bool:
        if self.is_admin:
            return True
        return tab in self.tab_list

    @property
    def category_ids_list(self) -> list[int]:
        try:
            return [int(x) for x in json.loads(self.allowed_category_ids or "[]")]
        except Exception:
            return []

    @category_ids_list.setter
    def category_ids_list(self, v: list[int]):
        self.allowed_category_ids = json.dumps(v)

    @property
    def subcategory_ids_list(self) -> list[int]:
        try:
            return [int(x) for x in json.loads(self.allowed_subcategory_ids or "[]")]
        except Exception:
            return []

    @subcategory_ids_list.setter
    def subcategory_ids_list(self, v: list[int]):
        self.allowed_subcategory_ids = json.dumps(v)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "username": self.username,
            "display_name": self.display_name,
            "role": self.role,
            "is_active": self.is_active,
            "brand_id": self.brand_id,
            "access_types": self.access_type_list,
            "allowed_category_ids": self.category_ids_list,
            "allowed_subcategory_ids": self.subcategory_ids_list,
            "allowed_filters": self.filter_list,
            "allowed_tabs": self.tab_list,
            "ecosystems": [ue.ecosystem_id for ue in self.ecosystems],
        }


class Ecosystem(Base):
    __tablename__ = "ecosystems"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(200), unique=True, nullable=False)
    description = Column(Text, default="")
    icon = Column(String(10), default="")
    is_active = Column(Boolean, default=True)

    categories = relationship("EcosystemCategory", back_populates="ecosystem", cascade="all, delete-orphan")
    brands = relationship("EcosystemBrand", back_populates="ecosystem", cascade="all, delete-orphan")
    users = relationship("UserEcosystem", back_populates="ecosystem", cascade="all, delete-orphan")

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "icon": self.icon,
            "is_active": self.is_active,
            "category_ids": [ec.category_id for ec in self.categories],
            "brand_ids": [eb.brand_id for eb in self.brands],
        }


class EcosystemCategory(Base):
    __tablename__ = "ecosystem_categories"
    __table_args__ = (UniqueConstraint("ecosystem_id", "category_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    ecosystem_id = Column(Integer, ForeignKey("ecosystems.id", ondelete="CASCADE"), nullable=False)
    category_id = Column(Integer, nullable=False)

    ecosystem = relationship("Ecosystem", back_populates="categories")


class EcosystemBrand(Base):
    __tablename__ = "ecosystem_brands"
    __table_args__ = (UniqueConstraint("ecosystem_id", "brand_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    ecosystem_id = Column(Integer, ForeignKey("ecosystems.id", ondelete="CASCADE"), nullable=False)
    brand_id = Column(Integer, nullable=False)

    ecosystem = relationship("Ecosystem", back_populates="brands")


class UserEcosystem(Base):
    __tablename__ = "user_ecosystems"
    __table_args__ = (UniqueConstraint("user_id", "ecosystem_id"),)

    id = Column(Integer, primary_key=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    ecosystem_id = Column(Integer, ForeignKey("ecosystems.id", ondelete="CASCADE"), nullable=False)

    user = relationship("User", back_populates="ecosystems")
    ecosystem = relationship("Ecosystem", back_populates="users")
