from sqlalchemy import Column, create_engine, Integer, String, Date, ForeignKey, Index
from sqlalchemy.orm import declarative_base, sessionmaker


engine = create_engine("mysql://root:swordfish@database:3306/codetest")
Base = declarative_base()


class Places(Base):
    __tablename__ = "places"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    city = Column(String(100),  nullable=False, unique=True)
    county = Column(String(100),  nullable=False)
    country = Column(String(100),  nullable=False)
    __table_args__ = (
      Index('idx_places_city', 'city'),
    )


class People(Base):
    __tablename__ = "people"

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    given_name = Column(String(100),  nullable=False)
    family_name = Column(String(100),  nullable=False)
    date_of_birth = Column(Date,  nullable=False)
    place_of_birth = Column(String(100), ForeignKey(Places.city, ondelete="SET NULL"))


Session = sessionmaker(bind=engine)
